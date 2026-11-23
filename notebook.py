# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Prepare the next team's dataset
# MAGIC Follow CHALLENGE.md. These headings are a starting point; implement and verify each stage yourself.

# COMMAND ----------

from pathlib import Path
import json

# Copy this Git folder's absolute path from its Workspace menu; include /Workspace/.
repo_root = Path("/Workspace/Users/alsu.flare.irs@catalyte.ai/to6-databricks-challenge-starter/")
assert (repo_root / "README.md").is_file(), "Check the Git folder path"
practice_result = spark.sql("SELECT 1 AS connection_check").first().asDict()
assert practice_result == {"connection_check": 1}
(repo_root / "outputs").mkdir(exist_ok=True)
(repo_root / "outputs/connection-practice.json").write_text(
    json.dumps(practice_result) + "\n", encoding="utf-8"
)

# COMMAND ----------

import hashlib

INPUT_SCHEMA = "training.to6_challenge3_inputs"
INITIAL_TABLE = f"{INPUT_SCHEMA}.returns_initial"
CORRECTED_TABLE = f"{INPUT_SCHEMA}.returns_corrected"
LINEAGE_VIEW = f"{INPUT_SCHEMA}.source_lineage"
workspace_user = spark.sql("SELECT current_user() AS username").first()["username"]
OUTPUT_SCHEMA = "training.to6_c3_" + hashlib.sha256(workspace_user.encode("utf-8")).hexdigest()[:12]
print({"input_schema": INPUT_SCHEMA, "output_schema": OUTPUT_SCHEMA})

# COMMAND ----------

# DBTITLE 1,Stage 1 notes
# MAGIC %md
# MAGIC ## 1. Understand the input
# MAGIC Record Catalog descriptions, sample observations and the supplied lineage source.
# MAGIC
# MAGIC **Source reference.** `training.to6_challenge3_inputs.source_lineage` (supplied view, not the Catalog UI Lineage tab) states that both
# MAGIC `returns_initial` and `returns_corrected` were created by SQL from `training.to6_challenge3_inputs.parsed_source`.
# MAGIC Catalog descriptions: `parsed_source` - *one row is one return; `return_id` identifies the return; `taxpayer_id` identifies a filer who may recur
# MAGIC across years; nonnegative amounts; zero income is valid*. `returns_initial` - *initial delivery with known delivery defects; validate before feature output*.
# MAGIC `returns_corrected` - *corrected delivery; one row per return; validate required values and `return_id` uniqueness*.
# MAGIC
# MAGIC **Row meaning and fields.** One row = one return. Uniqueness identifier: `return_id`. Required fields: `return_id`, `tax_year`, `income`, `deduction`.
# MAGIC `taxpayer_id` is a filer identifier (repeats across years by design - not a uniqueness key) and is excluded from features under need-to-know.
# MAGIC
# MAGIC **Findings that drive the next steps** (from the cell below): `returns_initial` has 13 rows for 12 distinct returns - `R01` appears twice and `R02`
# MAGIC has a null `income`. Both batches have two zero-income returns (`R03` 2022, `R09` 2024), which are valid and must keep a null ratio rather than
# MAGIC being dropped or divided by zero. Every year 2022-2024 has 4 distinct returns. Consequences: the completeness and uniqueness rules in Stage 2 must
# MAGIC reject the initial batch; the ratio must guard income = 0; the year split must use `tax_year` exactly.

# COMMAND ----------

# DBTITLE 1,Inspect descriptions, lineage, samples and batch profile
from pyspark.sql import functions as F

UPSTREAM_TABLE = f"{INPUT_SCHEMA}.parsed_source"
REQUIRED = ["return_id", "tax_year", "income", "deduction"]

# Catalog descriptions (same text shown in Catalog Explorer)
for t in [UPSTREAM_TABLE, INITIAL_TABLE, CORRECTED_TABLE, LINEAGE_VIEW]:
    comment = [r for r in spark.sql(f"DESCRIBE TABLE EXTENDED {t}").collect() if r.col_name == "Comment"]
    print(f"{t}: {comment[0].data_type if comment else '(no comment)'}")

# Supplied lineage view
display(spark.table(LINEAGE_VIEW))

# At least three sample rows from each delivery
for t in [INITIAL_TABLE, CORRECTED_TABLE]:
    print("Sample rows:", t)
    display(spark.table(t).orderBy("return_id").limit(5))

# Full-batch profile: returns by year, missing required values, duplicate IDs, zero income
def profile(table):
    df = spark.table(table)
    by_year = (df.groupBy("tax_year")
                 .agg(F.count("*").alias("rows"), F.countDistinct("return_id").alias("distinct_return_ids"),
                      F.sum(F.when(F.col("income") == 0, 1).otherwise(0)).alias("zero_income_rows"))
                 .orderBy("tax_year"))
    missing = df.select([F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}") for c in REQUIRED])
    dups = df.groupBy("return_id").count().filter("count > 1")
    zero = df.filter(F.col("income") == 0).select("return_id", "tax_year", "income", "deduction")
    print(f"== {table}: total rows={df.count()}, distinct return_id={df.select('return_id').distinct().count()}")
    display(by_year); display(missing); display(dups); display(zero)

for t in [INITIAL_TABLE, CORRECTED_TABLE]:
    profile(t)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE training.to6_challenge3_inputs.returns_initial;
# MAGIC
# MAGIC DESCRIBE training.to6_challenge3_inputs.returns_corrected;
# MAGIC
# MAGIC DESCRIBE training.to6_challenge3_inputs.source_lineage;

# COMMAND ----------

# DBTITLE 1,Stage 2 notes
# MAGIC %md
# MAGIC ## 2. Decide which batch is usable
# MAGIC Keep the initial rejection evidence and apply the same checks to the replacement.
# MAGIC
# MAGIC Rules applied identically to both batches (`check_batch`):
# MAGIC * **Completeness** - every required field (`return_id`, `tax_year`, `income`, `deduction`) is non-null.
# MAGIC * **Uniqueness** - each `return_id` occurs exactly once.
# MAGIC * **Validity** - amounts are nonnegative and `tax_year` is in 2022-2024 (zero income is valid).
# MAGIC
# MAGIC The initial batch fails completeness (`R02` has null `income`) and uniqueness (`R01` occurs twice), so it is rejected **before** any feature
# MAGIC table is written. Its affected records and reasons are saved to `<OUTPUT_SCHEMA>.initial_rejected_records` and `outputs/initial_rejected_records.csv`,
# MAGIC with the rule summary in `outputs/initial_rejection.json`. Next action: no repair of the initial batch (no imputing `R02` income as 0, no silent
# MAGIC dedup of `R01`); the replacement `returns_corrected` is assessed with the same rules and, only if it passes, used for the final run.

# COMMAND ----------

# DBTITLE 1,Check both batches, preserve rejection evidence
from datetime import datetime, timezone
import uuid

RUN_TS = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
RUN_ID = f"run-{RUN_TS.replace(':', '').replace('-', '')}-{uuid.uuid4().hex[:6]}"
NOTEBOOK_URL = "https://dbc-48a16403-f66f.cloud.databricks.com/editor/notebooks/127024866558243"
print({"run_id": RUN_ID, "run_ts_utc": RUN_TS, "notebook_url": NOTEBOOK_URL})

def check_batch(table):
    """Return (passed, rule_results, affected_df) using the same rules for every batch."""
    df = spark.table(table)
    dup_ids = df.groupBy("return_id").count().filter("count > 1").withColumnRenamed("count", "copies")
    reasons = F.concat_ws("; ",
        *[F.when(F.col(c).isNull(), F.lit(f"completeness: {c} is null")) for c in REQUIRED],
        F.when(F.col("copies").isNotNull(), F.concat(F.lit("uniqueness: return_id occurs "), F.col("copies").cast("string"), F.lit(" times"))),
        F.when((F.col("income") < 0) | (F.col("deduction") < 0), F.lit("validity: negative amount")),
        F.when(~F.col("tax_year").between(2022, 2024), F.lit("validity: tax_year outside 2022-2024")))
    flagged = (df.join(dup_ids, "return_id", "left").withColumn("rejection_reason", reasons)
                 .filter(F.col("rejection_reason") != "").drop("copies"))
    rules = {
        "completeness_null_required_rows": df.filter(" OR ".join(f"{c} IS NULL" for c in REQUIRED)).count(),
        "uniqueness_duplicate_return_ids": dup_ids.count(),
        "validity_negative_amount_rows": df.filter("income < 0 OR deduction < 0").count(),
        "validity_year_out_of_range_rows": df.filter("tax_year NOT BETWEEN 2022 AND 2024").count(),
        "total_rows": df.count(),
        "distinct_return_ids": df.select("return_id").distinct().count(),
    }
    passed = all(v == 0 for k, v in rules.items() if k.split("_")[0] in ("completeness", "uniqueness", "validity"))
    return passed, rules, flagged

# --- Initial batch: must be rejected before any feature output ---
initial_ok, initial_rules, initial_affected = check_batch(INITIAL_TABLE)
print("INITIAL:", "PASS" if initial_ok else "REJECTED", initial_rules)
assert not initial_ok, "Expected the initial batch to fail completeness/uniqueness checks"

REJECTION_REF = f"initial-rejection-{RUN_ID}"
affected_out = (initial_affected.withColumn("rejection_ref", F.lit(REJECTION_REF))
                .withColumn("source_table", F.lit(INITIAL_TABLE))
                .orderBy("return_id", "tax_year"))
affected_out.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{OUTPUT_SCHEMA}.initial_rejected_records")
display(spark.table(f"{OUTPUT_SCHEMA}.initial_rejected_records"))

(repo_root / "outputs").mkdir(exist_ok=True)
affected_out.toPandas().to_csv(repo_root / "outputs/initial_rejected_records.csv", index=False)
(repo_root / "outputs/initial_rejection.json").write_text(json.dumps({
    "rejection_ref": REJECTION_REF, "run_id": RUN_ID, "checked_at_utc": RUN_TS, "notebook_url": NOTEBOOK_URL,
    "table": INITIAL_TABLE, "decision": "REJECTED", "rule_results": initial_rules,
    "affected_records_table": f"{OUTPUT_SCHEMA}.initial_rejected_records",
    "next_action": "Do not repair or use the initial batch; assess returns_corrected with the same rules.",
}, indent=2) + "\n", encoding="utf-8")

# --- Replacement batch: same rules; must pass to be used ---
corrected_ok, corrected_rules, corrected_affected = check_batch(CORRECTED_TABLE)
print("CORRECTED:", "PASS" if corrected_ok else "REJECTED", corrected_rules)
assert corrected_ok and corrected_affected.count() == 0, "Replacement batch failed checks - stop, do not write features"
(repo_root / "outputs/corrected_checks.json").write_text(json.dumps({
    "run_id": RUN_ID, "checked_at_utc": RUN_TS, "table": CORRECTED_TABLE,
    "decision": "ACCEPTED", "rule_results": corrected_rules}, indent=2) + "\n", encoding="utf-8")
ACCEPTED_TABLE = CORRECTED_TABLE

# COMMAND ----------

# DBTITLE 1,Stage 3 notes
# MAGIC %md
# MAGIC ## 3. Deliver a checked feature table
# MAGIC Implement the task's data contract and verify the saved result by reading it back.
# MAGIC
# MAGIC Contract for `<OUTPUT_SCHEMA>.return_features`: exactly `return_id` (string), `tax_year` (int), `deduction_to_income` (double) =
# MAGIC `deduction / income`, null when `income = 0`. One row per accepted return; `taxpayer_id`, `income` and `deduction` are excluded (need-to-know).
# MAGIC The table is written with `CREATE OR REPLACE` semantics (overwrite), so reruns cannot append copies. The nonzero check compares the saved ratio
# MAGIC for `R04` with an expected value computed in plain Python from the upstream `parsed_source` row (59 / 400 = 0.1475).

# COMMAND ----------

# DBTITLE 1,Build, save and read back return_features
assert ACCEPTED_TABLE == CORRECTED_TABLE and corrected_ok, "Only the accepted replacement batch may feed features"
FEATURES_TABLE = f"{OUTPUT_SCHEMA}.return_features"

features = (spark.table(ACCEPTED_TABLE)
            .select("return_id", "tax_year",
                    F.when(F.col("income") == 0, F.lit(None).cast("double"))
                     .otherwise(F.col("deduction").cast("double") / F.col("income").cast("double"))
                     .alias("deduction_to_income")))
features.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(FEATURES_TABLE)

# ---- Read back the SAVED table and verify the contract ----
saved = spark.table(FEATURES_TABLE)
accepted = spark.table(ACCEPTED_TABLE)
assert saved.columns == ["return_id", "tax_year", "deduction_to_income"], saved.columns
assert saved.count() == accepted.count() == saved.select("return_id").distinct().count(), "row count / uniqueness mismatch"
assert saved.select("return_id", "tax_year").exceptAll(accepted.select("return_id", "tax_year")).count() == 0
assert accepted.select("return_id", "tax_year").exceptAll(saved.select("return_id", "tax_year")).count() == 0

# Nonzero case: expected value derived independently from the upstream source row
src = spark.table(UPSTREAM_TABLE).filter("return_id = 'R04'").collect()
assert len(src) == 1
expected_r04 = src[0]["deduction"] / src[0]["income"]          # plain Python: 59 / 400
actual_r04 = saved.filter("return_id = 'R04'").first()["deduction_to_income"]
print({"R04_source": (src[0]["deduction"], src[0]["income"]), "expected": expected_r04, "saved": actual_r04})
assert abs(actual_r04 - expected_r04) < 1e-12 and abs(expected_r04 - 0.1475) < 1e-12

# Zero-income cases stay in the table with a null ratio
zero_ids = [r.return_id for r in accepted.filter("income = 0").collect()]
zero_saved = saved.filter(F.col("return_id").isin(zero_ids)).orderBy("return_id")
display(zero_saved)
assert zero_saved.count() == len(zero_ids) == 2 and zero_saved.filter("deduction_to_income IS NOT NULL").count() == 0
# Only zero-income returns may have a null ratio
assert saved.filter("deduction_to_income IS NULL").count() == len(zero_ids)

readback_evidence = {
    "run_id": RUN_ID, "table": FEATURES_TABLE, "columns": saved.columns, "rows": saved.count(),
    "nonzero_check": {"return_id": "R04", "source_table": UPSTREAM_TABLE, "deduction": src[0]["deduction"],
                      "income": src[0]["income"], "expected": expected_r04, "saved": actual_r04},
    "zero_income_check": [r.asDict() for r in zero_saved.collect()],
}
(repo_root / "outputs/feature_readback_checks.json").write_text(json.dumps(readback_evidence, indent=2) + "\n", encoding="utf-8")
display(saved.orderBy("return_id"))

# COMMAND ----------

# DBTITLE 1,Stage 4 notes
# MAGIC %md
# MAGIC ## 4. Make the handoff usable
# MAGIC Save the required year tables. Keep references and IDs-and-values evidence for two successful runs.
# MAGIC Write SUBMISSION.md and the small exports inside this Databricks Git folder.
# MAGIC
# MAGIC Splits are built **from the saved `return_features`** (`2022 -> features_train`, `2023 -> features_validation`, `2024 -> features_test`), each written
# MAGIC with overwrite. Checks: every split row has its assigned year, no return ID is in more than one split, the union of splits equals `return_features`
# MAGIC exactly (IDs and values), and no table has duplicate IDs. At the end of each successful run the IDs and values of all four tables are saved to
# MAGIC `outputs/run_snapshots/<run_id>.json`; the next cell compares the two most recent successful snapshots row-by-row.

# COMMAND ----------

# DBTITLE 1,Save year splits, verify, snapshot this run
SPLITS = {2022: f"{OUTPUT_SCHEMA}.features_train", 2023: f"{OUTPUT_SCHEMA}.features_validation", 2024: f"{OUTPUT_SCHEMA}.features_test"}

saved_features = spark.table(FEATURES_TABLE)
for year, table in SPLITS.items():
    saved_features.filter(F.col("tax_year") == year).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)

# ---- Verify the saved splits ----
split_dfs = {y: spark.table(t) for y, t in SPLITS.items()}
for year, df in split_dfs.items():
    assert df.columns == ["return_id", "tax_year", "deduction_to_income"]
    assert df.filter(F.col("tax_year") != year).count() == 0, f"wrong year in {SPLITS[year]}"
    assert df.count() == df.select("return_id").distinct().count(), f"duplicate IDs in {SPLITS[year]}"
union = split_dfs[2022].unionByName(split_dfs[2023]).unionByName(split_dfs[2024])
assert union.count() == union.select("return_id").distinct().count(), "a return appears in more than one split"
assert union.exceptAll(saved_features).count() == 0 and saved_features.exceptAll(union).count() == 0, "splits != features"
assert saved_features.select("return_id").exceptAll(spark.table(ACCEPTED_TABLE).select("return_id")).count() == 0
split_counts = {SPLITS[y]: df.count() for y, df in split_dfs.items()}
print("Split row counts:", split_counts, "| features:", saved_features.count())

# ---- Exports of the saved tables (small synthetic data) ----
for name, table in [("return_features", FEATURES_TABLE)] + [(t.split(".")[-1], t) for t in SPLITS.values()]:
    spark.table(table).orderBy("return_id").toPandas().to_csv(repo_root / f"outputs/{name}.csv", index=False)

# ---- Snapshot of this successful run: IDs and values for every output table ----
def rows_of(table):
    return [[r.return_id, r.tax_year, r.deduction_to_income] for r in spark.table(table).orderBy("return_id").collect()]

def last_write(table):
    h = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()
    return {"version": h.version, "timestamp": str(h.timestamp), "operation": h.operation,
            "mode": (h.operationParameters or {}).get("mode")}

all_tables = [FEATURES_TABLE] + list(SPLITS.values())
snapshot = {
    "run_id": RUN_ID, "run_ts_utc": RUN_TS, "finished_ts_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "notebook_url": NOTEBOOK_URL, "accepted_input": ACCEPTED_TABLE, "rejection_ref": REJECTION_REF,
    "tables": {t: {"rows": rows_of(t), "last_write": last_write(t)} for t in all_tables},
}
(repo_root / "outputs/run_snapshots").mkdir(exist_ok=True)
(repo_root / f"outputs/run_snapshots/{RUN_ID}.json").write_text(json.dumps(snapshot, indent=2) + "\n", encoding="utf-8")
print("Saved snapshot:", f"outputs/run_snapshots/{RUN_ID}.json")

# COMMAND ----------

# DBTITLE 1,Compare the two most recent successful runs
# Compare IDs and values (not just counts) between the two most recent successful-run snapshots
snap_files = sorted((repo_root / "outputs/run_snapshots").glob("*.json"),
                    key=lambda p: json.loads(p.read_text())["finished_ts_utc"])
if len(snap_files) < 2:
    print(f"{len(snap_files)} successful snapshot(s) so far - run the whole notebook again to produce the comparison.")
else:
    a, b = (json.loads(p.read_text()) for p in snap_files[-2:])
    per_table = {}
    for t in a["tables"]:
        ra, rb = a["tables"][t]["rows"], b["tables"][t]["rows"]
        ids_a, ids_b = [r[0] for r in ra], [r[0] for r in rb]
        per_table[t] = {
            "rows_run_a": len(ra), "rows_run_b": len(rb),
            "no_duplicate_ids_run_a": len(ids_a) == len(set(ids_a)),
            "no_duplicate_ids_run_b": len(ids_b) == len(set(ids_b)),
            "identical_ids": ids_a == ids_b,
            "identical_ids_and_values": ra == rb,
            "differences": [x for x in ra if x not in rb] + [x for x in rb if x not in ra],
            "last_write_run_b": b["tables"][t]["last_write"],
        }
    comparison = {
        "run_a": {k: a[k] for k in ("run_id", "run_ts_utc", "finished_ts_utc", "notebook_url")},
        "run_b": {k: b[k] for k in ("run_id", "run_ts_utc", "finished_ts_utc", "notebook_url")},
        "snapshot_files": [f"outputs/run_snapshots/{p.name}" for p in snap_files[-2:]],
        "tables": per_table,
        "all_identical": all(v["identical_ids_and_values"] and v["no_duplicate_ids_run_a"] and v["no_duplicate_ids_run_b"]
                              for v in per_table.values()),
    }
    (repo_root / "outputs/run_comparison.json").write_text(json.dumps(comparison, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({t: {k: v[k] for k in ("rows_run_a", "rows_run_b", "identical_ids_and_values")} for t, v in per_table.items()}, indent=1))
    print("ALL IDENTICAL:", comparison["all_identical"])
    assert comparison["all_identical"], "Runs differ - investigate before handoff"