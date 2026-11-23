# My dataset submission

Authored in Databricks. Implementation and evidence: `notebook` (exported as `notebook.py`) in this Git folder.

## Inputs and outputs

**Inputs (read-only)**
* `training.to6_challenge3_inputs.returns_initial`: initial delivery, 13 rows. Rejected.
* `training.to6_challenge3_inputs.returns_corrected`: replacement delivery, 12 rows. Accepted and used for the final runs.
* `training.to6_challenge3_inputs.source_lineage`: the supplied lineage view. It says both deliveries were created by SQL from
  `training.to6_challenge3_inputs.parsed_source`.
* `training.to6_challenge3_inputs.parsed_source`: the upstream source. Used only to derive the expected ratio independently.

**Row meaning:** one row is one tax return. `return_id` identifies the return and must be unique. `taxpayer_id` identifies the filer,
who can recur across years. Required fields: `return_id`, `tax_year`, `income`, `deduction`. Amounts are nonnegative, and zero income is valid.

**Outputs (assigned schema `training.to6_c3_915f8c75478a`)**
* `training.to6_c3_915f8c75478a.return_features`: one row per accepted return (12 rows). Columns are exactly
  `return_id`, `tax_year`, `deduction_to_income`.
* `training.to6_c3_915f8c75478a.features_train` (tax_year 2022, 4 rows)
* `training.to6_c3_915f8c75478a.features_validation` (tax_year 2023, 4 rows)
* `training.to6_c3_915f8c75478a.features_test` (tax_year 2024, 4 rows)
* `training.to6_c3_915f8c75478a.initial_rejected_records`: the rejected batch's affected records with their reasons.

**Ratio definition:** `deduction_to_income = deduction / income` (double). It is NULL when `income = 0`; those returns stay in the table.
`taxpayer_id`, `income` and `deduction` are left out of the feature tables (need-to-know rule).

## Checks and decisions

**Source reference:** the Catalog descriptions of the four input objects plus the `source_lineage` view. These are recorded in Stage 1 of the notebook,
with 5 sample rows from each delivery and a full-batch profile (rows by year, nulls in required fields, duplicate IDs, zero-income rows).

**Rules (`check_batch`, applied the same way to both batches):**
* Completeness: no NULLs in required fields.
* Uniqueness: each `return_id` appears once.
* Validity: amounts are >= 0 and `tax_year` is in 2022-2024.

**Initial batch: REJECTED before any feature table was written.** Completeness failed on 1 row: `R02` (2022) has NULL `income`. Uniqueness
failed on 1 ID: `R01` appears 2 times. The batch has 13 rows but only 12 distinct IDs. The 3 affected rows and their reasons are saved in
`training.to6_c3_915f8c75478a.initial_rejected_records` and `outputs/initial_rejected_records.csv`. The rule summary, rejection reference
(`initial-rejection-<run_id>`) and next action are in `outputs/initial_rejection.json`. The initial batch was not repaired: `R02` income was not
set to 0 and `R01` was not silently deduplicated.

**Replacement batch: ACCEPTED.** All rule counts were 0 (12 rows, 12 distinct IDs). Evidence: `outputs/corrected_checks.json`.

**Nonzero expected value:** for `R04`, Python computed the expected ratio from the `parsed_source` row: 59 / 400 = 0.1475. The value saved in
`return_features` is 0.1475. A separate SQL check after the runs joined `return_features` to `parsed_source` and found 0 mismatches across all 12 returns.

**Saved-table readback** (`outputs/feature_readback_checks.json`): the columns are exactly the three contract columns. There are 12 rows and 12
distinct IDs, and the IDs and years match `returns_corrected` exactly. The zero-income returns `R03` (2022) and `R09` (2024) are present with a
NULL ratio, and they are the only rows with a NULL ratio.

## Two successful runs

Both were interactive runs of every code cell in the notebook, in order:
https://dbc-48a16403-f66f.cloud.databricks.com/editor/notebooks/127024866558243

| Run | run_id | Started (UTC) | Finished (UTC) | Snapshot |
| --- | --- | --- | --- | --- |
| 1 | run-20261120T161304Z-bb63cf | 2026-11-20 16:13:04 | 2026-11-20 16:13:49 | `outputs/run_snapshots/run-20261120T161304Z-bb63cf.json` |
| 2 | run-20261120T161446Z-03e856 | 2026-11-20 16:14:46 | 2026-11-20 16:15:30 | `outputs/run_snapshots/run-20261120T161446Z-03e856.json` |

Each snapshot holds every `(return_id, tax_year, deduction_to_income)` row of `return_features` and the three year tables, plus each table's
latest Delta write. `outputs/run_comparison.json` compares the two snapshots row by row. In all four tables the IDs and values are identical,
there are 0 differences and no duplicate IDs (12 / 4 / 4 / 4 rows). The result is `all_identical: true`.

* **Year assignment:** the notebook asserts that each year table holds only its assigned `tax_year`. After the runs, a separate SQL count found
  4 of 4 rows on the assigned year in each table.
* **Every accepted return appears exactly once:** the union of the three year tables has no repeated `return_id`. It equals `return_features`
  in both directions (`exceptAll`), and `return_features` equals the accepted IDs.
* **No appended copies:** every table is written in overwrite mode. Delta history shows `CREATE OR REPLACE TABLE AS SELECT`, and the row counts
  were the same after run 2 as after run 1 (12 / 4 / 4 / 4), not doubled.

## Final files and limitations

Final exports in `outputs/`:
* `return_features.csv`, `features_train.csv`, `features_validation.csv`, `features_test.csv`: the saved tables, from run 2.
* `initial_rejected_records.csv`, `initial_rejection.json`: the affected records, reasons and rejection reference.
* `corrected_checks.json`: the replacement batch's rule results.
* `feature_readback_checks.json`: the nonzero expected value and the zero-income readback.
* `run_snapshots/*.json` (2 files) and `run_comparison.json`: the two-run IDs-and-values evidence.

Practice file, not part of the task: `outputs/connection-practice.json`.

Limitations and things not verified:
* Each run overwrites the rejection exports. After the two full runs, Stages 2-3 were re-run on 2026-11-20, so
  `outputs/initial_rejection.json` and `outputs/initial_rejected_records.csv` carry reference
  `initial-rejection-run-20261120T174800Z-bf10d1`, and `outputs/feature_readback_checks.json` carries that run_id. The same 3 affected rows
  were produced each time; full runs 1 and 2 used `initial-rejection-run-20261120T161304Z-bb63cf` and
  `initial-rejection-run-20261120T161446Z-03e856`.
* In `initial_rejected_records.csv`, pandas writes `income` as a float (`100.0`) because the column contains a NULL. The Delta table keeps it as int.
* Interactive runs only; no Jobs run IDs.
* The checks cover this task's data contract only. They do not show that the data has no model leakage, and no labels are included.
* The `source_lineage` view is a supplied map; the Catalog UI Lineage tab was not checked separately.
* The GitHub commit contents are checked after Commit & Push, which is outside this file.
