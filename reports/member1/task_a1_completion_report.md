# Task A1 Final Verification & Delivery Report

**Workstream:** Member 1 (Developer A) — Demand Forecasting Contracts & Analytical Pipeline  
**Repository:** [https://github.com/thenithin342/urban-service-intelligence](https://github.com/thenithin342/urban-service-intelligence)  
**Branch:** [`feature/final-demand-export`](https://github.com/thenithin342/urban-service-intelligence/tree/feature/final-demand-export)  
**Head Commit SHA:** [`b545d14791fb96d4aaeb54164cee03feea79b038`](https://github.com/thenithin342/urban-service-intelligence/commit/b545d14791fb96d4aaeb54164cee03feea79b038)  
**Pull Request:** [PR #4: feat(member1): safe demand contracts export, adapter integration, and validation](https://github.com/thenithin342/urban-service-intelligence/pull/4)  
**Initial Feature Commit SHA:** [`a0693644364805e3fd6c3b77bd4e6a325e2742b1`](https://github.com/thenithin342/urban-service-intelligence/commit/a0693644364805e3fd6c3b77bd4e6a325e2742b1)  
**Status:** Verification Complete; Pull Request Open against `main` (Unmerged)

---

## 1. Remote GitHub Branch & PR Verification

- **Branch Name:** `feature/final-demand-export`
- **Pull Request:** [https://github.com/thenithin342/urban-service-intelligence/pull/4](https://github.com/thenithin342/urban-service-intelligence/pull/4)
- **Head SHA:** `b545d14791fb96d4aaeb54164cee03feea79b038`
- **Remote Tracking:** `origin/feature/final-demand-export` is fully up-to-date.
- **Commit History on Feature Branch:**
  1. `a069364` — `feat(member1): implement safe demand contract export and adapter verification`
  2. `799a258` — `test(member1): add rollback failure tests and double counting guards`
  3. `7aef5f6` — `test(member1): support clean checkout environments and add synthetic fixture test`
  4. `b545d14` — `fix(member1): add horizon and metadata consistency validation and document publication atomicity limits`
- **Remote CI Run Status:**
  - Pull Request CI Job [`42343275360`](https://github.com/thenithin342/urban-service-intelligence/actions/runs/42343275360): **PASSED** in 4m 35s.
  - Branch Push CI Job [`42343150302`](https://github.com/thenithin342/urban-service-intelligence/actions/runs/42343150302): **PASSED** in 3m 03s.
- **Branch Protection:** This branch has **NOT** been merged into `main`.

---

## 2. Files Changed

Total diff against base commit `3f78671`: **4 files changed, 1,374 insertions(+), 12 deletions(-)**

| File | Change | Description |
|---|---|---|
| `scripts/export_demand_contracts.py` | Created (+474 lines) | Staged exporter for `demand_forecast` (147 rows) and `demand_metrics` (6 rows) with schema/invariant validation, SHA-256 provenance hashing, backup/rollback, missing future labels guard, uniform origin/version/cutoff validation, strict horizon-to-date mapping, and explicit documentation of sequential copy limitations. |
| `tests/unit/test_demand_contracts.py` | Created (+875 lines) | 21 comprehensive unit tests covering roundtrip export, adapter readback, chronological/horizon integrity, numerical bounds, key uniqueness, double-counting guards, write failure rollback, first-time orphan cleanup, incomplete output classification, synthetic fixtures, and new metadata/mapping consistency guards. |
| `docs/data_contracts.md` | Modified (+7, -5 lines) | Updated Section 2.3 contract schemas, metadata keys, and invariants for `demand_forecast` and `demand_metrics`. |
| `docs/member1/demand_contract.md` | Modified (+18, -2 lines) | Documented staging workflow, rollback guarantees, publication safety limitations, and provenance verification commands. |

---

## 3. Corrected Forecast Totals (Double-Counting Resolution)

### Why 79,276.0 was Flawed
The 147 rows in `demand_forecast` comprise:
- 42 rows for total borough demand (`complaint_type == 'ALL'`: 6 geographies × 7 daily horizons)
- 105 rows for modeled category subsets (`complaint_type != 'ALL'`: 15 borough/category pairs × 7 daily horizons)

Because the category forecasts represent modeled subsets of the borough totals, naively summing all 147 rows added category demand into borough totals, causing 21,016 complaints to be double counted ($58{,}260 + 21{,}016 = 79{,}276$).

### True Citywide 7-Day Predicted Demand (Strictly `complaint_type == 'ALL'`)
Computed exclusively across `complaint_type == 'ALL'` rows for the 7-day forecast window (`2024-03-01` to `2024-03-07`):
$$\mathbf{58{,}260.0}\text{ predicted complaints}$$

*Borough-by-Borough Forecast Breakdown:*
- **BROOKLYN:** 17,734.0 complaints
- **QUEENS:** 14,370.0 complaints
- **MANHATTAN:** 12,061.0 complaints
- **BRONX:** 11,933.0 complaints
- **STATEN ISLAND:** 2,116.0 complaints
- **UNKNOWN:** 46.0 complaints
*(Reconciled against Member 1 Phase 6 documentation in `reports/member1/phases5_to_8_results.md`).*

### Modeled Category Subsets (Strictly `complaint_type != 'ALL'`)
Reported independently across the 15 modeled series (7-day forecast sum):
$$\mathbf{21{,}016.0}\text{ predicted complaints}$$

*Category Breakdown:*
- **Illegal Parking:** 9,494.0 complaints
- **HEAT/HOT WATER:** 6,277.0 complaints
- **Noise - Residential:** 5,245.0 complaints

### Double-Counting Regression Guard Test
Added `test_demand_forecast_prevents_double_counting_and_separates_totals()` in `tests/unit/test_demand_contracts.py` to assert:
1. `len(all_rows) == 42` and `all_rows["predicted_count"].sum() == 58260.0`
2. `len(cat_rows) == 105` and `cat_rows["predicted_count"].sum() == 21016.0`
3. For each borough, the sum of category predictions is strictly bounded by the borough total ($\sum \text{category} \le \text{borough\_total}$).

---

## 4. Test Results: Local Environment vs. Remote GitHub CI

### Local Test Execution (Real Artifacts Present)
- **Ruff:** `ruff check src scripts tests dashboard` → `0 errors` (All checks passed).
- **Demand Contracts Suite:** `pytest -v tests/unit/test_demand_contracts.py` → **`21 passed, 1 warning in 6.35s`** (100% pass rate).
- **Member 1 Audit Integration Suite:** `pytest -v tests/integration/test_member1_final_audit.py tests/integration/test_member1_audit.py` → **`7 passed, 1 warning in 138.90s`**.
- **Full Repository Regression Suite:**
  ```powershell
  python -m pytest -q -rs --deselect tests/integration/test_etl_end_to_end.py::test_end_to_end_100k_sample
  ```
  - **Summary:** **`303 passed, 1 skipped, 1 deselected, 2 warnings in 1114.02s`**
  - **Passed:** 303 tests across unit, integration, and dashboard suites.
  - **Skipped (1):** `tests/unit/test_member2_mining.py:102` (real Member 2 mining artifacts absent in local checkout).
  - **Deselected (1):** `tests/integration/test_etl_end_to_end.py::test_end_to_end_100k_sample` (reserved for manual sample-e2e job).

### Remote GitHub CI Execution (Clean Checkout, Artifacts Gitignored)
- **Remote CI Run:** Job ID `128299232060` (PR #4)
- **Summary:** **`275 passed, 34 skipped, 1 deselected, 161 warnings in 4m 35s`**
- **Why CI Counts Differ:**
  - `artifacts/` is gitignored. In clean CI checkout, real data artifacts do not exist.
  - 34 tests skip cleanly with explicit reasons (`tests/app/test_member3_adapters.py` and `tests/app/test_member3_app_smoke.py` skip with `"real Member 1 tables absent"`).
  - 1 test deselected: `test_end_to_end_100k_sample` (same as local).
  - 161 warnings: PySpark / distutils version warnings.
  - **Important Note:** GitHub CI validates code integrity, fixtures, and synthetic tests. It does **not** reproduce real model training or large artifact generation.

---

## 5. Staged Publication & Rollback Audit (Safety & Limitations)

The publication pipeline in `scripts/export_demand_contracts.py` implements a fail-closed staging and rollback architecture:

1. **Pre-Publication Validation Gate:**
   - In-memory validation verifies schema, positive values, complete horizons (1..7), uniform origin, uniform version, uniform training cutoff, strict horizon-to-date mapping (`target_date == origin + horizon_days`), and absent future labels (`actual_count` null guard).
   - If any validation check fails, staging is halted and production is untouched.
2. **Isolated Staging:**
   - Files (`part-00000.parquet` and draft `.meta.json`) are written into `.staging_<contract_name>_<uuid>`.
   - Staged files are read back and validated before replacing production files.
3. **Rollback & Backup Restoration:**
   - Pre-existing valid contract files are copied to a temporary `.backup_<contract_name>_<uuid>`.
   - If a failure occurs during the file copy to production, the backup is restored, ensuring pre-existing valid tables remain intact.
4. **First-Time Orphan Prevention:**
   - If publishing for the first time and an error occurs midway, both destination and staging are cleaned up, leaving no empty or corrupt directories.
5. **Incomplete Output Classification:**
   - `dashboard.adapters.check_table()` checks both Parquet directory contents and sibling `.meta.json`. Partial exports (missing `.meta.json`, empty folder, corrupt metadata) are classified as `invalid` or `missing`, never as `ok` or `real`.
6. **Publication Safety Limitation (No OS Atomic Directory Swap Claim):**
   - File copies (`shutil.copy2` for `part-00000.parquet` followed by `.meta.json`) are **sequential file operations**, not OS-level single-transaction atomic directory swaps.
   - While runtime Python exceptions trigger automatic backup restoration, the system does not claim kernel-level atomic guarantees against hard process aborts (e.g., SIGKILL / sudden power loss midway through file copying).

---

## 6. Contract & Dashboard Verification Results

### Contract Validator (`scripts/check_outputs.py --json`)
- `demand_forecast`: **Status: `ok`**, 147 rows, 0 problems, 0 warnings.
- `demand_metrics`: **Status: `ok`**, 6 rows, 0 problems, 0 warnings.

### Dashboard Adapter Verification (`dashboard.adapters`)
```python
from dashboard import adapters as ad
fc = ad.load_contract("demand_forecast", allow_fixture=False)
met = ad.load_contract("demand_metrics", allow_fixture=False)
```
- `fc.source`: **`"real"`** (147 rows loaded from production Parquet).
- `met.source`: **`"real"`** (6 rows loaded from production Parquet).
- `fc.status`: `"ok"`, `met.status`: `"ok"`.

### Underlying Artifact Provenance & Model Reconciliation
- **Selected Model:** `seasonal_naive` (version `demand-v2-seasonal_naive-2024-02-29`), as recorded in `artifacts/member1/demand_categories/model_manifest.json`.
- **Prediction Values:** Exact numerical parity with `artifacts/member1/demand_categories/predictions.parquet` (`0.0` max absolute deviation).
- **Dates & Windows:**
  - Training Cutoff: `2024-02-24` (history origin `2024-02-29`).
  - Forecast Horizon: `2024-03-01` to `2024-03-07` (consecutive 7 days).
  - Test Evaluation Window: `2024-02-15` to `2024-02-29` (14-day holdout).
- **Candidate Evaluation Metrics (Reconciled from `metrics/` on test split):**
  - `weekday_mean`: MAE = 54.150, RMSE = 103.161, WAPE = 0.1011
  - `lr_scaled`: MAE = 61.194, RMSE = 102.265, WAPE = 0.1143
  - `lr_unscaled`: MAE = 61.226, RMSE = 102.352, WAPE = 0.1144
  - `random_forest`: MAE = 71.614, RMSE = 129.179, WAPE = 0.1338
  - `seasonal_naive`: MAE = 72.718, RMSE = 117.945, WAPE = 0.1358
  - `gbt`: MAE = 87.028, RMSE = 165.534, WAPE = 0.1625
- **Source SHA-256 Checksum:** `3a6a21ea0b32710b39619f54fdbb34eaca75c50e5b351df7a888eee5f2a9f22e` (verified matching raw manifest `data/raw/member1_history/manifest.json`).

---

## 7. Limitations & Operational Boundaries

1. **Retrospective Academic Decision Support:** Prototype designed for retrospective analysis on NYC 311 historical data; not an active dispatch or real-time staffing optimizer.
2. **Study Boundary:** Contract covers the 91-day historical winter study (`2023-12-01` to `2024-02-29`). Multi-season and annual studies remain isolated in their respective folders and are not overwritten into this contract.
3. **Cross-Member Contracts:** Downstream contracts for Member 2 (`pattern_rules`, `resolution_summary`) and Member 3 (`priority_scores`) remain fixtures or missing in this checkout until those workstreams execute.
