# Shared data contracts (Phases 4-7)

Contract version: `1.0.0` (2026-10-25). Machine-readable source of truth:
`src/utils/contracts.py` (`CONTRACTS`, `validate`, `write_table`).
Member 1 Phase 3 tables keep their own contract in
`docs/member1/artifact_contract.md`; this document covers the outputs added
for mining, resolution, demand forecasts and prioritization.

Naming follows the existing repository convention: Member 1 = A (data,
forecasts), Member 2 = B (mining, resolution), Member 3 = C (dashboard,
prioritization, integration).

## 1. General rules

- Every table is a directory of Parquet files (Spark `write.parquet` or
  `contracts.write_table`, which writes `part-00000.parquet`). Readers use
  `pandas.read_parquet(<dir>)`.
- Every table has a sibling `<dir>.meta.json` with at least
  `contract_version`, `producer`, `generated_at_utc`, `is_fixture`, plus
  `coverage_start`/`coverage_end` and `model_version` where applicable.
- Producers call `validate()` before publishing; the dashboard calls it after
  reading and refuses to render a table that violates its contract.
- All outputs live under the gitignored `artifacts/` tree. Only code, docs and
  the small synthetic fixtures in `tests/fixtures/contracts/` are committed.
- Fixtures carry `"is_fixture": true` and invented values. The dashboard
  labels any fixture-backed panel as such; a fixture is never a result.
- Dates are NYC wall-clock calendar dates (`YYYY-MM-DD`). Counts are reported
  complaint frequency, not population-adjusted rates.
- Adding a column is backward compatible. Removing or retyping a column, or
  changing grain, requires a contract version bump and a coordinated PR.

## 2. Tables

Types: `string`, `int`, `float`, `date`, `list` (Parquet list of strings),
`bool`. Columns marked `?` are nullable; all others must be non-null.

### 2.1 Member 2 (B) - pattern mining

`artifacts/member2/mining/tables/pattern_rules` - one row per association rule.

| Column | Type | Meaning |
| :--- | :--- | :--- |
| `antecedent` | list | Items on the left-hand side (sorted) |
| `consequent` | list | Items on the right-hand side |
| `support` | float | Fraction of baskets containing antecedent and consequent |
| `confidence` | float | P(consequent given antecedent) |
| `lift` | float | confidence / support(consequent) |
| `rule_baskets` | int | Absolute number of baskets supporting the rule |
| `n_baskets` | int | Total baskets in the run |
| `min_support`, `min_confidence` | float | Thresholds used |
| `basket_definition` | string | Human-readable basket semantics |
| `coverage_start`, `coverage_end` | date | Created-date coverage of input |
| `model_version` | string | Producer version |

`artifacts/member2/mining/tables/frequent_itemsets` - `items` (list),
`item_count`, `freq`, `support`, `n_baskets`, `model_version`.

`artifacts/member2/mining/tables/threshold_experiments` - one row per
(min_support, min_confidence) run: `n_baskets`, `n_itemsets`, `n_rules`,
`median_lift?`, `max_lift?`, `runtime_seconds`.

### 2.2 Member 2 (B) - resolution and closure

`artifacts/member2/resolution/tables/resolution_summary` - grain
borough x complaint_type: `borough?`, `complaint_type`, `total_count`,
`eligible_count` (label-mature records), `closed_7d_count`,
`closure_rate_7d?`, `median_resolution_hours?`, `p90_resolution_hours?`,
`unresolved_count`, `coverage_start`, `coverage_end`.

`artifacts/member2/resolution/tables/model_comparison` - one row per model x
split: `model`, `split`, `n`, `positive_rate`, `pr_auc?`, `roc_auc?`,
`brier`, `precision_at_05`, `recall_at_05` (threshold 0.5), `model_version`.
The meta JSON records `train_cutoff` and the test window.

`artifacts/member2/resolution/predictions/closure_cohorts` - grain
borough x complaint_type over a stated prediction window:
`borough?`, `complaint_type`, `n_complaints`, `mean_closure_probability`,
`expected_open_after_7d` (= sum of 1 - p), `observed_closure_rate_7d?`,
`prediction_window_start`, `prediction_window_end`, `model_version`.
This is the backlog input to the priority score.

### 2.3 Member 1 (A) - demand forecasts (pending)

`artifacts/member1/forecasts/tables/demand_forecast` - grain
forecast_date x borough x complaint_type: `forecast_date`, `borough?`,
`complaint_type`, `predicted_count`, `training_cutoff`, `model_version`.

`artifacts/member1/forecasts/tables/demand_metrics` - `model`, `mae`,
`rmse`, `wape?` (null when the denominator is zero), `eval_start`,
`eval_end`, `model_version`.

Until Member 1 publishes these, the dashboard shows the fixture with a
fixture label and the priority score uses trailing observed demand.

### 2.4 Member 3 (C) - prioritization

`artifacts/member3/priorities/tables/priority_scores` - one row per
location x complaint_type for one scoring window:
`location_type` (`borough` or `zip`), `location`, `complaint_type`,
raw inputs `demand_value`, `backlog_value`, `delay_value?`,
normalized `demand_component`, `backlog_component`, `delay_component`
(each in [0, 1]), `priority_score`, `rank` (1 = highest), `window_start`,
`window_end`, `scoring_version`.

`artifacts/member3/priorities/tables/priority_backtest` - one row per
historical window x method x k: `window_start`, `window_end`,
`target_start`, `target_end`, `method`, `k`, `precision_at_k`,
`spearman?`, `n_units`.

## 3. Feature timing rule

Any value used to predict or rank as of time T must be computable from
records created before T. `closed_at`, `status_snapshot` and
`resolution_hours` describe the acquisition snapshot, so they may be used as
labels or as historical delay/backlog measured strictly before T, never as
features of the record being predicted.
