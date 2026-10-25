# Dashboard and adapters (Member 3)

Entry point `dashboard/app.py` builds one `st.navigation` menu; pages are
plain scripts in `dashboard/views/`. All data access goes through
`dashboard/adapters.py`; pages never open files themselves (enforced by
`tests/app/test_member3_adapters.py::test_views_do_not_read_files_directly`).

## Panels and sources

| Page | Panel | Table(s) | Producer | Fallback |
| :--- | :--- | :--- | :--- | :--- |
| Overview | KPIs, borough / agency / category bars, flag overlap | `complaints_by_borough`, `complaints_by_agency`, `complaints_by_category`, `borough_category`, `flag_overlap`, `phase3_manifest.json`, `etl_quality.json` | Member 1 | empty state |
| Trends | daily (total or by borough), weekly, hour x weekday | `complaints_daily`, `complaints_daily_borough`, `complaints_weekly`, `hour_weekday` | Member 1 | empty state |
| Hotspots | grid-cell map, ZIP hotspots, KMeans summary | `spatial_hotspots`, `spatial_grid_cells`, `zip_hotspots`, `cluster_summary` | Member 1 | empty state |
| Resolution | duration by borough; 7-day closure by borough x type | `resolution_by_borough`; `resolution_summary` | Member 1; Member 2 | fixture |
| Pattern Explorer | rules (filters), threshold runs, itemsets | `pattern_rules`, `mining_threshold_experiments`, `frequent_itemsets` | Member 2 | fixture |
| Forecasts and Closure | demand forecast + metrics; closure model comparison + cohorts | `demand_forecast`, `demand_metrics`, `closure_model_comparison`, `closure_cohort_predictions` | Member 1; Member 2 | fixture |
| Priorities | score breakdown, table, map, backtest | `priority_scores`, `priority_backtest` | Member 3 | empty state |
| Output Status | `check_outputs` report per contract table | all contract tables | - | - |

## Rules

- `TableResult.source` is `real`, `fixture`, `missing` or `invalid`. Fixture
  panels always show a FIXTURE warning ("invented values ... NOT results");
  invalid tables are never rendered, their contract violations are listed.
- Every page shows a coverage banner built from the manifests/meta
  (e.g. "2024-01-01 to 2024-01-12 (12 days; short sample window - not seasonal
  conclusions)") and lists which tables are REAL / FIXTURE / MISSING.
- Readers are `st.cache_data` functions keyed by path and file mtime, so a
  re-published table is picked up on the next rerun.
- Maps pass through `adapters.valid_map_points`, which drops null and
  out-of-bbox coordinates and asserts none remain.
- Roots: `USI_ROOT`, `USI_ARTIFACTS_ROOT`, `USI_FIXTURES_DIR`,
  `USI_DISABLE_FIXTURES=1` (tests use these to run against temporary trees).
