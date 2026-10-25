# Member 3 integration report (Phases 3-7)

Date of runs: 2026-10-25. Machine: Windows 11, local Spark (`local[2]`).
Environment: `.venv` Python 3.10, PySpark 3.5.3, pandas 2.3.3,
Streamlit 1.65.0, Plotly 7.1.0, Java 17, winutils in `C:\hadoop`. No new
dependencies were added.

Every number below comes from a command executed on that date. Counts are
reported-complaint frequency, not per-capita rates.

## 1. Data used

| Dataset | Path | Records | Coverage |
| :--- | :--- | ---: | :--- |
| 100k sample (cleaned) | `data/processed/nyc311_clean` | 100,000 | 2024-01-01 to 2024-01-12 (2024-01-12 partial: 191 records) |
| Member 1 history (cleaned) | `data/processed/member1_history` | 799,439 | 2023-12-01 to 2024-02-29 (91 days, 0 missing days) |
| Member 1 Phase 3 tables | `artifacts/member1/analytics/tables` | 16 tables, 188 KB on disk | sample window |

## 2. Dashboard: pages and panel sources

Navigation (`dashboard/app.py`, `st.navigation`): Analytics (Overview, Trends,
Hotspots, Resolution), Models (Pattern Explorer, Forecasts and Closure),
Decision support (Priorities, Output Status).

| Page / panel | Table | Source at time of writing |
| :--- | :--- | :--- |
| Overview, Trends, Hotspots (all panels) | Member 1 Phase 3 tables | REAL (sample, Jan 1-12 2024) |
| Resolution: duration by borough | `resolution_by_borough` | REAL (sample) |
| Resolution: 7-day closure by borough x type | `resolution_summary` | FIXTURE (Member 2 not yet published) |
| Pattern Explorer (rules, itemsets, threshold runs) | `pattern_rules`, `frequent_itemsets`, `mining_threshold_experiments` | FIXTURE |
| Forecasts: demand forecast and metrics | `demand_forecast`, `demand_metrics` | FIXTURE (Member 1 forecasts pending) |
| Closure: model comparison, cohort predictions | `closure_model_comparison`, `closure_cohort_predictions` | FIXTURE |
| Priorities (breakdown, table, map, backtest) | `priority_scores`, `priority_backtest` | REAL (history, borough level) |
| Output Status | all 10 contract tables | live check |

Fixture panels carry a warning "FIXTURE - synthetic contract fixture with
invented values ... NOT results" and the page banner lists them under FIXTURE.
As soon as a producer writes a contract-valid table with its `.meta.json`, the
adapters switch that panel to REAL with no code change (verified by the
export/import test, section 4).

The app never loads raw or cleaned record-level data: all reads go through
`dashboard/adapters.py`, and `test_app_reads_only_small_artifact_tables`
records every `pandas.read_parquet` call while rendering all pages and asserts
each path is under `artifacts/` (no `nyc311_clean` / `member1_history`).
Artifacts read by the app total about 250 KB (Member 1 tables 188 KB,
Member 3 tables 60 KB).

## 3. Priority score and backtest (measured)

Definition: `docs/member3/priority_scoring.md`. Score =
0.5 x demand + 0.3 x backlog + 0.2 x delay (min-max normalized components);
demand and backlog are observed (no real Member 1 forecast / Member 2 cohort
outputs exist yet; the meta says so). Advisory operational priority, not a
staffing optimum. Window 7 days, step 7, k in {10, 25, 50}, random seed 42.

Command (published artifacts):
`python scripts/run_member3_priorities.py --config configs/sample.yaml --input data/processed/member1_history`

- Input 799,439 records; Spark aggregates: 139,956 `created` rows, 151,740
  `delay` rows, 5 location centroids. Aggregation 30.89 s, total 37.89 s
  (wall clock incl. JVM start 39.4 s).
- Scoring window 2024-02-23 to 2024-02-29: 612 borough x complaint-type units.
- Backtest: 12 rolling windows (window ends 2023-12-07 ... 2024-02-22), mean
  universe 600.2 units.

| Method | k | mean precision@k | mean Spearman |
| :--- | ---: | ---: | ---: |
| priority_score | 10 | 0.525 | 0.709 |
| volume_only | 10 | 0.892 | 0.938 |
| random | 10 | 0.017 | -0.006 |
| priority_score | 25 | 0.527 | 0.709 |
| volume_only | 25 | 0.863 | 0.938 |
| random | 25 | 0.050 | -0.006 |
| priority_score | 50 | 0.587 | 0.709 |
| volume_only | 50 | 0.892 | 0.938 |
| random | 50 | 0.077 | -0.006 |

Per-window priority_score precision@10 ranged 0.5-0.6; per-window Spearman
ranged 0.644-0.854 (volume_only 0.909-0.961, random -0.083 to 0.041).

Additional runs (written to a scratch root, not published):

- ZIP level, same history: 8,335 units in the scoring window, mean universe
  8,153.7, 12 windows, runtime 50.22 s. precision@10/25/50:
  priority_score 0.658 / 0.603 / 0.515, volume_only 0.675 / 0.723 / 0.777,
  random 0.000 / 0.007 / 0.010; mean Spearman priority_score 0.594, volume_only 0.699, random -0.000.
- 100k sample, borough, `--window-days 3 --step-days 3`: 2024-01-12 dropped
  as partial, 11 full days, 2 windows, 548 units. precision@10/25/50:
  priority_score 0.60 / 0.64 / 0.64, volume_only 0.90 / 0.90 / 0.88,
  random 0.00 / 0.10 / 0.11; mean Spearman priority_score 0.845, volume_only 0.916, random 0.003. Two windows are
  far too few to draw conclusions.

Interpretation: the score is far better than random at anticipating where
complaints keep arriving, but below the last-window volume baseline at
predicting next-window volume. That is expected, because the score
deliberately adds backlog and service delay; it measures how much the ranking
departs from pure volume, not whether it is the right allocation.

## 4. Tests, lint and launch (measured)

| Command | Result | Time |
| :--- | :--- | ---: |
| `.venv\Scripts\python.exe -m ruff check src scripts tests dashboard` | All checks passed | 0.3 s |
| `.venv\Scripts\python.exe -m pytest -q -rs --durations=15` (full suite) | 223 passed, 0 skipped, 0 failed | 1151.87 s (19 min 12 s) |
| `pytest -q tests/app` (adapters, checks, AppTest smoke, export/import) | 90 passed | 24.04 s |
| `pytest -q tests/unit/test_member3_priority.py tests/unit/test_contracts.py` | 41 passed | 2.05 s |
| `pytest -q tests/integration/test_member3_priority_spark.py` (Spark vs pandas aggregate, CLI) | 3 passed | 42.37 s |
| `pytest -q tests/integration/test_spark_worker.py tests/integration/test_spark_smoke.py tests/integration/test_etl_end_to_end.py` (real Spark workers, ETL smoke on the real 100k sample) | 6 passed | 66.99 s |
| `streamlit run dashboard/app.py --server.headless true` | HTTP 200 on `/` and `/_stcore/health` = `ok` after 2.1-2.5 s; process stopped | - |
| `python scripts/check_outputs.py` | exit 1: 2/10 ok (priority tables), 8 fixture-only | - |
| `python scripts/check_outputs.py --allow-missing` | exit 0 (fixture-only reported as WARN) | - |

The slowest tests are Member 1 Spark integration tests
(`test_preprocessing_ablation_runs` 326.6 s, `test_kmeans_and_summary`
155.1 s). App smoke tests run every page through the real `st.navigation`
entry with (a) the repository artifacts, (b) an empty artifacts root with
fixtures and (c) an empty root without fixtures (empty states), with no
exceptions.

CI: `.github/workflows/ci.yml` (Python 3.10, Temurin Java 17, ruff, pytest,
`check_outputs.py --allow-missing`). The default job deselects the real-sample
ETL test because data is not in git; a manual `sample-e2e` job downloads the
sample and runs it. The workflow has not been executed on GitHub from this
environment (nothing was pushed).

## 5. Contract changes

None. `priority_scores` additionally carries `window_count`, `delay_records`,
`center_lat`, `center_lon` as extra columns, which the contract allows
(adding columns is backward compatible; `validate` checks required columns
only).

## 6. Limitations

- Member 2 mining/closure and Member 1 forecast panels are fixture-backed; the
  priority score therefore uses observed demand and backlog.
- Phase 3 dashboard tables cover only Jan 1-12 2024 (not seasonal); priorities
  use three winter months, not a full annual cycle.
- Reporting bias and absolute (not per-capita) volume; weights are a policy
  choice, not optimized; min-max components are only comparable within a run.
- Delay median comes from a fixed-bin histogram (exact to bin width).
- `closed_at` is an acquisition snapshot taken long after the windows.
- Output staleness compares against the newest cleaned dataset; outputs built
  from the sample are flagged stale once the history (later coverage end) is
  present, which is intended but conservative.
- The headless launch check confirms the server starts and serves; page
  rendering is verified through `streamlit.testing.v1.AppTest`, not a browser.
