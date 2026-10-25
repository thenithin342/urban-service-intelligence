# Windows local runbook (fresh clone to dashboard)

Single-machine, local-mode Spark. All commands run from the repository root in
Windows PowerShell. Generated data and artifacts are gitignored; nothing below
needs network access except the two download steps.

## 0. Prerequisites (one time)

- Python 3.10 (3.13 is not supported: Spark workers crash on it).
- Java 17 on PATH: `java -version`.
- winutils for Hadoop 3.3.x: `C:\hadoop\bin\winutils.exe` and `hadoop.dll`.
  `src/utils/spark.py` sets `HADOOP_HOME=C:\hadoop` and PATH automatically when
  that file exists; otherwise set `HADOOP_HOME` and add `C:\hadoop\bin` to PATH.
- About 2 GB free disk for the 3-month history (CSV + Parquet).

## 1. Environment

```powershell
git clone <repo-url> Bigdata_project
cd Bigdata_project
py -V:3.10 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
.\.venv\Scripts\python.exe -m ruff check src scripts tests dashboard
```

## 2. 100k sample (January 1-12 2024) and ETL

```powershell
.\.venv\Scripts\python.exe scripts/download_sample.py --config configs/sample.yaml
.\.venv\Scripts\python.exe scripts/run_etl.py --config configs/sample.yaml
```

Outputs: `data/raw/nyc311_sample.csv`, `data/manifests/manifest_sample.json`,
`data/processed/nyc311_clean/` (Parquet, `_SUCCESS`),
`data/processed/etl_quality.json`.

## 3. Member 1 analytics (Phase 3 tables used by the dashboard)

```powershell
.\.venv\Scripts\python.exe scripts/run_member1_analytics.py --config configs/sample.yaml
```

Outputs: `artifacts/member1/analytics/tables/*`, `phase3_manifest.json`,
`phase3_metrics.json`, `figures/`.

## 4. Member 1 historical expansion (Dec 2023 - Feb 2024, optional but used for priorities)

```powershell
.\.venv\Scripts\python.exe scripts/download_history.py --config configs/history.yaml
.\.venv\Scripts\python.exe scripts/run_history_etl.py --config configs/history.yaml
```

Ready when `data/processed/member1_history/_SUCCESS` and
`artifacts/member1/phase4/etl_quality.json` exist. Run the two steps
sequentially; downloads resume verified daily checkpoints.

## 5. Member 2 mining and resolution (when available)

Being written by Member 2; until they exist the dashboard shows the labelled
contract fixtures for these panels.

```powershell
.\.venv\Scripts\python.exe scripts/run_member2_mining.py --config configs/sample.yaml
.\.venv\Scripts\python.exe scripts/run_member2_resolution.py --config configs/sample.yaml
```

Expected outputs (contract `docs/data_contracts.md`):
`artifacts/member2/mining/tables/{pattern_rules,frequent_itemsets,threshold_experiments}`,
`artifacts/member2/resolution/tables/{resolution_summary,model_comparison}`,
`artifacts/member2/resolution/predictions/closure_cohorts`, each with a sibling
`.meta.json`. Member 1 demand forecasts
(`artifacts/member1/forecasts/tables/{demand_forecast,demand_metrics}`) are
pending likewise.

## 6. Member 3 priorities

```powershell
# Borough level over the 3-month history (recommended)
.\.venv\Scripts\python.exe scripts/run_member3_priorities.py --config configs/sample.yaml --input data/processed/member1_history
# Sample only (12 days: use a 3-day window so the backtest has windows)
.\.venv\Scripts\python.exe scripts/run_member3_priorities.py --config configs/sample.yaml --window-days 3 --step-days 3
# ZIP level
.\.venv\Scripts\python.exe scripts/run_member3_priorities.py --config configs/sample.yaml --input data/processed/member1_history --location-type zip
```

Each run overwrites `artifacts/member3/priorities/tables/{priority_scores,priority_backtest}`
(plus `.meta.json`). If real Member 1 forecasts or Member 2 closure cohorts are
present they replace the observed demand/backlog inputs (borough level); the
meta states which source was used.

## 7. Output checks

```powershell
.\.venv\Scripts\python.exe scripts/check_outputs.py                 # exit 1 on any problem
.\.venv\Scripts\python.exe scripts/check_outputs.py --allow-missing # missing/fixture-only = warning
.\.venv\Scripts\python.exe scripts/check_outputs.py --json
```

Reports missing, fixture-only, contract violations, missing model_version and
stale outputs (generated before the input data was written, or coverage_end
before the latest cleaned coverage).

## 8. Dashboard

```powershell
.\.venv\Scripts\python.exe -m streamlit run dashboard/app.py
# headless (servers/CI):
.\.venv\Scripts\python.exe -m streamlit run dashboard/app.py --server.headless true --server.port 8501
```

Pages: Overview, Trends, Hotspots, Resolution, Pattern Explorer, Forecasts and
Closure, Priorities, Output Status. The app reads only small aggregate tables
and JSON manifests; it never loads the raw CSV or cleaned record-level
Parquet. Every page shows a coverage banner and labels each panel REAL,
FIXTURE (synthetic, not results) or MISSING. Point it at another tree with
`$env:USI_ROOT = "D:\other_clone"` (and optionally `USI_ARTIFACTS_ROOT`).

## 9. Tests

```powershell
.\.venv\Scripts\python.exe -m pytest -q                       # full suite (Spark tests are slow)
.\.venv\Scripts\python.exe -m pytest -q tests/app             # dashboard adapters + AppTest smoke
.\.venv\Scripts\python.exe -m pytest -q tests/unit/test_member3_priority.py tests/integration/test_member3_priority_spark.py
```

`tests/integration/test_etl_end_to_end.py` requires the downloaded 100k sample
(step 2). Real-artifact dashboard tests skip with an explicit reason when
`artifacts/` is absent.

## Troubleshooting

- `Python worker exited unexpectedly`: wrong interpreter; use the `.venv`
  Python 3.10 (driver/worker are pinned by `src/utils/spark.py`).
- `HADOOP_HOME and hadoop.home.dir are unset` / Parquet write fails: winutils
  missing (step 0).
- Port in use: pass another `--server.port`.
- Dashboard shows FIXTURE on a panel: the upstream table has not been
  produced; run the producer step above, then reload (cached readers re-read
  when files change).
