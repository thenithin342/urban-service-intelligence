# Urban Service Intelligence

Scalable analytics prototype for NYC 311 service-request data:
demand prediction and transparent resource-allocation review.
Built on NYC Open Data (311 Service Requests from 2020 to Present,
dataset `erm2-nwe9`). A research and education decision-support
prototype. Advisory outputs only, NOT a municipal dispatch tool.

## Project status

- Phase 0 (repository, environment, Spark smoke test): done.
  See `docs/decisions.md`.
- Phase 1 (bounded acquisition, profiling, EDA): done.
  See `reports/data_quality.md` and `reports/methodology.md`.
- Phase 2 (PySpark ETL to partitioned Parquet plus a machine-readable
  quality report): done. See `reports/audit_phase2.md`.
- Member 1 Phase 3 (Spark EDA, spatial analysis, comparative clustering):
  implemented; see `reports/member1/phase3_findings.md` for saved evidence.
- Member 1 Phase 4 (bounded historical expansion and coverage audit):
  implemented; see `reports/member1/premerge_audit.md` for verification and coverage.
- Member 1 Phases 5-8 (demand forecasting, forecast handoff, benchmarks,
  final audit): pending. Mining, closure models and dashboard integration
  belong to Members 2 and 3; their completion is not implied here.
- Member 3 Phases 3-7 (Streamlit dashboard, output adapters and checks,
  advisory priority score + backtest, CI, integration): implemented. The
  dashboard renders Member 1 Phase 3 tables and Member 3 priorities from real
  outputs; Member 2 mining/closure and Member 1 forecast panels show labelled
  synthetic FIXTURES until those producers publish. See
  `reports/member3/integration_report.md`, `docs/member3/priority_scoring.md`
  and `docs/runbook_windows.md`.

The latest Member 1 end-to-end verification, fixes and known limits are recorded
in `reports/member1/premerge_audit.md`.

## Technology stack

Python 3.10, PySpark 3.5.3 (local mode), Java 17, pandas, pyarrow,
scikit-learn, Streamlit, pytest, ruff. Exact pinned versions are in
`requirements.txt`. Parquet is the analytical storage format.

## Setup

Windows PowerShell (primary supported shell):

```powershell
py -V:3.10 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

If the `py` launcher is unavailable, substitute the Python 3.10
interpreter path directly
(`C:\...\Python310\python.exe -m venv .venv`).
Python 3.13 is not supported: Spark Python workers crash on it
(see `docs/decisions.md`).

Linux/macOS:

```bash
python3.10 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Prerequisites: Java 17 (`java -version`) and, on Windows only, a
one-time winutils install outside the repo
(`C:\hadoop\bin\winutils.exe` + `hadoop.dll` from a Hadoop 3.3.x
build, `HADOOP_HOME=C:\hadoop`, `C:\hadoop\bin` on PATH).
`src/utils/spark.py` pins the Spark driver and worker interpreters
to the running interpreter and auto-discovers `C:\hadoop`, so fresh
shells work without manual environment configuration.

## Usage

```powershell
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe scripts/download_sample.py --config configs/sample.yaml
.\.venv\Scripts\python.exe scripts/run_etl.py --config configs/sample.yaml
.\.venv\Scripts\python.exe scripts/run_member1_analytics.py --config configs/sample.yaml
.\.venv\Scripts\python.exe -m streamlit run dashboard/app.py
```

Member 3 priorities and output checks (full sequence from a fresh clone:
`docs/runbook_windows.md`):

```powershell
.\.venv\Scripts\python.exe scripts/run_member3_priorities.py --config configs/sample.yaml --input data/processed/member1_history
.\.venv\Scripts\python.exe scripts/check_outputs.py --allow-missing
```

The dashboard (`dashboard/app.py`, pages in `dashboard/views/`) reads only
small aggregate artifacts through `dashboard/adapters.py`, never the raw or
cleaned record-level data, and labels every panel REAL, FIXTURE or MISSING.

The original test, sample download, ETL and dashboard commands were verified
on the standard environment on 2026-10-14:
33 pytest tests passed; the dashboard additionally smoke-tested
headless (HTTP 200, no exceptions). `run_etl.py` also accepts
`--snapshot-date YYYY-MM-DD` and `--master`.

Member 1 historical expansion (separate from the 100k sample):

```powershell
.\.venv\Scripts\python.exe scripts/download_history.py --config configs/history.yaml
.\.venv\Scripts\python.exe scripts/run_history_etl.py --config configs/history.yaml
```

Downloads resume verified daily checkpoints. Row/storage limits fail explicitly
instead of truncating the time window. See `docs/member1/history_contract.md`.

## Repository layout

```text
configs/      Fixed acquisition/ETL windows (sample + full-scale)
data/         raw/ + processed/ (gitignored); manifests/ (small provenance JSON)
src/          ingestion, processing, analytics, mining, modeling,
              prioritization, evaluation, utils
scripts/      Sample/history downloads, ETL, Member 1 analytics (working);
              build_features, train_models, benchmark (later-phase stubs)
tests/        unit/, integration/, fixtures/
dashboard/    Streamlit app (reads computed artifacts only)
notebooks/    EDA notebooks (render src/ logic, hold no pipeline code)
artifacts/    Generated outputs (gitignored)
reports/      Data-quality, methodology, audit reports
docs/         Architecture, data dictionary, decisions log
```

## Data provenance and reproducibility

- Source: https://data.cityofnewyork.us/d/erm2-nwe9
- Every download writes `data/raw/*.csv` plus
  `data/manifests/manifest_*.json` (request URLs, filters,
  acquisition time, row counts, SHA256, failures, source metadata).
- Raw data, processed data, and artifacts are gitignored. Only code,
  configs, documentation, small manifests, and test fixtures are
  committed.
- Fixed historical `where` windows, stable `$order`, recorded seed
  (42), deterministic pandas and Spark transformations. Re-running the
  pipeline over the same inputs reproduces identical outputs
  (row-hash verified in `reports/audit_phase2.md`).

## Testing

`python -m pytest -q` runs unit tests (ingestion helpers, EDA logic,
ETL contract, CLI validation), Spark integration tests (real
Python-worker transformations, ETL edge cases, idempotent reruns),
and an end-to-end ETL test over the real 100,000-record sample.
`tests/app/` holds the dashboard adapter, check and Streamlit AppTest smoke
tests. `ruff check src scripts tests dashboard` must be clean. CI
(`.github/workflows/ci.yml`) runs lint and pytest on Python 3.10 / Java 17.

## Limitations

Single-machine local Spark (not a cluster); current sample covers
2024-01-01 to 2024-01-12; complaint volume measures reporting
frequency, not infrastructure quality; rankings, when added, will be
advisory absent real capacity and cost data. See `docs/decisions.md`
and phase progress reports for the full list.
