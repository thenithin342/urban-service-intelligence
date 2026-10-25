# Urban Service Intelligence

Scalable analytics prototype for NYC 311 service-request data:
demand prediction and transparent resource-allocation review.
Built on NYC Open Data (311 Service Requests from 2020 to Present,
dataset `erm2-nwe9`). A research and education decision-support
prototype. Advisory outputs only, NOT a municipal dispatch tool.

## Project status

- Phase 0 (repository, environment, Spark smoke test): done.
  See `PROGRESS_PHASE0_PHASE1.md`.
- Phase 1 (bounded acquisition, profiling, EDA): done.
  See `PROGRESS_PHASE0_PHASE1.md` and `reports/data_quality.md`.
- Phase 2 (PySpark ETL to partitioned Parquet plus a machine-readable
  quality report): done. See `PROGRESS_PHASE2.md` and
  `reports/audit_phase2.md`.
- Member 1 Phase 3 (Spark EDA, spatial analysis, comparative clustering):
  implemented; see `PROGRESS_MEMBER1_PHASE3.md` for saved evidence.
- Member 1 Phase 4 (bounded historical expansion and coverage audit):
  see `PROGRESS_MEMBER1_PHASE4.md` for current verification and data coverage.
- Member 1 Phases 5-8 (demand forecasting, forecast handoff, benchmarks,
  final audit): pending. Mining, closure models and dashboard integration
  belong to Members 2 and 3; their completion is not implied here.

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
`ruff check src scripts tests` must be clean.

## Limitations

Single-machine local Spark (not a cluster); current sample covers
2024-01-01 to 2024-01-12; complaint volume measures reporting
frequency, not infrastructure quality; rankings, when added, will be
advisory absent real capacity and cost data. See `docs/decisions.md`
and phase progress reports for the full list.
