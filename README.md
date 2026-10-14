# Urban Service Demand Prediction and Intelligent Resource Allocation

Research/educational decision-support prototype using NYC 311 Service Requests
(2020-Present, dataset `erm2-nwe9`). Advisory outputs only, NOT a dispatch tool.

## Status

- Phase 0 (repo + env + Spark smoke): implemented, see PROGRESS_PHASE0_PHASE1.md
- Phase 1 (acquisition + profiling + EDA): implemented, see reports/data_quality.md
- Phases 2-8: not started (stubs exist for CLI entry points)

## Setup (Windows PowerShell)

```powershell
# Python 3.13 is the default interpreter on this machine.
# Full Spark Python-worker ops require Python 3.10 (see docs/decisions.md).
py -V:3.13 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Linux/macOS:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Java 17 is required for Spark. Verify with `java -version`.

Spark on Windows: set before running Spark code:

```powershell
$env:PYSPARK_PYTHON = (Get-Command python).Source
$env:PYSPARK_DRIVER_PYTHON = (Get-Command python).Source
```

If Python-worker tasks crash on Python 3.13, re-run Spark Python-worker
checks with Python 3.10 + PySpark 3.5.3 (verified working on this machine).

## Commands (only those verified below actually work)

```powershell
python -m pytest -q
python scripts/download_sample.py --config configs/sample.yaml
streamlit run dashboard/app.py
```

Stubs (return exit code 2 until their phase lands):
`run_etl.py`, `build_features.py`, `train_models.py`, `benchmark.py`.

## Data provenance

- Source: https://data.cityofnewyork.us/d/erm2-nwe9
- Every download writes `data/raw/*.csv` + `data/manifests/manifest_*.json`
  (URLs, filters, acquisition time, row counts, SHA256, failures).
- Raw data and artifacts are gitignored. Commit only code + small manifests.

## Reproducibility

- Fixed historical `where` windows in `configs/*.yaml`, stable `$order`,
  deterministic pandas groupby/sort, `seed: 42` recorded in manifests.
- Re-running on the same saved sample yields identical EDA outputs
  (covered by `test_save_eda_outputs_deterministic`).
