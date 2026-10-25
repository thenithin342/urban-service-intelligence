# Member 1 Phase 4 - Historical expansion and completeness audit

Date: 2026-10-25. Branch: `feature/data-analytics`.
Baseline commit: `4da3e9810abd2ea5c275e4cb5dbe05bace7cd947`.
Existing uncommitted Phase 3 corrections were retained.
Status: PASS for the bounded Phase 4 expansion. Real acquisition, ETL,
readback, full regression suite and lint all passed with saved evidence.

## Document and implementation review

Reviewed the Member 1 master handoff, three-member execution plan, README,
AGENTS, Phase 0/1/2/3 progress, architecture, decisions, data dictionary,
methodology, quality/audit reports, Member 1 findings and artifact contracts.
The README's claim that Phase 3 had not started was stale. Phase 3 code,
tables, figures, metrics and correctness audit were already present.
The previous Phase 3 report recorded 67 passing tests. The current full-suite
rerun passed all 76 tests, including the existing Phase 3 checks and 9 new tests.

The user's request to move to the next part authorizes Phase 4 after the prior
Phase 3 review boundary. Member 2 mining/closure code and Member 3 dashboard/
priority code remain outside this implementation. No PR, push or merge made.

## Working implementation

- `src/ingestion/history.py`, `scripts/download_history.py`: preflight schema
  validation, whole-window daily counts, deterministic keyset pagination,
  bounded CSV streaming, checksums and atomic complete-day checkpoints.
  Interrupted days restart; complete verified days resume. Count changes,
  duplicate keys, unexpected dates and row/storage limits fail explicitly.
- `configs/history.yaml`: 2023-12-01 through exclusive 2024-03-01,
  50,000-row pages, 1,200,000-row and 600,000,000-byte bounds.
- `src/processing/history.py`, `scripts/run_history_etl.py`: existing Phase 2
  cleaner, checksum validation, unique-key and daily reconciliation, acquisition
  snapshot dates, separate historical Parquet and readback count verification.
- Unit and real Spark integration tests; README and historical contract updated.
  Corrected the remaining stale histogram quantiles in the Phase 3 contract.
  Qualified unsupported seasonal/causal and staffing claims in the older report;
  observed complaint counts alone do not establish those conclusions.

## Measured real-data results

Source: NYC Open Data `erm2-nwe9`; live metadata checked during acquisition.
Event dates: 2023-12-01 through 2024-02-29 inclusive (91 calendar days).
Acquisition snapshot date: 2026-10-25 UTC, distinct from event dates.

| Month | Source-verified records |
| --- | ---: |
| 2023-12 | 271,817 |
| 2024-01 | 287,186 |
| 2024-02 | 240,436 |
| Total | 799,439 |

- Source daily counts matched before and after acquisition.
- Raw CSV bytes: 122,939,797. All 91 requested days accounted for; no
  zero-complaint days. A second actual acquisition run reused 91 checkpoints.
- ETL input/output/readback: 799,439 records. Distinct keys: 799,439.
  Excluded missing keys: 0. Duplicates removed: 0. Missing event days: 0.
- Flagged records: 27,935; unflagged: 771,504.
- Flag occurrences: unresolved 14,483; invalid coordinates 11,289;
  invalid ZIP 8,573; missing borough 599; negative duration 377.
  Occurrences overlap and must not be summed as a flagged-record count.
- Missing resolution durations: 14,860 (14,483 unresolved + 377 negative).
  Durations remain null; coordinates were not imputed.
- Original sample SHA256 remains
  `7ed7541b56fa816459280483cf0d52f9d98b05bf34e884fc113afb87feafb652`.

## Commands and saved evidence

Commands from the repository root, using Python 3.10 and PySpark 3.5.3:

```powershell
.\.venv\Scripts\python.exe scripts/download_history.py --config configs/history.yaml
.\.venv\Scripts\python.exe scripts/download_history.py --config configs/history.yaml
.\.venv\Scripts\python.exe scripts/run_history_etl.py --config configs/history.yaml
.\.venv\Scripts\python.exe -m pytest -q tests/unit/test_history.py tests/integration/test_history_etl.py --basetemp artifacts/member1/phase4/tmp-targeted-isolated
.\.venv\Scripts\python.exe -m pytest -q --basetemp artifacts/member1/phase4/tmp-full
.\.venv\Scripts\python.exe -m ruff check src scripts tests
```

The real acquisition ran under installed Python 3.13 (HTTP/CSV only); the Spark
ETL and integration tests used `.venv` Python 3.10.0 and PySpark 3.5.3.
`PYTEST_DISABLE_PLUGIN_AUTOLOAD=1` isolated unrelated system test plugins for
the targeted Spark verification. The new 9 tests passed in 49.92 seconds;
the later unit-only rerun passed 8 tests in 0.82 seconds. Lint passed.
The Python-worker probe returned the expected count of 2.

Final full suite (same plugin isolation): **76 passed, 1 warning in 785.32
seconds (13:05)**. The warning is PySpark's existing `distutils` deprecation.
The long clustering step was checked with Java thread dumps; task stages
advanced and the run completed successfully. This test duration is not a
Spark/Pandas benchmark. Full output is saved in `artifacts/member1/phase4/pytest.txt`.

Saved local evidence (ignored, never committed):

- `data/raw/member1_history/`: 91 daily CSVs, checkpoint JSONs,
  request identity, source metadata and complete acquisition manifest.
- `data/processed/member1_history/`: cleaned Parquet, partitioned year/month.
- `artifacts/member1/phase4/download.txt`, `resume.txt`, `etl_run.txt`,
  `etl_quality.json`, `history_tests.txt`, `history_unit.txt`, `worker_probe.txt`,
  `ruff.txt`, `reconciliation.json`, `runtime.json`: actual command output,
  provenance reconciliation and runtime versions.

## Environment issues and limits

The documented `.venv` was absent. Recreated it from installed Python 3.10,
reusing installed PySpark 3.5.3 through system-site-packages. The first dependency
installation encountered sandbox DNS restrictions; the expanded install then
timed out downloading PyArrow. Required analytics libraries were restored
separately with a longer timeout: pandas 2.3.3, NumPy 2.2.6, matplotlib 3.10.9,
Plotly 7.1.0, pytest 9.1.1 and ruff 0.17.0. No dependency pins were changed.
The complete requirements installation was not finished (PyArrow and dashboard
dependencies are not part of this verification). This recovery venv exposes
system packages; pip reported an unrelated system TensorFlow/NumPy conflict.
TensorFlow is not used here. A fully isolated venv installed from requirements
remains the recommended setup for other machines.
Java gateway creation failed inside Windows sandbox temp storage; real Spark
verification succeeded with approved local execution outside that sandbox.

This is an initial three-month expansion spanning two calendar years, not the
aspirational full 2022-2025 archive. It supports a short-horizon forecasting
experiment next; it cannot establish annual seasonality. The CSV budget excludes
metadata, temporary overhead and Parquet storage. Source count checks cannot
detect equal-count substitutions or updates to historical status fields.
Checkpoints can represent different acquisition times. These are snapshots,
not historical status trajectories. See `docs/member1/history_contract.md`.

## Member 1 remaining roadmap

| Phase | State and next deliverable |
| --- | --- |
| 0-2 | Existing acquisition/ETL foundations and recorded audit evidence |
| 3 | Existing EDA/geospatial/clustering reverified by the full passing suite |
| 4 | PASS: bounded historical expansion, ETL, resume, source coverage and full regression checks |
| 5 | Next: leakage-safe daily demand features, baselines, Spark MLlib comparison and chronological evaluation |
| 6 | Forecast/geospatial handoff for prioritization; Member 3 owns priority logic |
| 7 | Measured Spark/Pandas benchmarks at multiple scales |
| 8 | Member 1 final methodology, evidence and reproducibility audit |

No forecasting accuracy or benchmark performance is claimed in Phase 4.

## Pre-merge follow-up audit (2026-10-25)

See `reports/member1/premerge_audit.md` for the complete follow-up audit, nine
resolved findings and real end-to-end verification on the 100k sample, 799,439
historical records and a fresh two-page 9,026-record download. The full regression
run passed 85 tests; a final unit pass after the last plotting/configuration
fixes passed 55 tests. Lint passed. Environment and sandbox limitations are
recorded explicitly. No merge into main was performed.
