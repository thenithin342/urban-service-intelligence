# Member 1 Phase 4 historical data contract

The initial bounded window is December 2023 through February 2024 (exclusive
end 2024-03-01). This replaces neither the sample nor its analytics outputs.
`configs/history.yaml` records dates, API endpoint, row and byte budgets.
To expand toward 2022-2025, choose another output directory and explicit bounds.
Three winter months cannot establish annual seasonality.

## Acquisition

`scripts/download_history.py` uses `src/ingestion/history.py`. All dates are
half-open NYC wall dates; event timestamps retain the source convention.
The live metadata must contain the 11 existing raw fields before any download.
The source daily count query covers the entire requested window before and
after acquisition. Zero-count dates are represented explicitly.

Each `data/raw/member1_history/YYYY-MM-DD.csv` has the existing raw schema.
Its neighboring JSON contains row count, SHA256, bytes, UTC acquisition time
and request parameters. Records are ordered by `(created_date, unique_key)`;
keyset pagination checks strictly increasing cursors and rejects duplicate keys
within a day. The Spark gate additionally rejects duplicates across days.
Only complete, checksum-verified days resume. Interrupted days restart; their
`.partial` file is never an ETL input. A different request cannot reuse the same
output directory. The byte budget measures CSV content, excluding metadata and
temporary overhead; allow additional disk space for Parquet and checkpoints.

`manifest.json` has `complete=false` until all requested days reconcile with
source counts. Failures are saved and the CLI exits nonzero. It includes monthly
counts and explicit zero-complaint days. Checkpoints and the manifest are ignored
along with raw data. Counts detect additions/removals, but equal-count changes
and source field updates are not detectable: this is not a transactional snapshot.
Resumed days can have different acquisition dates, which are retained.

## Cleaning and handoff

`scripts/run_history_etl.py` reuses Phase 2 `clean`, verifies checksums before
reading, checks input/output/distinct-key and per-day counts, writes partitioned
Parquet to `data/processed/member1_history`, and verifies the readback count.
The schema stays identical to `docs/data_dictionary.md`. The output is overwritten
on a rerun; sample Parquet remains in `data/processed/nyc311_clean`.

Publication first writes and checks a sibling staging directory. A write or
readback failure leaves the previous history output intact. The captured source
manifest hash must still match before publication; acquisition and ETL must run
sequentially. Invalid UTC acquisition timestamps and inconsistent source/date or
monthly metadata fail before Spark transformations. CSV parsing supports quoted
newlines and the doubled quotes emitted by the download writer.
Only one ETL writer may publish to a given output directory at a time.

`data_snapshot_date` is the UTC acquisition date for each daily checkpoint,
not the complaint event date or the ETL run date. It describes current observed
status/closure values; those values are not valid historical forecast features.
The quality report is `artifacts/member1/phase4/etl_quality.json`, with provenance
hash, snapshot dates, daily/monthly counts, missing-day check and quality flags.
No new dashboard tables are introduced. Members 2/3 can read these cleaned
records or rerun their own aggregates under the existing schema.

To run analytics on this expanded input, explicitly select its provenance:

```powershell
.\.venv\Scripts\python.exe scripts/run_member1_analytics.py --config configs/sample.yaml --input data/processed/member1_history --ingestion-manifest data/raw/member1_history/manifest.json --outdir artifacts/member1/analytics_history
```

An overridden input without an explicit manifest does not inherit the sample's
provenance. Sparse geographic inputs retain nullable `cluster: int` values and
typed ablation rows; `cluster_summary` is omitted when clustering is unavailable.

## Verification

Unit tests cover leap day/exclusive bounds, complete-day resume, checksums,
zero days, keyset pages, duplicates, interruptions, source count drift and budgets.
The Spark integration test uses a tiny synthetic fixture and checks Parquet
readback, acquisition dates, unresolved durations and failure on tampering.
Real measured coverage and commands are saved in `PROGRESS_MEMBER1_PHASE4.md`.
