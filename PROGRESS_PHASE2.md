# Progress summary - Phase 2 Distributed ETL (2026-10-14, measured only)

Environment: project .venv (Python 3.10.0 + PySpark 3.5.3, Java 17.0.15,
local[2]). Full suite: 27 passed in 237s under .venv.
Post-suite: `ruff check src scripts tests` clean (13 findings fixed);
unit tests re-run green (17 passed) after lint fixes.
Audit (see reports/audit_phase2.md): flag reconciliation exact
(2715 rows = 1889x1 + 755x2 + 69x3 + 2x4 flags); row-level idempotency
proven (100000/100000 identical row hashes, 0 diffs); 9 fixes applied;
final full suite 33 passed in 238s.

## Files added

- src/processing/clean.py (RAW_SCHEMA, parse/dedupe/validate, write_clean)
- scripts/run_etl.py (rewritten from stub; --config/--snapshot-date/--master)
- tests/integration/test_spark_worker.py (4 real Python-worker tests)
- tests/integration/test_etl.py (4 Spark tests on synthetic edge cases)
- tests/integration/test_etl_end_to_end.py (100k end-to-end)
- tests/unit/test_etl_contract.py (schema/contract, no Spark)
- PROGRESS_PHASE2.md (this file)

## Files modified

- requirements.txt (repinned exact: pyspark 3.5.3, pandas 2.3.3, ...)
- src/utils/spark.py (ensure_worker_env: interpreter pin + winutils auto)
- configs/sample.yaml + default.yaml (input_csv, etl_output, etl_quality_report)
- README.md (3.10 .venv setup, winutils, verified commands)
- docs/architecture.md, docs/data_dictionary.md, docs/decisions.md

## ETL processing statistics (data/processed/etl_quality.json)

- input_rows 100000, rows_excluded_missing_key 0, duplicates_removed 0,
  output_rows 100000 (reconciles exactly).
- unresolved 1104 (1.10%), closed_before_created 19, invalid_coords 1317,
  invalid_created 0, invalid_closed 0.
- flagged_rows 2715, clean_rows 97285.
- flag_breakdown: negative_duration 19, invalid_coords 1317,
  invalid_zip 1094, unresolved 1104, missing_borough 80.
- Null %: closed_at 1.10, resolution_hours 1.12, lat/lon 1.32,
  incident_zip 1.09, borough 0.08, descriptor 0.58, created 0.0.
- Rerun idempotency: second run metrics identical (IDEMPOTENT: True).

## Spark worker-test results (all real Python workers, .venv)

- test_create_dataframe_python_worker, test_udf_python_worker (sum 306),
  test_shuffle_groupby_python_worker, test_rdd_map_python_worker: PASS.
- Old JVM-only smoke test retained and passing.

## Parquet schema and partitions

- Path data/processed/nyc311_clean (gitignored), 2 snappy part files
  (~2.2MB each), partition year=2024/month=1 (single, expected: sample
  covers Jan 1-12).
- 24 columns: unique_key, created_at, closed_at, complaint_raw,
  complaint_type, descriptor, agency, status_snapshot, borough_raw,
  borough, incident_zip_raw, incident_zip, latitude, longitude,
  resolution_hours, created_day, created_week, weekday, hour,
  data_snapshot_date, data_quality_flags + year/month partitions.
- Borough counts reconcile with Phase 1 (BROOKLYN 32470 ... NULL 80).

## Data-quality findings

- 1335 genuine zero durations (source closed==created to the second);
  real, not imputed. Unresolved rows all have null resolution (0 leaks).
- Descriptor nulls 0.58% under Spark vs 2.87% in Phase 1 pandas report
  (pandas default-NA list inflates; Spark figure authoritative).
- 19 negative durations preserved with null resolution + flag.
- No unparseable timestamps in this sample (both invalid counters 0).

## Limitations and risks (Phase 2 acceptance: all criteria met)

1. Sample covers Jan 1-12 only (unique_key ordering); full-month needs
   larger/stratified intake (carried from Phase 1).
2. Single-machine local Spark, not a cluster; no throughput benchmark yet
   (Phase 7). No timing claims made.
3. winutils lives outside the repo (C:\hadoop); fresh machines need the
   one-time binary install documented in README.
4. System Python 3.13 remains worker-incompatible; all Spark work must
   use .venv (enforced by docs + ensure_worker_env).
5. status_snapshot is point-in-time; historical backlog reconstruction
   limits deferred to Phase 6 design.
