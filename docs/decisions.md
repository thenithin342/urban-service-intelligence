# Decisions log

## 2026-10-14: Python + Spark versions
- Plan asks Python 3.11 + PySpark 3.5.x + Java 17.
- Machine has Python 3.13 (default, rich packages) and Python 3.10.
  No 3.11 installer present. Java is 17.0.15 (compatible).
- Finding: PySpark Python-worker tasks (createDataFrame/collect) crash
  on Python 3.13 on this Windows host with both PySpark 3.5.3 and 4.0.1
  (java.net.SocketException / worker exited unexpectedly; JVM-only
  `spark.range().count()` works on 3.13 + 4.0.1).
- Verified: Python 3.10 + PySpark 3.5.3 `createDataFrame(...).count()==3`.
- Decision: primary dev runtime stays Python 3.13 (requirements pins
  pyspark==4.0.1); Spark smoke test in pytest is JVM-only so `pytest -q`
  passes on 3.13; full Python-worker Spark work must run under
  Python 3.10 until the 3.13 worker issue is resolved. Revisit in Phase 2
  (ETL needs Python workers, so Phase 2 must either use 3.10 venv or
  fix 3.13 worker transport).

## 2026-10-14: Sample window
- Fixed window 2024-01-01..2024-01-31 ordered by unique_key, page 10k,
  target 100k. Stable re-downloads; manifest records URLs + SHA256.

## 2026-10-14: EDA on pandas in Phase 1
- Phase 1 profiling uses pandas (fast, deterministic). Spark ETL/aggregates
  arrive in Phase 2/3. Dashboard reads artifact CSVs, never raw API.

## 2026-10-14 (Phase 2): Standardize on Python 3.10 + PySpark 3.5.3
- Resolves the open 3.13 worker issue by standardizing: project `.venv`
  built from Python 3.10.0, requirements.txt repinned to exact versions
  (pyspark==3.5.3, pandas==2.3.3, pyarrow==25.0.1, pytest==9.1.1, ...).
- `src/utils/spark.py::ensure_worker_env` pins PYSPARK_PYTHON/DRIVER to the
  running interpreter and auto-discovers C:\hadoop (winutils), so driver
  and workers always match in fresh shells.
- Verified: createDataFrame + UDF + shuffle groupBy + RDD map all pass
  under .venv (tests/integration/test_spark_worker.py, 4 tests).

## 2026-10-14 (Phase 2): winutils required for Parquet on Windows
- Parquet writes failed with HADOOP_HOME unset (RawLocalFileSystem needs
  winutils for permissions), then with UnsatisfiedLinkError until
  C:\hadoop\bin was also on PATH (JVM must load hadoop.dll).
- Fix: C:\hadoop\bin\winutils.exe + hadoop.dll (Hadoop 3.3.x build),
  HADOOP_HOME persisted via setx, code fallback in ensure_worker_env.
- Two real bugs fixed during Phase 2: (1) duplicate-attribute
  AnalysisException from reusing latitude/longitude names in expressions
  (fixed with _lat/_lon temp columns); (2) the winutils/PATH issue above.

## 2026-10-14 (Phase 2): ETL rules
- Dedupe keeps earliest created_at per unique_key (nulls last); conflicts
  counted in metrics.duplicates_removed.
- Timestamps parsed as naive NYC wall time, formats
  yyyy-MM-dd'T'HH:mm:ss.SSS with fallback yyyy-MM-dd'T'HH:mm:ss.
- borough_std null (flag missing_borough) for empty/"Unspecified"; raws
  borough_raw/complaint_raw/incident_zip_raw retained.
- Invalid ZIP/coords -> null + flag, never repaired. resolution_hours only
  for valid closed>=created; unresolved/negative -> null + flag, never
  zero-filled, closure never fabricated.
- Output overwritten (mode overwrite, partitioned year/month): reruns are
  idempotent (verified byte-identical metrics on 100k rerun).
- Genuine zero durations exist (1335 rows where source closed==created to
  the second); these are real, not imputation.
- Descriptor null discrepancy: Phase 1 pandas reported 2.87% (its default
  NA list treats literals like "NA" as missing); Spark reads only truly
  empty fields as null -> 0.58% (576 rows). Spark figure is authoritative
  for the cleaned table; Phase 1 report kept as-is with this note.

## 2026-10-14 (audit): reconciliation + idempotency + fixes
- Flag math reconciles exactly (see reports/audit_phase2.md): 2715 rows
  = 1889 single-flag + 826 multi-flag; 3614 occurrences; overlaps led by
  coords&zip 577 and unresolved&zip 216.
- Row-level idempotency proven via sorted SHA256 row hashes: 100k/100k
  identical, 0 diffs, schema identical.
- Fixed: stale CLEAN_COLUMNS, dedupe tie order, keyless dedupe collapse,
  4xx retry waste, snapshot-date validation, default.yaml timeouts,
  README venv path + dashboard claim (headless HTTP 200 verified).
