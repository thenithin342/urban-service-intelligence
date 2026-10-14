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
