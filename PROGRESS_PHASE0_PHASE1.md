# Progress summary - Phase 0 + Phase 1 (2026-10-14, measured only)

## Files created

- requirements.txt (pyspark==4.0.1 primary; see docs/decisions.md)
- .gitignore (ignores data/raw, data/processed, artifacts, models, creds)
- README.md, AGENTS.md, git repo initialized (no commit yet)
- configs/default.yaml, configs/sample.yaml (fixed Jan-2024 window, seed 42)
- src/__init__.py + ingestion/processing/analytics/mining/modeling/
  prioritization/evaluation/utils packages
- src/utils/config.py (YAML load + required-key check)
- src/utils/spark.py (local session helper, UI off, 4 shuffle partitions)
- src/ingestion/downloader.py (SODA paging, retry/backoff, dedupe, manifest)
- src/ingestion/metadata.py (schema snapshot saver)
- src/analytics/eda.py (pandas profiling + top/borough/volume aggregates)
- scripts/download_sample.py (working), run_etl.py / build_features.py /
  train_models.py / benchmark.py (stubs, exit 2 until their phase)
- dashboard/app.py (minimal Phase 1 page reading artifacts/eda_sample)
- notebooks/01_eda.ipynb (renders saved artifacts via src code)
- tests/unit/test_downloader.py, test_eda.py, test_config.py (13 tests)
- tests/integration/test_spark_smoke.py (JVM-only range count)
- tests/fixtures/sample_311.csv (8-row edge-case fixture)
- data/manifests/schema_erm2-nwe9.json, manifest_sample.json (+config snap)
- artifacts/eda_sample/{top_categories,by_borough,volume_daily}.csv + profile.json
- reports/data_quality.md, reports/methodology.md
- docs/architecture.md, docs/data_dictionary.md, docs/decisions.md

## Exact commands executed

- pip install "pyspark==3.5.3" then "pyspark==4.0.1" (3.13 default python)
- Python3.10: pip install pyspark==3.5.3 (worker verification only)
- python -m pytest -q  -> 14 passed (final full run, incl. Spark smoke)
- python scripts/download_sample.py --config configs/sample.yaml
  (2 pages x 50k; first 10k-page attempt too slow, retuned, see below)
- profiling + determinism re-run scripts (see test + verify output)

## Test results and errors (honest)

- PASS: 14/14 pytest (13 unit + 1 Spark integration, 53.8s final run).
- PASS: determinism - re-ran save_eda_outputs on real 100k CSV into temp
  dir; all 4 files byte-identical to committed artifacts.
- PASS: dashboard data-load check (3 artifact CSVs load, shapes sane);
  `streamlit run` interactive launch NOT executed (headless session).
- ERROR FOUND + FIXED: 10k-page download made no progress in 10 min
  (SODA ordered-window latency ~20s/request dominates). Fixed with
  page_size 50000 + timeout 180 + progress logging; 100k rows in ~2-4 min.
- ERROR FOUND + DOCUMENTED: Spark Python-worker tasks crash on Python 3.13
  (both PySpark 3.5.3 and 4.0.1; SocketException/worker-exited). JVM-only
  ops pass on 3.13+4.0.1; full createDataFrame verified on Python 3.10 +
  PySpark 3.5.3 (count==3). Smoke test is JVM-only so pytest passes;
  Phase 2 ETL must run under Python 3.10 or fix 3.13 transport.

## Observed dataset (real, from manifest/profile)

- Window Jan 2024 population: 287186 rows (count(*) check).
- Sample: 100000 rows written, 100000 unique keys, 0 duplicates, 0 failures.
- SHA256 7ed7541b56fa816459280483cf0d52f9d98b05bf34e884fc113afb87feafb652.
- Coverage actually 2024-01-01..2024-01-12 (ordered by unique_key, not time).
- Nulls: closed 1.10%, descriptor 2.87%, zip 1.09%, lat/lon 1.32%.
- Anomalies: 19 closed<created; 1317 missing/invalid coords; 80 "Unspecified".
- Top: Illegal Parking 15450, HEAT/HOT WATER 14706, Noise-Residential 7846.
- Peak day 01-10: 11006; partial cutoff day 01-12: 191.

## Incomplete / risks / Phase 2 next

- [ ] Phase 2 ETL (src/processing/clean.py + scripts/run_etl.py): typed
  schema, dedupe rule, borough/ZIP/coords validation, resolution_hours,
  partition Parquet, idempotent reruns + Spark tests (needs Py3.10 env).
- [ ] Decide full-month coverage (larger target vs stratified paging).
- [ ] Interactive `streamlit run dashboard/app.py` check on a GUI host.
- [ ] No git commit yet (uncommitted skeleton + manifests; raw/ignored OK).
- [ ] Phases 3-8 not started by design (plan: verify before proceeding).
