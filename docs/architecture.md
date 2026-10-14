# Architecture (Phase 0/1 slice)

SODA API (erm2-nwe9)
  -> scripts/download_sample.py + src/ingestion/downloader.py
  -> data/raw/*.csv + data/manifests/manifest_*.json
  -> src/analytics/eda.py (pandas profiling + aggregates)
  -> artifacts/eda_sample/*.csv + profile.json
  -> dashboard/app.py (Streamlit, reads artifacts only)
  -> reports/data_quality.md (measured profile)

Later phases add: src/processing (Spark ETL -> Parquet),
src/mining (FP-Growth), src/modeling (forecast + closure),
src/prioritization (scores), scripts/benchmark.py.

Local Spark only. Not a multi-node cluster. No Kafka/K8s/cloud in MVP.
