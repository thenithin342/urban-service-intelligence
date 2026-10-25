# Member 1 Sample Fixtures

These sanitized, small JSON fixture files represent sample outputs from Member 1's Phase 3 analytical pipelines.
They are provided for Members 2 and 3 so they can develop, test, and render downstream models and dashboard components without requiring the full 100,000-record Parquet dataset or a running PySpark instance.

## Available Fixtures:
- `complaints_by_borough.json`: Total reported complaints across all boroughs (including unassigned `null`).
- `complaints_by_category_sample.json`: Top 10 complaint classifications by frequency.
- `complaints_daily.json`: Daily complaint volumes across the 12-day January 2024 observation period.
- `cluster_summary.json`: Cluster summary metrics for the selected Spark MLlib KMeans model ($k=2$, `log_count_scaled`).
- `spatial_hotspots_sample.json`: Top 10 spatial grid hotspot cells ($\Delta = 0.01^\circ$) with calculated density per $\text{km}^2$.

Full dataset schemas, data types, and contract guarantees are documented in `docs/member1/artifact_contract.md`.

`demand_handoff.json` is a measured six-row Phase 6 consumer example from the
2024-02-29 historical origin, acquired in the 2026-10-25 snapshot. It contains
borough totals and descriptive spatial counts, not priority ranks or current
forecasts. See `docs/member1/demand_contract.md` for its schema and limits.
