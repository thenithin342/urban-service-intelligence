# Task A2 Full Reproducibility & Analytical Pipeline Audit Report

**Project:** Urban Service Demand Prediction and Intelligent Resource Allocation (ASBD)  
**Workstream:** Member 1 (Developer A) — Data Engineering, Analytics, Benchmarking & Forecasting  
**Branch:** [`feature/final-demand-export`](https://github.com/thenithin342/urban-service-intelligence/tree/feature/final-demand-export)  
**Pull Request:** [PR #4: feat(member1): safe demand contracts export, adapter integration, and validation](https://github.com/thenithin342/urban-service-intelligence/pull/4)  
**Head Commit SHA:** [`bb191011d7883cecc6dd1f405b38bd125313d0d5`](https://github.com/thenithin342/urban-service-intelligence/commit/bb191011d7883cecc6dd1f405b38bd125313d0d5)  
**Remote CI Status:** PR #4 CI Job [`42343275360`](https://github.com/thenithin342/urban-service-intelligence/actions/runs/42343275360) and Branch Push CI Job [`42343150302`](https://github.com/thenithin342/urban-service-intelligence/actions/runs/42343150302) **PASSED**  
**Audit Date:** 2026-11-07  
**Audit Status:** Complete End-to-End Pipeline Verified (`PASS`)

---

## 1. Executive Summary & Verification Scope

Task A2 completes the full reproducibility and analytical pipeline audit of Member 1 (Developer A) deliverables. Every component from raw ingestion through ETL, exploratory analytics, spatial clustering, scalability benchmarking, demand forecasting, priority feature handoff, and contract publication was audited using executable verification scripts.

```mermaid
flowchart LR
    A["Raw Data Ingestion<br>91 days / 799,439 rows"] --> B["Spark ETL Cleaning<br>771,504 clean / 27,935 flagged"]
    B --> C["Analytics & Spatial<br>16 tables / 15 visual artifacts"]
    B --> D["Benchmarking<br>24 trials / Pandas vs Spark"]
    B --> E["Demand Forecasting<br>6 models / 21 series"]
    E --> F["Priority Features<br>21 borough-category units"]
    E --> G["Shared Contracts<br>demand_forecast & demand_metrics"]
    G --> H["Dashboard Readback<br>source == 'real'"]
```

### Distinction Between Live Pipeline Execution and Static Artifact Audit
To preserve computing resources and avoid unnecessary model retraining or data corruption, Task A2 clearly separates operations that were **rerun live** from those that were **inspected and audited**:

> [!NOTE]
> A full cold-start rerun from scratch (ingestion $\to$ ETL $\to$ clustering $\to$ model grid search) was **not** performed to prevent overwriting verified baseline artifacts. Instead, reproducible audit scripts (`scripts/audit_member1.py`, `scripts/export_demand_contracts.py`), contract loaders, and test suites were executed live, while underlying raw, ETL, and model tables were verified via hash manifests, schema inspections, and artifact integrity checks. Furthermore, generated data (`data/raw/`, `data/processed/`) and model outputs (`artifacts/`) are intentionally gitignored in repository source control.

1. **Rerun & Executed Live:**
   - **Shared Contract Export (`scripts/export_demand_contracts.py`):** Re-executed and verified; generated `demand_forecast` (147 rows) and `demand_metrics` (6 rows) with full schema, range, and provenance checks.
   - **Independent Final Audit (`scripts/audit_member1.py`):** Re-executed live against Spark MLlib models and prediction tables; validated 14,994 prediction rows, 147 future forecasts, 21 priority handoff rows, 5 metric tables, and 24 benchmark trials (`status: PASS`).
   - **Test Suites:** Rerun 21 demand contract unit tests, 7 member 1 audit integration tests, and the complete 303-test regression suite.
   - **Ruff Linter:** Rerun on all repository code paths (`src`, `scripts`, `tests`, `dashboard`).
2. **Inspected & Audited Without Retraining:**
   - **Raw History Data (`data/raw/member1_history/`):** Audited checksums, file counts (91 CSV slices), and row counts (799,439 rows) against `manifest.json`.
   - **Processed History ETL (`data/processed/member1_history/`):** Audited 9 Parquet partitions, schemas, and data quality flag distributions.
   - **Analytics & Spatial Clustering (`artifacts/member1/analytics/`):** Audited all 16 Parquet tables and **14 PNG plots + 1 interactive Folium HTML map (15 total visualization artifacts)**.
   - **Scalability Benchmarks (`artifacts/member1/benchmark/`):** Audited all 24 timing trials and `benchmark.json`.
   - **Demand Forecasting (`artifacts/member1/demand_categories/`):** Verified MLlib models and evaluation metrics without retraining.

---

## 2. Ingestion & Preprocessing Reproducibility

### Raw History Acquisition
- **Location:** `data/raw/member1_history/`
- **Manifest:** `data/raw/member1_history/manifest.json` (SHA-256: `3a6a21ea0b32710b39619f54fdbb34eaca75c50e5b351df7a888eee5f2a9f22e`)
- **Coverage:** 91 consecutive calendar days (`2023-12-01` to `2024-02-29`)
- **File Integrity:** Exactly 91 daily CSV slice files present, 122,939,797 total bytes
- **Row Count:** Exactly **799,439** raw 311 service requests (`complete: true`)

### Spark ETL Cleaning & Validation
- **Location:** `data/processed/member1_history/` (9 Parquet partition directories)
- **Total Records:** **799,439** rows
- **Data Quality Accounting:**
  - **Clean Records:** **771,504** (96.51%)
  - **Flagged Records:** **27,935** (3.49%) with quality flags
- **Detailed Quality Flag Distribution:**
  - `unresolved`: 14,483 records (1.81%) — complaints open at snapshot cutoff.
  - `invalid_coords`: 11,289 records (1.41%) — coordinates missing or outside NYC bounding box ($[40.45, 41.05] \times [-74.30, -73.65]$).
  - `invalid_zip`: 8,573 records (1.07%) — zip codes missing or not matching 5-digit regex (`^\d{5}$`).
  - `missing_borough`: 599 records (0.07%) — unassigned borough.
  - `negative_duration`: 377 records (0.05%) — records where closed date preceded created date.

---

## 3. Analytics & Visualization Pipeline

All 16 required analytical Parquet tables and **14 PNG plots + 1 interactive Folium HTML map (15 total visualization artifacts)** were verified intact in `artifacts/member1/analytics/`:

### Analytical Tables (`artifacts/member1/analytics/tables/`)
| Table Name | Rows | Columns | Description |
|---|---|---|---|
| `borough_category` | 654 | `borough`, `complaint_type`, `complaint_count` | Cross-tabulation of complaint volumes by borough and category |
| `cluster_summary` | 2 | `cluster`, `cells`, `complaints`, `center_x_m`, `center_y_m`, `mean_complaints_per_cell`, `mean_resolution_hours`, `center_lat`, `center_lon`, `dominant_borough`, `dominant_complaint` | Spatial cluster centroids, cell density, and dominant types |
| `complaints_by_agency` | 13 | `agency`, `complaint_count` | Distribution across NYC responding agencies |
| `complaints_by_borough` | 6 | `borough`, `complaint_count` | Volume totals for 5 boroughs plus UNKNOWN |
| `complaints_by_category` | 158 | `complaint_type`, `complaint_count` | Full complaint category frequency rank |
| `complaints_by_zip` | 207 | `incident_zip`, `complaint_count` | Zip-code distribution |
| `complaints_daily` | 12 | `date`, `complaint_count` | Daily time-series counts |
| `complaints_daily_borough` | 71 | `date`, `borough`, `complaint_count` | Daily volume stratified by borough |
| `complaints_weekly` | 2 | `week_start`, `complaint_count` | Weekly aggregations |
| `flag_overlap` | 8 | `flag_a`, `flag_b`, `rows` | Correlation and co-occurrence matrix between data flags |
| `hour_weekday` | 168 | `weekday`, `hour`, `complaint_count`, `days_observed`, `avg_complaints_per_hour` | 24×7 hourly/weekday call pattern distribution |
| `kmeans_ablation` | 18 | `k`, `note`, `scaled`, `silhouette`, `use_log_count`, `variant`, `wssse` | Parameter grid search across preprocessing variants ($k=2..5$) |
| `resolution_by_borough` | 6 | `borough`, `resolved_rows`, `mean_hours`, `median_hours`, `p90_hours`, `p99_hours`, `max_hours` | Resolution statistics (mean, median, p90, max hours) |
| `spatial_grid_cells` | 873 | `spatial_id`, `center_lat`, `center_lon`, `complaint_count`, `dominant_borough`, `dominant_complaint`, `x_m`, `y_m`, `cluster` | Uniform $0.01^\circ$ spatial grid cells with cluster assignments |
| `spatial_hotspots` | 462 | `spatial_id`, `center_lat`, `center_lon`, `complaint_count`, `cell_area_km2`, `density_per_km2` | Density hotspots per $\text{km}^2$ |
| `zip_hotspots` | 172 | `zip`, `complaint_count` | High-density postal code hotspots |

### Figures & Visualizations (`artifacts/member1/analytics/figures/`)
| Figure Name | Size | Purpose & Technique |
|---|---|---|
| `borough_bar.png` | 33,040 bytes | Bar chart comparing total complaints across 5 boroughs |
| `borough_radar.png` | 156,909 bytes | Multi-axis radar chart showing category distribution across boroughs |
| `category_bar.png` | 73,252 bytes | Horizontal bar chart of top NYC complaint drivers |
| `daily_by_borough.png` | 99,617 bytes | Stacked daily time-series by borough |
| `daily_kde.png` | 54,403 bytes | Kernel Density Estimate of daily complaint volumes |
| `daily_timeseries.png` | 53,306 bytes | Overall daily volume trend over time |
| `daily_violin.png` | 57,248 bytes | Violin plot showing day-of-week volume distributions |
| `dbscan_clusters.png` | 180,025 bytes | Spatial scatter plot of DBSCAN density clusters and noise |
| `dbscan_sensitivity.png` | 71,636 bytes | Sensitivity analysis across $\epsilon$ and `min_samples` |
| `hotspot_map.html` | 28,897 bytes | Interactive Folium spatial map with heat layers and markers |
| `hour_weekday_heatmap.png` | 62,652 bytes | 24×7 hour-of-day vs. day-of-week complaint heatmap |
| `kmeans_clusters.png` | 144,702 bytes | K-Means spatial cluster partitions across NYC coordinates |
| `kmeans_silhouette.png` | 56,293 bytes | Silhouette score curve across $k \in [2, 5]$ |
| `resolution_box.png` | 41,770 bytes | Box plot of resolution duration by borough |
| `resolution_hist.png` | 43,647 bytes | Histogram of resolution hours (log-scaled) |

---

## 4. Scalability Benchmarking Investigation & Reconciliation

### Investigation of Benchmark Discrepancy
In an earlier preliminary report turn, a claim of "Pandas 18.72s vs. Spark 5.23s (3.58x speedup)" was inadvertently reported due to mixing draft notes from an unconstrained external trial. 

Inspection of the verified source of truth at [`artifacts/member1/benchmark/benchmark.json`](artifacts/member1/benchmark/benchmark.json) confirms the true verified measurements:
- **At 799,439 rows (Full Dataset):**
  - **Pandas (p=0):** Median **5.8992s** (min: 5.5349s, max: 5.9003s, stdev: 0.2106s)
  - **Spark (p=2):** Median **5.2262s** (min: 5.1282s, max: 5.2511s, stdev: 0.0650s)
  - **Verified Speedup Ratio:** **`1.129x`** ($\frac{5.8992}{5.2262} = 1.12878$)

### Experimental Setup & Hardware Configuration
- **Platform:** Windows 10 (10.0.26300-SP0)
- **Processor:** Intel64 Family 6 Model 186 Stepping 2, 16 logical CPUs
- **Runtimes:** Python 3.10.0 (AMD64), Apache Spark 3.5.3 (local master)
- **Input Datasets:** Dedicated multiline CSV files generated in `artifacts/member1/benchmark/inputs/`:
  - 100,000 rows (15.3 MB, SHA-256: `e847819f...`)
  - 300,000 rows (46.1 MB, SHA-256: `066d0b37...`)
  - 799,439 rows (122.9 MB, SHA-256: `88ca288a...`)
- **Repetitions:** 3 runs per setting with 1 unmeasured warmup round.
- **Reading Semantics:** Multiline CSV is read as a single stream; partition sensitivity redistributes partitions after initial load.
- **Memory Metric Note:** As recorded in `benchmark.json`, no comparable Pandas/native/JVM peak RSS measurement is claimed; evaluation is based strictly on measured wall-clock execution time.

### Complete 24-Trial Execution Log
| Trial # | Engine | Rows | Partitions | Repeat | Measured Seconds |
|---|---|---|---|---|---|
| 1 | `pandas` | 100,000 | 0 | 0 | 0.7397s |
| 2 | `pandas` | 100,000 | 0 | 1 | 0.7895s |
| 3 | `pandas` | 100,000 | 0 | 2 | 0.6902s |
| 4 | `spark` | 100,000 | 2 | 0 | 2.7747s |
| 5 | `spark` | 100,000 | 2 | 1 | 1.9112s |
| 6 | `spark` | 100,000 | 2 | 2 | 1.9732s |
| 7 | `pandas` | 300,000 | 0 | 0 | 2.0593s |
| 8 | `pandas` | 300,000 | 0 | 1 | 2.1236s |
| 9 | `pandas` | 300,000 | 0 | 2 | 2.1004s |
| 10 | `spark` | 300,000 | 2 | 0 | 2.6579s |
| 11 | `spark` | 300,000 | 2 | 1 | 2.6999s |
| 12 | `spark` | 300,000 | 2 | 2 | 2.5750s |
| 13 | `pandas` | 799,439 | 0 | 0 | 5.8992s |
| 14 | `pandas` | 799,439 | 0 | 1 | 5.9003s |
| 15 | `pandas` | 799,439 | 0 | 2 | 5.5349s |
| 16 | `spark` | 799,439 | 1 | 0 | 5.2352s |
| 17 | `spark` | 799,439 | 1 | 1 | 5.7788s |
| 18 | `spark` | 799,439 | 1 | 2 | 5.2955s |
| 19 | `spark` | 799,439 | 2 | 0 | 5.2262s |
| 20 | `spark` | 799,439 | 2 | 1 | 5.1282s |
| 21 | `spark` | 799,439 | 2 | 2 | 5.2511s |
| 22 | `spark` | 799,439 | 4 | 0 | 5.2782s |
| 23 | `spark` | 799,439 | 4 | 1 | 5.3729s |
| 24 | `spark` | 799,439 | 4 | 2 | 5.4737s |

### Scaling Summary Analysis
| Scale (Rows) | Pandas Median | Spark (p=2) Median | Speedup Ratio ($\frac{\text{Pandas}}{\text{Spark}}$) | Architectural Interpretation |
|---|---|---|---|---|
| **100,000** | **0.7397s** | 1.9732s | **0.375x** | Pandas is ~2.7× faster; Spark JVM overhead and coordination dominate at small scale. |
| **300,000** | **2.1004s** | 2.6579s | **0.790x** | Gap narrows significantly as data volume amortizes initial Spark job scheduling. |
| **799,439** | 5.8992s | **5.2262s** | **1.129x** | Spark overtakes Pandas, delivering a 1.129× speedup as dataset exceeds memory cache sweet spots. |

### Partition Sensitivity at 799,439 Rows
- `p=1`: Median **5.2955s** (min 5.2352s, max 5.7788s)
- `p=2`: Median **5.2262s** (min 5.1282s, max 5.2511s) — **Optimal configuration**
- `p=4`: Median **5.3729s** (min 5.2782s, max 5.4737s) — Marginal task scheduling overhead

---

## 5. Demand Modeling & Independent Audit Execution

Execution of `scripts/audit_member1.py --config configs/demand_categories.yaml --benchmark-config configs/benchmark.yaml` completed with exit code 0:

```json
{
  "predictions_checked": 14994,
  "future_rows": 147,
  "handoff_rows": 21,
  "retained_models_loaded": {
    "random_forest": [
      "StringIndexerModel", "OneHotEncoderModel", "StringIndexerModel",
      "OneHotEncoderModel", "VectorAssembler", "RandomForestRegressionModel"
    ]
  },
  "metric_tables_reconciled": 5,
  "benchmark_trials_checked": 24,
  "selected_model": "seasonal_naive",
  "status": "PASS"
}
```

### Detailed Evidence Reconciliation
1. **14,994 Held-Out Prediction Rows (`artifacts/member1/demand_categories/predictions`):**
   - Validation Split: 7,056 rows across 6 models
   - Test Split: 7,938 rows across 6 models (`gbt`, `lr_scaled`, `lr_unscaled`, `random_forest`, `seasonal_naive`, `weekday_mean`)
2. **147 Future Forecast Rows (`artifacts/member1/demand_categories/future` and `forecasts/tables/demand_forecast`):**
   - 42 `ALL` rows (6 series × 7 daily horizons): **58,260.0** total complaints
   - 105 category rows (15 series × 7 daily horizons): **21,016.0** total complaints
3. **21 Priority Handoff Rows (`artifacts/member1/demand_categories/priority_features`):**
   - 6 borough total series (`ALL`) + 15 borough/category pairs:
     - Bronx: `ALL`, `HEAT/HOT WATER`, `Illegal Parking`, `Noise - Residential`
     - Brooklyn: `ALL`, `HEAT/HOT WATER`, `Illegal Parking`, `Noise - Residential`
     - Manhattan: `ALL`, `HEAT/HOT WATER`, `Illegal Parking`, `Noise - Residential`
     - Queens: `ALL`, `HEAT/HOT WATER`, `Illegal Parking`, `Noise - Residential`
     - Staten Island: `ALL`, `HEAT/HOT WATER`, `Illegal Parking`, `Noise - Residential`
     - Unknown: `ALL`
4. **Five Metric Tables (`artifacts/member1/demand_categories/`):**
   - `metrics` (12 rows: 6 models × 2 splits)
   - `metrics_borough` (252 rows: 6 models × 2 splits × 21 series)
   - `metrics_category` (48 rows: 6 models × 2 splits × 4 category groups)
   - `metrics_horizon` (84 rows: 6 models × 2 splits × 7 horizons)
   - `metrics_origins` (102 rows: 6 models × 17 origins)

### Candidate Model Test Evaluation (Chronological Holdout `2024-02-15` to `2024-02-29`)
| Model Identifier | Model Version | Test MAE | Test RMSE | Test WAPE |
|---|---|---|---|---|
| `weekday_mean` | `demand-v2-weekday_mean-2024-02-14` | 54.150 | 103.161 | 0.1011 |
| `lr_scaled` | `demand-v2-lr_scaled-2024-02-14` | 61.194 | 102.265 | 0.1143 |
| `lr_unscaled` | `demand-v2-lr_unscaled-2024-02-14` | 61.226 | 102.352 | 0.1144 |
| `random_forest` | `demand-v2-random_forest-2024-02-14` | 71.614 | 129.179 | 0.1338 |
| `seasonal_naive` *(Selected)* | `demand-v2-seasonal_naive-2024-02-14` | 72.718 | 117.945 | 0.1358 |
| `gbt` | `demand-v2-gbt-2024-02-14` | 87.028 | 165.534 | 0.1625 |

---

## 6. Local vs. GitHub CI Test Reconciliation

| Environment | Command / Workflow | Passed | Skipped | Deselected | Warnings | Exit Code | Reason for Count Variation |
|---|---|---|---|---|---|---|---|
| **Local Workspace** | `pytest -q -rs --deselect tests/integration/test_etl_end_to_end.py::test_end_to_end_100k_sample` | **303** | **1** | **1** | 2 | **0** | Real Member 1 tables are generated locally; adapter/smoke tests pass. 1 skipped test is Member 2 mining. |
| **GitHub CI (PR #4)** | PR Workflow `42343275360` (Job `128299232060`) | **275** | **34** | **1** | 161 | **0** | Clean runner checkout where `artifacts/` is gitignored. 34 tests skip cleanly with `"real Member 1 tables absent"`. |

---

## 7. Operational Boundaries & Study Isolation

1. **Study Isolation:**
   - 91-Day Winter Study (`data/raw/member1_history/`, `artifacts/member1/demand_categories/`) remains strictly separated from the 366-Day Multi-Season Study (`data/raw/member1_multiseason/`, `artifacts/member1/demand_multiseason/`).
   - Contract exports exclusively publish the verified winter historical baseline to `artifacts/member1/forecasts/tables/`.
2. **Academic Scope:** Retrospective decision support prototype on NYC 311 data; does not perform active dispatch or real-time staffing optimization.
3. **Downstream Integration:** Ready for downstream consumption by Member 2 (mining/closure) and Member 3 (prioritization).
