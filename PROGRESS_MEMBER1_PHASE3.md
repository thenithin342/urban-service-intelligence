# Progress Report: Member 1 - Phase 3 (Distributed EDA, Geospatial Analytics & Clustering)

**Role:** Member 1 of 3 (Big Data / ML Engineer)  
**Date:** 2026-10-20 (Audited & Reconciled)  
**Branch:** `feature/data-analytics`  
**Commit Baseline:** `c472133c2d010a71a8dd9b6eb2dc2712681a6faa` (`feat: Phase 2 Spark ETL with validation flags and quality report`)  
**Phase Status:** **PASS** (100% verified with working code, executed CLI pipeline, generated figures & tables, audit reconciliation report, and 67 passing tests)

---

## 1. Files Created and Modified

### Source Code (`src/analytics/`)
- [`src/analytics/member1_config.py`](file:///c:/Users/theni/OneDrive/Documents/projects/asbd_project/src/analytics/member1_config.py): Configuration resolver and validator for Member 1 settings (grid sizes, projection anchors, bounds, ablation parameters).
- [`src/analytics/spark_eda.py`](file:///c:/Users/theni/OneDrive/Documents/projects/asbd_project/src/analytics/spark_eda.py): Distributed PySpark aggregations (borough, category, agency, ZIP, daily/weekly volume, diurnal hour-weekday heatmap, resolution quantiles, flag co-occurrence, bounded driver collection).
- [`src/analytics/geospatial.py`](file:///c:/Users/theni/OneDrive/Documents/projects/asbd_project/src/analytics/geospatial.py): Deterministic degree grid binning ($\Delta = 0.01^\circ$), coordinate boundary enforcement, area normalization ($\text{km}^2$), local tangent plane metric projection, single-node haversine DBSCAN.
- [`src/analytics/clustering.py`](file:///c:/Users/theni/OneDrive/Documents/projects/asbd_project/src/analytics/clustering.py): Spark MLlib KMeans clustering pipeline (`VectorAssembler` $\rightarrow$ `StandardScaler` $\rightarrow$ `KMeans`), silhouette & WSSSE evaluation, cluster profiling, and preprocessing ablation.
- [`src/analytics/figures.py`](file:///c:/Users/theni/OneDrive/Documents/projects/asbd_project/src/analytics/figures.py): Headless, report-ready visualizations (matplotlib PNGs and Plotly HTML map) with data-grounded takeaway captions, axes units, and provenance stamps.

### CLI Pipeline (`scripts/`)
- [`scripts/run_member1_analytics.py`](file:///c:/Users/theni/OneDrive/Documents/projects/asbd_project/scripts/run_member1_analytics.py): End-to-end command-line runner reading cleaned Parquet, running Spark EDA, geospatial binning, KMeans ablation, DBSCAN sensitivity, and generating figures, tables, manifests, and metrics. Fixed resolution sampling to evaluate all 98,877 resolved rows.

### Test Suite (`tests/`)
- [`tests/unit/test_member1_analytics.py`](file:///c:/Users/theni/OneDrive/Documents/projects/asbd_project/tests/unit/test_member1_analytics.py): Unit tests for configuration overrides, metric coordinate roundtrips, figure generation edge cases, dynamic timeseries coverage, cluster scatter with cells and centroids, and empty dataframe guards (18 passed).
- [`tests/integration/test_member1_spark.py`](file:///c:/Users/theni/OneDrive/Documents/projects/asbd_project/tests/integration/test_member1_spark.py): PySpark integration tests on synthetic fixture verifying Spark vs. Pandas aggregation equivalence, null handling, deterministic sampling, KMeans pipelines, DBSCAN clusters, deterministic Window ranking under shuffled partitions, dynamic calendar month coverage, and null/UNKNOWN category pre-normalization (17 passed).

### Documentation & Reports
- [`docs/member1/artifact_contract.md`](file:///c:/Users/theni/OneDrive/Documents/projects/asbd_project/docs/member1/artifact_contract.md): Formal data and interface contract for Member 2 (rule mining/closure models) and Member 3 (dashboard/prioritization), including `spatial_grid_cells.cluster` assignment.
- [`reports/member1/phase3_findings.md`](file:///c:/Users/theni/OneDrive/Documents/projects/asbd_project/reports/member1/phase3_findings.md): Full scientific research report addressing professor feedback (missingness, distribution inferences, preprocessing ablation, comparative clustering analysis, quantile reconciliation, and weighted resolution durations).
- [`reports/member1/phase3_correctness_audit.md`](file:///c:/Users/theni/OneDrive/Documents/projects/asbd_project/reports/member1/phase3_correctness_audit.md): Detailed correctness audit documenting root causes, data provenance, exact mathematical reconciliation, and the 5 targeted review resolutions.
- [`PROGRESS_MEMBER1_PHASE3.md`](file:///c:/Users/theni/OneDrive/Documents/projects/asbd_project/PROGRESS_MEMBER1_PHASE3.md): This tracking report.

### Non-Sensitive Test Fixtures (`tests/fixtures/member1/`)
- [`tests/fixtures/member1/complaints_by_borough.json`](file:///c:/Users/theni/OneDrive/Documents/projects/asbd_project/tests/fixtures/member1/complaints_by_borough.json)
- [`tests/fixtures/member1/complaints_by_category_sample.json`](file:///c:/Users/theni/OneDrive/Documents/projects/asbd_project/tests/fixtures/member1/complaints_by_category_sample.json)
- [`tests/fixtures/member1/complaints_daily.json`](file:///c:/Users/theni/OneDrive/Documents/projects/asbd_project/tests/fixtures/member1/complaints_daily.json)
- [`tests/fixtures/member1/spatial_hotspots_sample.json`](file:///c:/Users/theni/OneDrive/Documents/projects/asbd_project/tests/fixtures/member1/spatial_hotspots_sample.json)
- [`tests/fixtures/member1/cluster_summary.json`](file:///c:/Users/theni/OneDrive/Documents/projects/asbd_project/tests/fixtures/member1/cluster_summary.json)
- [`tests/fixtures/member1/README.md`](file:///c:/Users/theni/OneDrive/Documents/projects/asbd_project/tests/fixtures/member1/README.md)

### Shared Configuration & Dependencies
- [`configs/sample.yaml`](file:///c:/Users/theni/OneDrive/Documents/projects/asbd_project/configs/sample.yaml): Added `member1:` configuration block with grid parameters, projection anchors, $k$ value sweeps, and driver safety guards.
- [`configs/default.yaml`](file:///c:/Users/theni/OneDrive/Documents/projects/asbd_project/configs/default.yaml): Added aligned production `member1:` defaults.
- [`requirements.txt`](file:///c:/Users/theni/OneDrive/Documents/projects/asbd_project/requirements.txt): Added `matplotlib==3.10.9` for static headless PNG generation.

---

## 2. Data Actually Used

- **Source Dataset:** NYC Open Data 311 Service Requests from 2020 to Present (`erm2-nwe9`), SODA API.
- **Raw Input File:** `data/raw/nyc311_sample.csv` (SHA256: `7ed7541b56fa816459280483cf0d52f9d98b05bf34e884fc113afb87feafb652`).
- **Cleaned Parquet Path:** `data/processed/nyc311_clean/year=2024/month=1`.
- **Total Records:** 100,000 rows.
- **Unique Key Count:** 100,000 distinct `unique_key` (0 duplicates).
- **Temporal Coverage:** `2024-01-01 00:00:00` to `2024-01-12 01:19:58` (12 calendar days observed; coverage ratio 1.0).
- **Snapshot Date:** `2026-10-14` (held constant from Phase 2).
- **Quality Flags:** 2,715 flagged rows (2.715%), 97,285 clean rows.
  - `invalid_coords`: 1,317 rows
  - `invalid_zip`: 1,094 rows
  - `unresolved`: 1,104 rows
  - `missing_borough`: 80 rows
  - `negative_duration`: 19 rows
  - *Pairwise flag overlaps:* 826 rows carry $\ge 2$ flags (`invalid_coords & invalid_zip` = 577, `unresolved & invalid_zip` = 216).

---

## 3. Real Executions & CLI Commands

1. **Member 1 Analytics Pipeline Execution:**
   ```powershell
   & .venv\Scripts\python.exe scripts/run_member1_analytics.py --config configs/sample.yaml
   ```
   *Execution time:* ~42 seconds. Generates 16 Parquet tables, 14 PNG figures, 1 interactive HTML map, `phase3_manifest.json`, and `phase3_metrics.json`.

2. **Linter & Code Style Verification:**
   ```powershell
   & .venv\Scripts\python.exe -m ruff check src scripts tests
   ```
   *Result:* `All checks passed!` (0 errors, 0 warnings).

3. **Pytest Test Collection Verification:**
   ```powershell
   & .venv\Scripts\python.exe -m pytest --collect-only -q
   ```
   *Result:* `67 tests collected in 4.70s`.

4. **Full Pytest Execution (Unit + Integration + Worker Transport):**
   ```powershell
   & .venv\Scripts\python.exe -m pytest -q
   ```
   *Result:* `67 passed, 1 warning in 540.23s (0:09:00)`.

---

## 4. EDA & Geospatial Findings (All Verified & Measured)

- **Borough Distribution (`complaints_by_borough`):**
  - Brooklyn: 32,470 (32.47%)
  - Queens: 22,836 (22.84%)
  - Bronx: 20,995 (21.00%)
  - Manhattan: 20,092 (20.09%)
  - Staten Island: 3,527 (3.53%)
  - UNKNOWN: 80 (0.08%)
  - *Total:* Exactly 100,000 records.
- **Top 10 Categories (`complaints_by_category`):**
  1. Illegal Parking: 15,450 (15.45%)
  2. HEAT/HOT WATER: 14,706 (14.71%)
  3. Noise - Residential: 7,846 (7.85%)
  4. Blocked Driveway: 5,504 (5.50%)
  5. UNSANITARY CONDITION: 3,905 (3.91%)
  6. PAINT/PLASTER: 2,535 (2.54%)
  7. WATER LEAK: 2,420 (2.42%)
  8. Abandoned Vehicle: 2,393 (2.39%)
  9. PLUMBING: 2,328 (2.33%)
  10. DOOR/WINDOW: 1,771 (1.77%)
- **Diurnal Periodicity (`hour_weekday`):**
  - Trough: 03:00–05:00 AM (< 150 complaints/h).
  - Peak: Wednesday 11:00 AM with 721.5 normalized avg complaints/h (1,443 total across the two observed Wednesdays).
- **Resolution Quantiles ($N=98,877$ resolved, nonnegative durations):**
  - Mean: 340.1838 h (~14.17 days).
  - Median: 15.6103 h (~15.61 hours) (Spark approx at accuracy=50,000: 15.61 h).
  - $P_{90}$: 699.2756 h (~29.14 days) (Spark approx at accuracy=50,000: 699.28 h, rounded: 699.3 h).
  - $P_{99}$: 6,973.4017 h (~290.56 days) (Spark approx at accuracy=50,000: 6,973.40 h, rounded: 6,973.4 h).
  - Max: 23,639.3667 h (~984.97 days).
  - Queens exhibits fastest median resolution (6.87 h); UNKNOWN tickets take longest (median 191.74 h).
  - Zero-duration resolutions (`created_at == closed_at`): 1,335 rows.
  - Excluded from duration: 1,104 unresolved + 19 negative = 1,123 nulls.
- **Geospatial Hotspots:**
  - 873 active cells in NYC bounding box ($40.49..40.92^\circ\text{N}, -74.26..-73.70^\circ\text{W}$).
  - 462 hotspot cells have $\ge 50$ complaints.
  - Top hotspot cell `25:31` (Long Island City / Sunnyside, Queens, $40.745^\circ\text{N}, -73.945^\circ\text{W}$; dominant borough `QUEENS`) recorded 2,842 complaints ($3,046 \text{ complaints/km}^2$).

---

## 5. Comparative Clustering & Preprocessing Ablation Results

### 5.1. Preprocessing Ablation on PySpark MLlib KMeans
Grid-cell features: local projected metric coordinates $(x_m, y_m)$ and volume:
- **`log_count_scaled` (Primary Recommended):** Applies $\ln(1+\text{count})$ and standardizes with mean and unit variance.
  - $k=2$: Silhouette **0.598994**, WSSSE = 1,561.557 (Optimal separation into core vs peripheral volume).
  - $k=3$: Silhouette 0.537558, WSSSE = 1,063.367.
  - $k=4$: Silhouette 0.512676, WSSSE = 818.464.
  - $k=5$: Silhouette 0.507907, WSSSE = 686.054.
  - $k=6$: Silhouette 0.546357, WSSSE = 573.241.
  - $k=8$: Silhouette 0.490591, WSSSE = 464.548.
- **`log_count_unscaled`:** Unscaled metric distances ($10^4$ m) completely swamp volume ($<8$). Clusters are geographic slices with zero volume sensitivity. Silhouette 0.6665 is a metric artifact of spatial separation alone ($\text{WSSSE} = 9.20 \times 10^{10}$).
- **`count_scaled`:** Linear volume without log transform suffers from extreme leverage by top hotspot cells, reducing $k=2$ silhouette to 0.5334.

### 5.2. PySpark MLlib KMeans ($k=2$, `log_count_scaled`)
From `artifacts/member1/analytics/tables/cluster_summary`:
- **Cluster 0 (Metropolitan Core & Dense Corridors):** 664 cells, 93,956 complaints (95.21% of valid geo complaints; mean 141.5/cell). Centroid: $40.73440^\circ\text{N}, -73.87360^\circ\text{W}$ ($x_m = 6,447.87\text{ m}, y_m = 3,803.48\text{ m}$). Dominant borough: `QUEENS`. Dominant complaint: `Illegal Parking`. Mean resolution duration: 335.02 h (complaint-weighted; unweighted cell mean 359.00 h).
- **Cluster 1 (Outer Periphery & Low Intensity):** 209 cells, 4,727 complaints (4.79% of valid geo complaints; mean 22.62/cell). Centroid: $40.58754^\circ\text{N}, -74.09725^\circ\text{W}$ ($x_m = -12,427.14\text{ m}, y_m = -12,435.61\text{ m}$). Dominant borough: `STATEN ISLAND`. Dominant complaint: `Illegal Parking`. Mean resolution duration: 463.99 h (complaint-weighted; unweighted cell mean 606.11 h).
- *Reconciliation Check:* $664 + 209 = 873$ cells (100%). $93,956 + 4,727 = 98,683$ complaints (exactly equal to total valid coordinate complaints).

### 5.3. Bounded Scikit-Learn DBSCAN (Single-Node, Haversine Metric)
Evaluated on bounded 20,000 valid coordinate points ($\text{min\_pts}=25$):
- $\epsilon = 100\text{ m}$: 30 clusters, 18,552 noise points (**92.76% noise**). Overly restrictive.
- $\epsilon = 250\text{ m}$ (Primary): **103 micro-clusters**, 11,979 noise points (**59.90% noise**). Isolates local street-level clusters.
- $\epsilon = 500\text{ m}$: 20 clusters, 2,575 noise points (**12.88% noise**).
- $\epsilon = 1,000\text{ m}$: 8 clusters, 441 noise points (**2.21% noise**). Merges distinct neighbourhoods into borough-sized blobs.

---

## 6. Generated Figures & Takeaways

All figures saved under `artifacts/member1/analytics/figures/`:
1. `borough_bar.png`: Brooklyn has the most reported complaints (32,470 of 100,000); counts are reporting frequency, not infrastructure quality.
2. `category_bar.png`: 'Illegal Parking' is the most reported category (15,450 records) in this window.
3. `daily_timeseries.png`: Full-day volume (Jan 1-11, 11 days) averages 9,074 records/day (range 6,703-11,006); all 12 days including partial Jan 12 (191 records) average 8,333.
4. `daily_by_borough.png`: Brooklyn contributes the largest share of daily volume (32.5% of records in window).
5. `hour_weekday_heatmap.png`: Peak cell is Wednesday 11:00 (721.5 avg records/h, normalized for observed weekday count); reporting has clear diurnal structure.
6. `daily_kde.png`: Daily volume is centred near 9,524 records/day; KDE is a smoothing of the observed days only.
7. `daily_violin.png`: Violins show the spread of daily volume within each borough; wide spreads mean unstable daily demand, not higher counts.
8. `resolution_hist.png`: Median resolution is 15.6 h (p90 699.3 h, p99 6973.4 h); unresolved rows are excluded, not zero-filled.
9. `resolution_box.png`: Median resolution is highest in UNKNOWN (191.7 h) among boroughs; box plots use resolved rows only and hide outliers.
10. `borough_radar.png`: Radar compares only normalised category shares per borough; a longer spoke means a larger share of that borough's own volume, not more records.
11. `kmeans_silhouette.png`: Best silhouette 0.599 at k=2; silhouette alone does not prove cluster meaning.
12. `kmeans_clusters.png`: 2 cluster centres shown over valid coordinates; clusters are descriptive reporting-intensity groups, not risk labels.
13. `dbscan_clusters.png`: DBSCAN found 103 dense clusters with 59.9% noise on a bounded sample; it is single-node, not distributed, and eps is in metres.
14. `dbscan_sensitivity.png`: At eps=100 m noise is 92.8% with 30 clusters; larger eps merges points and reduces noise, showing that the density result is parameter-dependent.
15. `hotspot_map.html`: 462 grid cells with >= threshold records; top cell 25:31 located in Long Island City / Sunnyside, Queens (40.745, -73.945).

---

## 7. Quality Assurance & Test Verification

- **Linting:** `ruff check src scripts tests` passed with 0 findings.
- **Unit Tests:** `tests/unit/test_member1_analytics.py` (18 passed); 39 unit tests passed repo-wide.
- **Integration Tests:** `tests/integration/test_member1_spark.py` (17 passed); 28 integration & worker tests passed repo-wide.
- **Total Test Suite:** 67 passed out of 67 tests across the entire repository.
- **Python Worker Verification:** Spark Python-worker tasks (`createDataFrame`, UDFs, shuffle groupBys, RDD maps) successfully execute under Python 3.10.0 + PySpark 3.5.3 with local winutils on Windows.

---

## 8. Interface Hand-off to Teammates

- **Member 2 (Mining & Classification):** Delivered clean feature definitions, verified timestamp semantics, and flag co-occurrence tables.
- **Member 3 (Dashboard & Prioritization):** Delivered formal contract in [`docs/member1/artifact_contract.md`](file:///c:/Users/theni/OneDrive/Documents/projects/asbd_project/docs/member1/artifact_contract.md), 16 Parquet tables, 15 figure files, `phase3_manifest.json`, `phase3_metrics.json`, and small non-sensitive JSON fixtures in [`tests/fixtures/member1/`](file:///c:/Users/theni/OneDrive/Documents/projects/asbd_project/tests/fixtures/member1/).

---

## 9. Blocker & Review Findings Resolution

- **Hotspot Cell 25:31 Labeling Corrected:** Verified and corrected geographic description of cell `25:31` ($40.745^\circ\text{N}, -73.945^\circ\text{W}$, `dominant_borough`: `QUEENS`) to Long Island City / Sunnyside, Queens (was mislabeled as Crown Heights/Bed-Stuy).
- **Weekday-Hour Heatmap Normalized:** Updated `hour_weekday` to calculate `days_observed` and `avg_complaints_per_hour` per (weekday, hour) pair. The heatmap now plots normalized hourly rates, preventing 2 observed Wednesdays from appearing twice as intense as 1 observed Saturday/Sunday.
- **Demand Averages Separate Complete Days:** Daily demand now explicitly reports both the 11 complete days (Jan 1–11: 9,074 complaints/day, range 6,703-11,006) and the overall 12-day arithmetic average (8,333 complaints/day including the 191 records from partial Jan 12).
- **Scalable Spark Resolution Statistics:** Replaced driver-side raw row collection with distributed Spark bucketing (`resolution_histogram_spark`, 50 rows) and Spark five-number summary statistics (`resolution_box_stats_spark`, 6 rows). Zero raw resolved records are collected on the driver.
- **Coverage Terminology Clarified:** Replaced ambiguous `coverage_ratio: 1.0` with explicit `observed_span_completeness: 1.0` (Jan 1–12 window completeness) and `month_coverage_ratio: 0.3871` (38.71% of January 2024, or 35.48% for full days).
- **Audit Lineage Documented:** Lineage and verification steps recorded in [`reports/member1/phase3_correctness_audit.md`](file:///c:/Users/theni/OneDrive/Documents/projects/asbd_project/reports/member1/phase3_correctness_audit.md).
- **Pre-Merge Cleanup & Reconciliations:**
  - *Quantile Unification:* Standardized Spark resolution quantile estimation across all tables, reports, and figure takeaways to `accuracy=50,000` (Median: 15.61 h, P90: 699.28 h, P99: 6,973.40 h), eliminating all legacy accuracy=10,000 estimates.
  - *Verified Resolved Population:* Corrected outdated docstring reference (79,252 rows) in `resolution_overall_stats` to the verified population count of 98,877 resolved rows.
  - *Missing Category Normalization:* Fixed `_dominant` and `_dominant_by_cluster` so null values are coalesced to `"UNKNOWN"` prior to `groupBy`, preventing split null/UNKNOWN groups, verified by unit/integration tests.
  - *Cluster ID Stability Notice:* Documented in `docs/member1/artifact_contract.md` that MLlib KMeans cluster IDs (0 and 1) are descriptive and subject to index reassignment upon model retraining.
  - *Heuristic Completeness Disclosure:* Formally documented in code and contracts that `min_hour == 0 and max_hour == 23` is an observational completeness heuristic rather than proof of complete ingestion.

---

## 10. Phase 3 Acceptance: PASS

All code, tests, documentation, figures, audit records, and contract deliverables for Phase 3 are complete, mathematically reconciled, and verified.
Phase 3 was the previous review boundary. On 2026-10-25 the user authorized
moving to the next Member 1 phase. See `PROGRESS_MEMBER1_PHASE4.md` for current
execution evidence and remaining work. No new PR creation or merge is claimed.
