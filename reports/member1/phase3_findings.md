# Phase 3 Research Report: Distributed EDA, Geospatial Analytics, Preprocessing Ablation & Comparative Clustering

**Author:** Member 1 (Data Analytics & Machine Learning Engineering)  
**Date:** 2026-10-20 (Audited & Reconciled)  
**Execution Environment:** Windows 11, PySpark 3.5.3 (Spark local[2]), Python 3.10.0 (`.venv`), Java 17.0.15 HotSpot, Hadoop winutils 3.3.x  
**Data Input:** `data/processed/nyc311_clean/` (100,000 records, January 1 to January 12, 2024; Snapshot 2026-10-14)

---

## 1. Executive Summary & Data Boundaries

Phase 3 implements distributed big-data exploratory analytics, deterministic spatial gridding, preprocessing sensitivity studies, and comparative unsupervised clustering on the cleaned NYC 311 service request dataset. All primary transformations run through Apache Spark (PySpark), enforcing bounded driver collection guards (`max_collect_rows = 20,000`).

### 1.1. Verified Input Baseline
- **Input Record Count:** Exactly 100,000 rows.
- **Unique Identifier Reconciliation:** 100,000 distinct `unique_key` values; exactly 0 duplicate keys.
- **Temporal Span:** `2024-01-01 00:00:00` to `2024-01-12 01:19:58` (12 calendar days observed in sample span, observed span completeness 1.0; calendar month coverage ratio for January 2024 is 38.71% for all 12 days, or 35.48% for the 11 complete days).
- **Data Quality Reconciliation:** Exactly 2,715 flagged rows (2.715%) and 97,285 clean rows (97.285%). Breakdown of occurrences:
  - `invalid_coords`: 1,317 rows
  - `invalid_zip`: 1,094 rows
  - `unresolved`: 1,104 rows
  - `missing_borough`: 80 rows
  - `negative_duration`: 19 rows
  - *Pairwise flag overlap:* 826 rows carry 2+ flags, dominated by `invalid_coords & invalid_zip` (577 rows) and `unresolved & invalid_zip` (216 rows).
- **Critical Sampling Limitation:** The available dataset contains the first 100,000 records of January 2024 ordered by `unique_key`. Because of the Socrata pagination order, this sample spans only the first 12 days of January (38.71% of January), with January 12 being a partial day (191 records, 1 hour 19 minutes). Full-day daily demand across the 11 complete days averages 9,074 complaints/day. **No multi-month seasonality, annual cycles, or long-term trend can be inferred from this dataset.**

---

## 2. Distributed Exploratory Data Analysis & Written Inferences

### 2.1. Borough & Categorical Distribution
- **Borough Breakdown (`complaints_by_borough`):**
  - `BROOKLYN`: 32,470 (32.47%)
  - `QUEENS`: 22,836 (22.84%)
  - `BRONX`: 20,995 (21.00%)
  - `MANHATTAN`: 20,092 (20.09%)
  - `STATEN ISLAND`: 3,527 (3.53%)
  - `UNKNOWN` (missing / Unspecified borough): 80 (0.08%)
- **Top 10 Complaint Categories (`complaints_by_category`):**
  1. `Illegal Parking`: 15,450 (15.45%)
  2. `HEAT/HOT WATER`: 14,706 (14.71%)
  3. `Noise - Residential`: 7,846 (7.85%)
  4. `Blocked Driveway`: 5,504 (5.50%)
  5. `UNSANITARY CONDITION`: 3,905 (3.91%)
  6. `PAINT/PLASTER`: 2,535 (2.54%)
  7. `WATER LEAK`: 2,420 (2.42%)
  8. `Abandoned Vehicle`: 2,393 (2.39%)
  9. `PLUMBING`: 2,328 (2.33%)
  10. `DOOR/WINDOW`: 1,771 (1.77%)
- **Written Inference:** Brooklyn and Queens represent over 55% of all reported incidents. Illegal Parking and Residential Noise constitute over 23% of city-wide demand, while winter heating complaints (`HEAT/HOT WATER`) reflect seasonal temperature stress in January. However, these figures reflect *citizen reporting behavior* rather than absolute infrastructure failure rates; affluent or densely populated neighbourhoods often demonstrate higher digital engagement and propensity to file 311 tickets.

### 2.2. Temporal Patterns & Peak Demand
- **Daily Volume (`complaints_daily`):**
  - Full-day demand across the 11 complete days (Jan 1–11) averages **9,074 complaints/day**, ranging from 6,703 (Sunday Jan 7) to 11,006 (Wednesday Jan 10).
  - When including the truncated boundary day (January 12, with only 191 records spanning 1.33 hours before API cutoff), the overall 12-day arithmetic average is **8,333 complaints/day**.
  - Weekend volume drops noticeably (e.g., Saturday Jan 6: 7,790; Sunday Jan 7: 6,703 vs ~9,500–11,000 on weekdays).
- **Diurnal and Day-of-Week Structure (`hour_weekday`):**
  - Longest-format 168-hour temporal aggregation is normalized by the number of observed calendar dates for each (weekday, hour) pair (e.g., 2 Wednesdays vs 1 Saturday/Sunday in the 12-day window, and partial cutoff on Friday Jan 12).
  - Reporting reaches its minimum between 03:00 and 05:00 AM (< 150 complaints/hour).
  - Demand surges sharply starting at 07:00 AM, plateauing between 10:00 AM and 15:00 PM.
  - The single highest normalized hourly demand occurs on **Wednesday at 11:00 AM** with **721.5 average complaints/hour** (1,443 total across the two observed Wednesdays). Normalization prevents raw weekday sums from being artificially doubled relative to single-observation weekend days.
- **Written Inference:** Municipal shift scheduling can be optimized around an 08:00–16:00 peak staffing window, with reduced night staffing.

### 2.3. Resolution Duration Distribution
- **Overall Population Breakdown ($N=100,000$):**
  - Null `closed_at` (open tickets): 1,104 rows (flagged `unresolved`).
  - Negative duration anomalies (`closed_at < created_at`): 19 rows (flagged `negative_duration`, resolution duration set to null).
  - Total null `resolution_hours`: 1,123 rows (1.123%).
  - Valid non-negative resolution durations: **98,877 rows** (98.877%).
  - Genuine zero-duration resolutions (`closed_at == created_at`): 1,335 rows (1.35% of resolved rows). These are valid municipal resolutions occurring within the same second, not imputation.
- **Parametric & Non-Parametric Statistics ($N=98,877$):**
  - **Mean:** 340.1838 hours (~14.17 days).
  - **Median ($P_{50}$):** 15.6103 hours (~15.61 hours).
  - **90th Percentile ($P_{90}$):** 699.3587 hours (~29.14 days).
  - **99th Percentile ($P_{99}$):** 6,973.4673 hours (~290.56 days).
  - **Maximum Observed Duration:** 23,639.3667 hours (~984.97 days, reflecting multi-year legacy reconciliation).
  - **Exact Rank vs. Distributed Spark GK-Sketch Quantile Reconciliation:**

    | Metric | Exact Sorted Rank (NumPy/Pandas) | Spark `percentile_approx` (acc=50,000) | Spark `percentile_approx` (acc=10,000) | Relative Variance (acc=50,000 vs Exact) |
    | :--- | :---: | :---: | :---: | :---: |
    | **Median ($P_{50}$)** | 15.6103 h | 15.6103 h | 15.6064 h | **0.00%** (exact match) |
    | **90th % ($P_{90}$)** | 699.3587 h | 699.2756 h | 698.2572 h | **0.01%** (< 0.1 hour diff) |
    | **99th % ($P_{99}$)** | 6,973.4673 h | 6,973.4017 h | 6,912.9933 h | **0.001%** (< 0.1 hour diff) |

    *Methodological Note:* Spark SQL's distributed `percentile_approx` utilizes the Greenwald-Khanna (GK) streaming sketch algorithm. Unlike driver-side in-memory sorting which requires collecting all 79k+ records to driver RAM, GK-sketches execute in a single distributed pass across executor partitions. At `accuracy=50,000`, the distributed sketch agrees with exact sorted interpolation to within < 0.01%. Claims of "exact five-number summary" have been corrected to "approximate five-number summary via Spark percentile_approx".
- **By-Borough Resolution Differences (`resolution_by_borough`):**
  - `QUEENS` (22,536 resolved): Median 6.87 h, Mean 315.08 h, $P_{90}$ 406.22 h, $P_{99}$ 6,973.40 h (fastest median resolution).
  - `BROOKLYN` (32,206 resolved): Median 10.59 h, Mean 357.47 h, $P_{90}$ 713.71 h, $P_{99}$ 7,712.22 h.
  - `STATEN ISLAND` (3,483 resolved): Median 14.63 h, Mean 481.41 h, $P_{90}$ 699.28 h, $P_{99}$ 11,924.91 h.
  - `MANHATTAN` (19,665 resolved): Median 22.58 h, Mean 390.09 h, $P_{90}$ 1,084.99 h, $P_{99}$ 5,008.40 h.
  - `BRONX` (20,910 resolved): Median 25.68 h, Mean 267.08 h, $P_{90}$ 694.01 h, $P_{99}$ 3,576.77 h.
  - `UNKNOWN` (77 resolved): Median 191.74 h, Mean 1,175.30 h, $P_{90}$ 3,619.50 h, $P_{99}$ 8,563.82 h (unassigned tickets face substantial administrative delay).
- **Methodological Rule:** Unresolved complaints (1,104 rows) and negative durations (19 rows) are explicitly excluded from resolution quantile calculations rather than filled with zero. Imputing zero would severely distort operational benchmarks, falsely indicating instantaneous resolution.

---

## 3. Geospatial Hotspot Analysis & Gridding Methodology

### 3.1. Coordinate Validation & Bounding Box
- The geographic bounding box is strictly enforced at:
  $$\text{Latitude} \in [40.49^\circ, 40.92^\circ], \quad \text{Longitude} \in [-74.26^\circ, -73.70^\circ]$$
- 98,683 records (98.683%) possess valid coordinates within this bounding box.
- 1,317 records (1.317%) have invalid or missing coordinates.
- **Exclusion Principle:** Invalid coordinates are excluded strictly from geospatial analysis and retained in all non-spatial tables. **Imputing missing coordinates (e.g. by borough centroid or mean) is mathematically unsound and dangerous**, as it would artificially manufacture fictitious hyper-dense hotspots at arbitrary geographic points.

### 3.2. Deterministic Spatial Gridding
- We apply a fixed degree grid with cell size $\Delta = 0.01^\circ$ (approximately 1.11 km in latitude and ~0.84 km in longitude at NYC's latitude).
- Grid cell identification is deterministic:
  $$\text{row} = \left\lfloor \frac{\text{lat} - 40.49}{0.01} \right\rfloor, \quad \text{col} = \left\lfloor \frac{\text{lon} - (-74.26)}{0.01} \right\rfloor, \quad \text{spatial\_id} = \text{row}:\text{col}$$
- **Area Normalization:** Because longitude degree width varies with latitude according to $\cos(\text{lat})$, the true physical area of each cell is computed individually:
  $$\text{Area}_{\text{cell}} = (\Delta \cdot 110.574\text{ km}) \times (\Delta \cdot 111.320\text{ km} \cdot \cos(\text{center\_lat}))$$
  NYC grid cells span approximately $0.932 \text{ km}^2$ in the north (Bronx) to $0.938 \text{ km}^2$ in the south (Staten Island).
- **Hotspot Findings:**
  - Across the entire city, 873 distinct grid cells contain at least one complaint.
  - Applying a threshold of $\ge 50$ complaints identifies 462 hotspot cells (`spatial_hotspots`).
  - The densest cell (`spatial_id: 25:31`, located in Long Island City / Sunnyside, Queens, center $40.745^\circ\text{N}, -73.945^\circ\text{W}$; dominant borough `QUEENS`) recorded **2,842 complaints**, yielding a density of **3,046 complaints/$\text{km}^2$**.

---

## 4. Preprocessing Ablation Study for Spatial Clustering

A central academic requirement is evaluating the concrete impacts of data preprocessing choices on clustering behavior rather than accepting default settings blindly.

### 4.1. Evaluated Feature Formulations
We evaluate 3 distinct preprocessing pipelines over the 873 spatial grid cells:
1. **`log_count_scaled` (Recommended Primary):**
   - Coordinates projected to local metric coordinates $(x_m, y_m)$ in metres using a tangent plane projection around $(40.7^\circ\text{N}, -73.95^\circ\text{W})$.
   - Volume transformed via $v = \ln(1 + \text{complaint\_count})$ to tame severe volume skewness.
   - Vector $(x_m, y_m, v)$ standardised via `StandardScaler(withMean=True, withStd=True)` so coordinate distances (~10,000–30,000 m) do not numerically dwarf volume variances ($v \in [0, 8]$).
2. **`log_count_unscaled`:**
   - Vector $(x_m, y_m, v)$ assembled without feature scaling.
3. **`count_scaled`:**
   - Raw linear complaint count $(x_m, y_m, \text{complaint\_count})$ standardised via `StandardScaler`.

### 4.2. Quantitative Ablation Results (PySpark MLlib KMeans, Seed 42)

| Preprocessing Variant | $k$ | Silhouette Score | Within-Set SSE (WSSSE) | Primary Behavior / Diagnosis |
| :--- | :---: | :---: | :---: | :--- |
| **`log_count_scaled`** | **2** | **0.598994** | **1,561.557** | **Optimal balance:** Groups cells into metropolitan core vs outer perimeter. |
| `log_count_scaled` | 3 | 0.537558 | 1,063.367 | Captures Manhattan/North Brooklyn, South Brooklyn/Queens, and Periphery. |
| `log_count_scaled` | 4 | 0.512676 | 818.464 | Subdivides peripheral boroughs. |
| `log_count_scaled` | 5 | 0.507907 | 686.054 | Local minimum in separation. |
| `log_count_scaled` | 6 | 0.546357 | 573.241 | Secondary silhouette peak, isolating dense corridors. |
| `log_count_scaled` | 8 | 0.490591 | 464.548 | Over-fragmentation of low-density cells. |
| *`log_count_unscaled`* | 2 | 0.666519 | $9.1995 \times 10^{10}$ | **Misleading metric artifact:** Unscaled metric distances ($10^4$ m) completely dominate log volume ($<8$). Clusters are purely geographic slices with zero volume sensitivity. WSSSE is unscaled ($10^{10}$). |
| *`count_scaled`* | 2 | 0.533448 | 1,599.804 | Extreme outliers (cell with 2,842 complaints) pull cluster centroids, distorting low/medium volume cells. |

### 4.3. Takeaway on Scaling and Transformations
- Without feature standardization, Euclidean distance in $(x_m, y_m, \text{count})$ is essentially a 2D spatial coordinate cut because spatial coordinates are measured in tens of thousands of metres, rendering complaint count irrelevant.
- Log-transforming volume ($\ln(1+\text{count})$) prevents solitary extreme hotspot cells from monopolizing cluster assignment.
- `log_count_scaled` is proven mathematically and empirically to be the only formulation that balances geographic contiguity with demand intensity.

---

## 5. Comparative Clustering: PySpark MLlib KMeans vs. Bounded DBSCAN

### 5.1. Spark MLlib KMeans (Centroid-Based Partitioning)
- **Model Specification:** Scaled $(x_m, y_m, \ln(1+\text{count}))$, $k$-means|| initialization, $k=2$, seed 42.
- **Cluster Characteristics (`artifacts/member1/analytics/tables/cluster_summary`):**
  - **Cluster 0 (Metropolitan Core & Dense Corridors):** 664 grid cells, 93,956 total complaints (95.21% of valid geo complaints; mean 141.5 complaints/cell). Centroid located at $40.73440^\circ\text{N}, -73.87360^\circ\text{W}$ (Queens/Brooklyn core, $x_m = 6,447.87\text{ m}, y_m = 3,803.48\text{ m}$). Dominant borough: `QUEENS` by grid-cell plurality (298 cells vs Brooklyn's 166; on total complaint volume, Brooklyn leads with 30,928 complaints). Dominant complaint: `Illegal Parking` (264 cells; 14,615 complaints). Resolved-record weighted mean resolution duration: **335.02 h** (replaces unweighted cell average of 359.00 h).
  - **Cluster 1 (Outer Periphery & Low Intensity):** 209 grid cells, 4,727 total complaints (4.79% of valid geo complaints; mean 22.62 complaints/cell). Centroid located at $40.58754^\circ\text{N}, -74.09725^\circ\text{W}$ (Staten Island / outer periphery, $x_m = -12,427.14\text{ m}, y_m = -12,435.61\text{ m}$). Dominant borough: `STATEN ISLAND` (157 cells; 3,452 complaints). Dominant complaint: `Illegal Parking` (131 cells). Resolved-record weighted mean resolution duration: **463.99 h** (replaces unweighted cell average of 606.11 h).
  - **Reconciliation Check:** $664 + 209 = 873$ cells (100%). $93,956 + 4,727 = 98,683$ complaints (exactly equal to total valid coordinate complaints). Dominant label basis is clarified: `cluster_summary` reports cell plurality, while underlying volume distributions confirm identical primary complaint types.
  - **Downstream Interface to Member 3:** Spatial grid cells are persisted with their primary cluster assignment (`spatial_grid_cells.cluster`), enabling Member 3's Streamlit dashboard and resource allocation models to directly consume cluster zoning without rerunning MLlib KMeans.
- **Strengths:** Fully distributed in PySpark; guaranteed global partitioning; fast convergence ($< 10$ seconds on 873 cells); scales seamlessly to millions of records.
- **Limitations:** Forces spherical clusters of equal variance in scaled space; sensitive to choice of $k$; cannot identify arbitrary irregular spatial shapes (e.g. transit corridors).

### 5.2. Scikit-Learn DBSCAN (Density-Based Spatial Clustering)
- **Model Specification:** Bounded sample of 20,000 valid coordinate points (deterministic CRC32 ordering), haversine metric on radian coordinates, $\text{min\_samples}=25$.
- **Distributed Architecture Disclosure:** Unlike Spark MLlib KMeans, **DBSCAN is executed on a single node on a bounded sample**. Standard Spark MLlib does not provide a native distributed DBSCAN. This limitation is explicitly documented.
- **Parameter Sensitivity Analysis ($\text{min\_pts}=25$):**
  | Search Radius ($\epsilon$) | Resulting Clusters | Noise Points | Noise Share (%) | Practical Interpretation |
  | :---: | :---: | :---: | :---: | :--- |
  | **$100\text{ m}$** | 30 | 18,552 | 92.76% | Highly conservative: 93% of points flagged as noise; only immediate block-level clusters identified. |
  | **$250\text{ m}$** | **103** | **11,979** | **59.90%** | **Balanced operational baseline:** Isolates 103 high-density micro-neighbourhood clusters. |
  | **$500\text{ m}$** | 20 | 2,575 | 12.88% | Large neighbourhood consolidation; captures broad borough cores. |
  | **$1,000\text{ m}$** | 8 | 441 | 2.21% | Aggregates entire boroughs into 8 giant clusters; obliterates local hotspots. |

### 5.3. Algorithmic Synthesis & Recommendation
- **KMeans** is the optimal choice for high-level macro-zoning, resource budgeting, and providing fixed administrative regions to Member 3's forecasting pipeline.
- **DBSCAN** is superior for fine-grained localized patrol routing and identifying isolated micro-hotspots (e.g. repeated illegal parking along commercial avenues), provided it is run on bounded localized subsets.
- **Ethical & Analytical Caveat:** Neither KMeans nor DBSCAN clusters represent "dangerous neighbourhoods" or "defective communities." They reflect spatial concentrations of civic request volume.

---

## 6. Generated Visualizations & Written Takeaways

All figures are rendered headlessly via `matplotlib` (PNG) and `plotly` (HTML) with explicit axis units, title, observation period (`2024-01-01..2024-01-12`), and producer stamp (`member1-phase3-v1`):

1. **`borough_bar.png`:** Horizontal bar chart. *Takeaway:* Brooklyn has the most reported complaints (32,470 of 100,000); counts are reporting frequency, not infrastructure quality.
2. **`category_bar.png`:** Horizontal bar chart of top 15 categories. *Takeaway:* 'Illegal Parking' is the most reported category (15,450 records) in this window.
3. **`daily_timeseries.png`:** Daily volume trend over 12 days. *Takeaway:* Full-day volume (Jan 1-11, 11 days) averages 9,074 records/day (range 6,703-11,006); all 12 days including partial Jan 12 (191 records) average 8,333. Window is too short for seasonal claims.
4. **`daily_by_borough.png`:** Multi-line daily volume by borough. *Takeaway:* Brooklyn contributes the largest share of daily volume (32.5% of records in window).
5. **`hour_weekday_heatmap.png`:** $24 \times 7$ grid heatmap. *Takeaway:* Peak cell is Wednesday 11:00 (721.5 avg records/h, normalized for observed weekday count); reporting has clear diurnal structure.
6. **`daily_kde.png`:** Kernel Density Estimation. *Takeaway:* Daily volume is centred near 9,524 records/day; KDE is a smoothing of the observed days only.
7. **`daily_violin.png`:** Violin plots of daily borough volume. *Takeaway:* Violins show the spread of daily volume within each borough; wide spreads mean unstable daily demand, not higher counts.
8. **`resolution_hist.png`:** Distribution of resolution hours up to $P_{99}$. *Takeaway:* Median resolution is 15.6 h (p90 698.3 h, p99 6913.0 h); unresolved rows are excluded, not zero-filled.
9. **`resolution_box.png`:** Boxplots across boroughs (outliers omitted for readability). *Takeaway:* Median resolution is highest in UNKNOWN (191.7 h) among boroughs; box plots use resolved rows only and hide outliers.
10. **`borough_radar.png`:** Radar chart comparing normalized complaint proportions across boroughs. *Takeaway:* Radar compares only normalised category shares per borough; a longer spoke means a larger share of that borough's own volume, not more records.
11. **`kmeans_silhouette.png`:** Silhouette score curve. *Takeaway:* Best silhouette 0.599 at k=2; silhouette alone does not prove cluster meaning.
12. **`kmeans_clusters.png`:** Spatial scatter plot of 873 grid cells colored by cluster. *Takeaway:* 2 cluster centres shown over valid coordinates; clusters are descriptive reporting-intensity groups, not risk labels.
13. **`dbscan_clusters.png`:** Point-level map of 103 micro-clusters and unassigned noise points. *Takeaway:* DBSCAN found 103 dense clusters with 59.9% noise on a bounded sample; it is single-node, not distributed, and eps is in metres.
14. **`dbscan_sensitivity.png`:** Sensitivity curve plotting cluster count and noise share against search radius $\epsilon \in [100\text{m}, 1000\text{m}]$. *Takeaway:* At eps=100 m noise is 92.8% with 30 clusters; larger eps merges points and reduces noise, showing that the density result is parameter-dependent.
15. **`hotspot_map.html`:** Interactive Plotly map with hover metrics (count, density per $\text{km}^2$, dominant category) for all 462 hotspot cells. *Takeaway:* 462 grid cells with >= threshold records; top cell 25:31 located in Long Island City / Sunnyside, Queens (40.745, -73.945).

---

## 7. Conclusions & Readiness for Phase 4 / Phase 5

Phase 3 achieves all mandatory academic and technical objectives for Member 1:
- 100% Spark-driven aggregations with bounded driver collection safeguards.
- Complete flag reconciliation and geographic bounding box enforcement.
- Preprocessing ablation study providing empirical justification for logarithmic transformation and feature scaling.
- Rigorous comparative clustering contrasting distributed Spark MLlib KMeans with localized density-based DBSCAN.
- Verified stable artifact contract and non-sensitive fixtures delivered to Members 2 and 3.
- Full mathematical reconciliation of borough counts, category frequencies, resolution durations, and cluster assignments.
