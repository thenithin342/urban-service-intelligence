# Phase 3 Correctness Audit & Numerical Reconciliation Report

**Auditor:** Member 1 (Big Data & Machine Learning Engineer)  
**Date:** 2026-10-20  
**Repository:** https://github.com/thenithin342/urban-service-intelligence  
**Branch:** `feature/data-analytics`  
**Execution Environment:** Windows 11, Python 3.10.0 (`.venv`), PySpark 3.5.3 (Spark `local[2]`), Java 17.0.15, Hadoop winutils 3.3.x  

---

## 1. Executive Summary

Following a mandatory review of the Phase 3 implementation report, a rigorous, evidence-based audit was executed to investigate numerical discrepancies across:
1. Input dataset identity and provenance.
2. Borough complaint frequencies.
3. Complaint category distributions.
4. Resolution duration statistics (mean, median, $P_{90}$, $P_{99}$).
5. Spatial clustering parameters, metrics, and cluster summaries.

### Audit Findings Summary
- **Input Data Integrity:** Both the raw CSV (`data/raw/nyc311_sample.csv`) and cleaned Parquet (`data/processed/nyc311_clean`) have identical row counts (100,000), 100,000 distinct `unique_key` values, identical SHA256 provenance (`7ed7541b56fa816459280483cf0d52f9d98b05bf34e884fc113afb87feafb652`), and span exactly `2024-01-01 00:00:00` to `2024-01-12 01:19:58`. The data was **never regenerated or replaced**.
- **Root Cause of Borough & Category Discrepancies:** The underlying PySpark analytical code and the generated Parquet tables in `artifacts/member1/analytics/tables/` (`complaints_by_borough`, `complaints_by_category`) **computed and stored the exact correct Phase 1 values**. The discrepancy originated strictly as an agent narrative reporting error in `PROGRESS_MEMBER1_PHASE3.md` and `reports/member1/phase3_findings.md`, where erroneous numbers had been transcribed.
- **Root Cause of Resolution Duration Discrepancy:** In `scripts/run_member1_analytics.py`, resolution figures (`resolution_hist.png`, `resolution_box.png`) and their manifest takeaways sampled only 20,000 rows (`deterministic_sample(..., cap)` where `cap = 20000`) instead of utilizing all 98,877 resolved records. This sampling artificially skewed the median (15.2 h vs true 15.61 h), $P_{90}$ (684.2 h vs true 699.36 h), and $P_{99}$ (6,631.6 h vs true 6,973.47 h). The previously cited mean of 95.8 h was completely inconsistent with the heavy-tailed distribution (true mean is 340.18 h).
- **Corrective Actions Taken:**
  1. Fixed `scripts/run_member1_analytics.py` to collect all 98,877 resolved records for figure generation.
  2. Re-executed `scripts/run_member1_analytics.py` to produce certified tables, figures, manifest, and metrics.
  3. Corrected all text narratives in `PROGRESS_MEMBER1_PHASE3.md`, `reports/member1/phase3_findings.md`, and `docs/member1/artifact_contract.md`.
  4. Verified full test suite (56 of 56 tests passed) and clean linting (`ruff check` clean).

---

## 2. Discrepancy 1: Input Data Identity & Lineage

### Investigation
The raw CSV and cleaned Parquet were audited independently via SHA256 hashing, row counting, and column profiling.

| Dataset Entity | File Path | Format | Record Count | Distinct Unique Keys | Date Span (Local NYC Wall Time) | SHA256 / CRC Checksum |
| :--- | :--- | :--- | :---: | :---: | :--- | :--- |
| **Raw Sample CSV** | `data/raw/nyc311_sample.csv` | CSV | 100,000 | 100,000 | `2024-01-01 00:00:00` to `2024-01-12 01:19:58` | `7ed7541b56fa816459280483cf0d52f9d98b05bf34e884fc113afb87feafb652` |
| **Cleaned Parquet** | `data/processed/nyc311_clean` | Parquet | 100,000 | 100,000 | `2024-01-01 00:00:00` to `2024-01-12 01:19:58` | Verified idempotent hash matching Phase 2 audit |
| **Phase 3 Parquet** | `data/processed/nyc311_clean` | Parquet | 100,000 | 100,000 | `2024-01-01 00:00:00` to `2024-01-12 01:19:58` | Read directly by `scripts/run_member1_analytics.py` |

### Verdict & Evidence
- The raw CSV SHA256 perfectly matches `data/manifests/manifest_sample.json` line 36.
- The dataset was **never modified, regenerated, or replaced**.
- Both Phase 1 and Phase 3 processed the exact same 100,000 SODA API records covering the first 12 days of January 2024.

---

## 3. Discrepancy 2: Borough Complaint Distribution

### Investigation
Phase 1 reported:
`BROOKLYN: 32470, QUEENS: 22836, BRONX: 20995, MANHATTAN: 20092, STATEN ISLAND: 3527, UNKNOWN: 80`

The previous Phase 3 narrative report erroneously reported:
`BROOKLYN: 32470, QUEENS: 28142, MANHATTAN: 19774, BRONX: 14792, STATEN ISLAND: 4742, UNKNOWN: 80`

We recomputed the distributions directly from:
1. `data/raw/nyc311_sample.csv` via independent Pandas aggregation.
2. `data/processed/nyc311_clean` via PySpark `df.groupBy("borough").count()`.
3. `artifacts/member1/analytics/tables/complaints_by_borough` via PyArrow Parquet read.

### Recomputed Results

| Borough | Phase 1 (Reported) | Previous Phase 3 (Erroneous Text) | Actual Raw CSV (Pandas) | Actual Cleaned Parquet (PySpark) | Actual Phase 3 Artifact (`complaints_by_borough`) | Status |
| :--- | :---: | :---: | :---: | :---: | :---: | :--- |
| **BROOKLYN** | 32,470 | 32,470 | 32,470 | 32,470 | 32,470 | Exact Match |
| **QUEENS** | 22,836 | 28,142 | 22,836 | 22,836 | 22,836 | **Report Text Error Fixed** |
| **BRONX** | 20,995 | 14,792 | 20,995 | 20,995 | 20,995 | **Report Text Error Fixed** |
| **MANHATTAN** | 20,092 | 19,774 | 20,092 | 20,092 | 20,092 | **Report Text Error Fixed** |
| **STATEN ISLAND** | 3,527 | 4,742 | 3,527 | 3,527 | 3,527 | **Report Text Error Fixed** |
| **UNKNOWN / None** | 80 | 80 | 80 | 80 | 80 | Exact Match |
| **TOTAL** | **100,000** | **100,000** | **100,000** | **100,000** | **100,000** | Exact Match |

### Root Cause & Resolution
The Parquet table generated by PySpark in Phase 3 (`artifacts/member1/analytics/tables/complaints_by_borough`) had the correct counts all along. The erroneous numbers existed only in the narrative text of the report. All documentation has been aligned with the true measured values.

---

## 4. Discrepancy 3: Complaint Category Distribution

### Investigation
Phase 1 reported:
`Illegal Parking: 15450, HEAT/HOT WATER: 14706, Noise - Residential: 7846, Blocked Driveway: 5504`

The previous Phase 3 narrative report erroneously reported:
`Illegal Parking: 15450, Noise - Residential: 12852, Heat/Hot Water: 11855, Blocked Driveway: 7123`

We recomputed the exact category counts from both raw CSV and cleaned Parquet:

### Recomputed Results

| Rank | Complaint Category | Phase 1 (Reported) | Previous Phase 3 (Erroneous Text) | Actual Raw CSV | Actual Cleaned Parquet | Actual Phase 3 Artifact (`complaints_by_category`) | Share (%) |
| :---: | :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| 1 | **Illegal Parking** | 15,450 | 15,450 | 15,450 | 15,450 | 15,450 | 15.45% |
| 2 | **HEAT/HOT WATER** | 14,706 | 11,855 | 14,706 | 14,706 | 14,706 | 14.71% |
| 3 | **Noise - Residential** | 7,846 | 12,852 | 7,846 | 7,846 | 7,846 | 7.85% |
| 4 | **Blocked Driveway** | 5,504 | 7,123 | 5,504 | 5,504 | 5,504 | 5.50% |
| 5 | **UNSANITARY CONDITION** | 3,905 | 3,828 (misc) | 3,905 | 3,905 | 3,905 | 3.91% |
| 6 | **PAINT/PLASTER** | 2,535 | — | 2,535 | 2,535 | 2,535 | 2.54% |
| 7 | **WATER LEAK** | 2,420 | — | 2,420 | 2,420 | 2,420 | 2.42% |
| 8 | **Abandoned Vehicle** | 2,393 | — | 2,393 | 2,393 | 2,393 | 2.39% |
| 9 | **PLUMBING** | 2,328 | — | 2,328 | 2,328 | 2,328 | 2.33% |
| 10 | **DOOR/WINDOW** | 1,771 | — | 1,771 | 1,771 | 1,771 | 1.77% |

### Root Cause & Resolution
The Phase 3 table `artifacts/member1/analytics/tables/complaints_by_category` contained the exact Phase 1 numbers. The erroneous figures in the prior text were narrative fabrications. All documentation has been corrected.

---

## 5. Discrepancy 4: Resolution Duration Statistics

### Investigation
Phase 3 previously reported:
`Mean: 95.8 h, Median: 15.2 h, P90: 684.2 h, P99: 6631.6 h`

The audit investigated:
1. Non-null closure timestamps.
2. Negative-duration anomalies (`closed_at < created_at`).
3. Genuine zero-duration resolutions (`closed_at == created_at`).
4. Exact quantiles over all valid non-negative resolution durations.

### Population Breakdown ($N=100,000$)
- **Total records:** 100,000
- **Null `closed_at` (open/unresolved):** 1,104 rows (flagged `unresolved`).
- **Negative durations (`closed_at < created_at`):** 19 rows (flagged `negative_duration`). The ETL correctly nulls `resolution_hours` for these rows to avoid negative values.
- **Total null `resolution_hours`:** $1,104 + 19 = 1,123$ rows (1.123%).
- **Valid non-negative resolution durations:** $100,000 - 1,123 = 98,877$ rows (98.877%).
- **Genuine zero-duration resolutions (`created_at == closed_at`):** 1,335 rows (1.35% of resolved rows). These are valid municipal resolutions occurring within the same second, not imputation.
- **Strictly positive resolution durations:** $98,877 - 1,335 = 97,542$ rows.

### Mathematical Recomputation of Resolution Quantiles

| Metric | Previously Reported in Phase 3 Text | Manifest Figure Takeaway (Pre-Fix) | True Value on All 98,877 Valid Resolved Rows | Independent Pandas / NumPy Verification |
| :--- | :---: | :---: | :---: | :---: |
| **Mean** | 95.8 h | — | **340.1838 h** (~14.17 days) | **340.1838 h** |
| **Median ($P_{50}$)** | 15.2 h | 15.2 h | **15.6103 h** (~15.61 h) | **15.6103 h** |
| **90th Percentile ($P_{90}$)** | 684.2 h | 684.2 h | **699.3587 h** (~29.14 days) | **699.3587 h** |
| **99th Percentile ($P_{99}$)** | 6,631.6 h | 6,631.6 h | **6,973.4673 h** (~290.56 days) | **6,973.4673 h** |
| **Maximum** | — | — | **23,639.3667 h** (~984.97 days) | **23,639.3667 h** |

### Per-Borough Resolution Statistics (Exact PySpark Percentile Approx)
From `artifacts/member1/analytics/tables/resolution_by_borough`:
- `BRONX` (20,910 resolved): Mean 267.08 h, Median 25.68 h, $P_{90}$ 694.01 h, $P_{99}$ 3,576.77 h.
- `BROOKLYN` (32,206 resolved): Mean 357.47 h, Median 10.59 h, $P_{90}$ 713.71 h, $P_{99}$ 7,712.22 h.
- `MANHATTAN` (19,665 resolved): Mean 390.09 h, Median 22.58 h, $P_{90}$ 1,084.99 h, $P_{99}$ 5,008.40 h.
- `QUEENS` (22,536 resolved): Mean 315.08 h, Median 6.87 h, $P_{90}$ 406.22 h, $P_{99}$ 6,973.40 h (fastest median).
- `STATEN ISLAND` (3,483 resolved): Mean 481.41 h, Median 14.63 h, $P_{90}$ 699.28 h, $P_{99}$ 11,924.91 h.
- `UNKNOWN` (77 resolved): Mean 1,175.30 h, Median 191.74 h, $P_{90}$ 3,619.50 h, $P_{99}$ 8,563.82 h.

### Root Cause & Code Fix
In `scripts/run_member1_analytics.py` lines 197-205, figure generation passed a 20,000-row sample to `histogram_resolution` and `box_resolution_by_borough`:
```python
# OLD BUGGY CODE:
resolved = spark_eda.bounded_pandas(
    geospatial.deterministic_sample(
        df.filter(F.col("resolution_hours").isNotNull()).select(
            "unique_key", "resolution_hours", "borough"
        ),
        cap,  # cap was 20,000!
    ),
    cap,
)
```
Because the resolution distribution has severe positive skew ($P_{99} \approx 6,973\text{ h}$, max $\approx 23,639\text{ h}$), taking a 20,000-row sample distorted the tail quantiles and produced median 15.2 h, $P_{90}$ 684.2 h, and $P_{99}$ 6,631.6 h.

**Fix Applied:** `scripts/run_member1_analytics.py` was modified to collect all 98,877 resolved records (guarded by a 200,000-row cap). Upon re-running, the figure generator computed the exact values:
`Median 15.6 h (p90 699.4 h, p99 6973.5 h)`.

---

## 6. Discrepancy 5: Clustering Metrics & Cluster Breakdown

### Investigation
The clustering audit verified:
1. Feature space definition and local tangent-plane projection.
2. Preprocessing ablation on Spark MLlib KMeans.
3. Cluster breakdown and complaint totals.
4. Bounded single-node DBSCAN sampling.

### Grid & Feature Construction
- Coordinate bounding box: $[40.49^\circ, 40.92^\circ]\text{N}, [-74.26^\circ, -73.70^\circ]\text{W}$.
- Total active grid cells ($\Delta = 0.01^\circ$): **873 cells**.
- Valid coordinate complaints: **98,683 rows** (1,317 rows excluded strictly from spatial tables due to invalid/missing coordinates).
- Projection anchor: Lat $40.7^\circ\text{N}$, Lon $-73.95^\circ\text{W}$ (equirectangular tangent plane in metres).

### Preprocessing Ablation Verification (PySpark MLlib KMeans, Seed 42)
All values in `artifacts/member1/analytics/tables/kmeans_ablation` were independently reproduced:

| Preprocessing Variant | $k$ | Silhouette Score | Within-Set SSE (WSSSE) | Status |
| :--- | :---: | :---: | :---: | :--- |
| **`log_count_scaled`** | **2** | **0.598994** | **1,561.557** | **Reproduced exactly (headline model)** |
| `log_count_scaled` | 3 | 0.537558 | 1,063.367 | Reproduced exactly |
| `log_count_scaled` | 4 | 0.512676 | 818.464 | Reproduced exactly |
| `log_count_scaled` | 5 | 0.507907 | 686.054 | Reproduced exactly |
| `log_count_scaled` | 6 | 0.546357 | 573.241 | Reproduced exactly |
| `log_count_scaled` | 8 | 0.490591 | 464.548 | Reproduced exactly |
| `log_count_unscaled` | 2 | 0.666519 | $9.1995 \times 10^{10}$ | Unscaled metric distance dwarfs log volume ($<8$) |
| `count_scaled` | 2 | 0.533448 | 1,599.804 | Extreme hotspot cells distort cluster assignments |

### Verified Cluster Summary (`artifacts/member1/analytics/tables/cluster_summary`)
For the selected model ($k=2$, `log_count_scaled`):

| Cluster | Cells | Total Complaints | Centroid Lat | Centroid Lon | Centroid $x_m$ (m) | Centroid $y_m$ (m) | Mean Complaints / Cell | Mean Resolution (h) | Dominant Borough | Dominant Category |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- | :--- |
| **0** | **664** | **93,956** | $40.73440^\circ\text{N}$ | $-73.87360^\circ\text{W}$ | 6,447.87 | 3,803.48 | 141.50 | 359.00 h | QUEENS | Illegal Parking |
| **1** | **209** | **4,727** | $40.58754^\circ\text{N}$ | $-74.09725^\circ\text{W}$ | -12,427.14 | -12,435.61 | 22.62 | 606.11 h | STATEN ISLAND | Illegal Parking |
| **Total** | **873** | **98,683** | — | — | — | — | — | — | — | — |

*Reconciliation Check:* $664 + 209 = 873$ cells (100%). $93,956 + 4,727 = 98,683$ complaints (exactly equal to `valid_coords_rows`).

### Bounded Scikit-Learn DBSCAN Audit
- Point sample: 20,000 rows (deterministic CRC32 ordering over valid coordinates).
- Architecture: Single-node Scikit-Learn `DBSCAN(metric="haversine")` on radian coordinates. Disclosed as non-distributed.
- At $\epsilon = 250\text{ m}$, $\text{min\_pts} = 25$:
  - Clusters: 103 micro-clusters.
  - Noise points: 11,979 points (59.895% noise).
- Sweep ($\text{min\_pts}=25$):
  - $\epsilon=100\text{ m}$: 30 clusters, 92.76% noise.
  - $\epsilon=500\text{ m}$: 20 clusters, 12.88% noise.
  - $\epsilon=1000\text{ m}$: 8 clusters, 2.21% noise.

---

## 7. Automated Test Verification & Quality Gates

The test suite and static analysis were executed using the dedicated `.venv` Python 3.10 interpreter:

1. **Test Collection:**
   ```powershell
   & .venv\Scripts\python.exe -m pytest --collect-only -q
   ```
   *Output:* `56 tests collected in 4.70s` (0 errors).

2. **Full Pytest Suite:**
   ```powershell
   & .venv\Scripts\python.exe -m pytest -q
   ```
   *Output:* `56 passed, 1 warning in 532.82s (0:08:52)` (100% passing rate).
   - 17 Phase 3 unit and Spark integration tests passed.
   - All legacy Phase 0, 1, and 2 tests passed.
   - All PySpark Python-worker tests (`test_spark_worker.py`) passed.

3. **Linter & Code Style:**
   ```powershell
   & .venv\Scripts\python.exe -m ruff check src scripts tests
   ```
   *Output:* `All checks passed!` (0 findings).

4. **Phase 3 CLI End-to-End Pipeline:**
   ```powershell
   & .venv\Scripts\python.exe scripts/run_member1_analytics.py --config configs/sample.yaml
   ```
   *Output:* Completed successfully. 16 Parquet tables and 15 figures generated into `artifacts/member1/analytics/`.

---

## 8. Summary of Files Changed

| File | Change Description |
| :--- | :--- |
| `scripts/run_member1_analytics.py` | Fixed resolution figure generation to collect all 98,877 resolved rows instead of a 20k sample. |
| `artifacts/member1/analytics/phase3_manifest.json` | Regenerated with exact resolution quantiles (median 15.6 h, P90 699.4 h, P99 6973.5 h). |
| `reports/member1/phase3_findings.md` | Overhauled with exact verified numbers for borough counts, categories, resolution stats, and cluster breakdown. |
| `PROGRESS_MEMBER1_PHASE3.md` | Aligned all reported findings, tables, takeaways, and metrics with measured data. |
| `docs/member1/artifact_contract.md` | Updated figure takeaways and resolution quantile descriptions. |
| `reports/member1/phase3_correctness_audit.md` | Created this comprehensive audit report. |

---

## 9. Remaining Limitations & Next Phase Readiness

- **Sample Scope:** The dataset represents the first 100,000 records of January 2024 (January 1–12), ordered by `unique_key`. Seasonal cycles and long-term trends cannot be derived until Phase 4 expands historical coverage.
- **Compute Architecture:** Spark runs in `local[2]` mode on Windows. DBSCAN runs as single-node Scikit-Learn.
- **Phase 3 Acceptance:** **PASS**. All numerical inconsistencies are resolved, verified, and backed by reproducible code.

---

## 10. Audit Addendum: Resolution of Review Findings

Following the initial audit review, five additional methodological improvements were implemented and verified with regression tests:

### 10.1. Geographical Label of Hotspot Cell 25:31 Corrected
- **Issue:** Hotspot cell `25:31` was previously referred to as Crown Heights / Bed-Stuy, Brooklyn in report prose.
- **Verification:** Center coordinates $40.745^\circ\text{N}, -73.945^\circ\text{W}$ are located in Long Island City / Sunnyside, Queens. The underlying Spark table `spatial_grid_cells` correctly identifies `dominant_borough: QUEENS`.
- **Correction:** Prose updated across all reports and documentation to **Long Island City / Sunnyside, Queens**. Regression unit test `test_hotspot_cell_25_31_queens_geographic_label` added.

### 10.2. Weekday-Hour Heatmap Normalized for Observed Frequency
- **Issue:** In a 12-day sample (Jan 1–12, 2024), Mondays through Thursdays occur twice, whereas Saturday and Sunday occur only once, and Friday Jan 12 is truncated (hours 0..1). Raw counts make weekdays appear twice as busy as weekends purely due to calendar frequency.
- **Correction:** In `src/analytics/spark_eda.py`, `hour_weekday_heatmap()` now calculates `days_observed` and `avg_complaints_per_hour = round(complaint_count / days_observed, 2)`.
- **Visualization:** `figures.py::heatmap_hour_weekday` plots normalized hourly rates. Takeaway updated: Peak normalized hourly rate is Wednesday 11:00 AM with **721.5 average records/hour** across the 2 observed Wednesdays. Regression test `test_hour_weekday_heatmap_normalized` added.

### 10.3. Demand Averages Separate Complete vs. Truncated Days
- **Issue:** January 12 contains only 191 records spanning 1.33 hours before API cutoff, artificially lowering the arithmetic daily mean.
- **Correction:** `spark_eda.py::time_coverage` and `figures.py::timeseries_daily` now report both:
  - **11 complete days (Jan 1–11):** 99,809 complaints, averaging **9,074 complaints/day** (range 6,703 to 11,006).
  - **All 12 days (including partial Jan 12):** 100,000 complaints, averaging **8,333 complaints/day**.
  - `timeseries_daily.png` marks Jan 12 distinctly with a red square and annotation. Regression test `test_timeseries_daily_distinguishes_partial_day` added.

### 10.4. Scalable Spark Population Statistics Replacing Driver Row Collection
- **Issue:** Collecting all 98,877 resolved rows to pandas on the driver creates an architectural vulnerability that would cause `OutOfMemoryError` on production multi-million row datasets.
- **Correction:** Implemented distributed Spark aggregation functions:
  - `resolution_histogram_spark(df, n_bins=50)`: buckets rows in Spark, returning a 50-row DataFrame (`bin_start`, `bin_end`, `count`) to the driver.
  - `resolution_box_stats_spark(df)`: computes exact five-number summary statistics (`q1`, `med`, `q3`, `whislo`, `whishi`, `mean_hours`) per borough in Spark, returning a 6-row DataFrame to the driver.
  - `figures.py::histogram_resolution` and `box_resolution_by_borough` render directly via `ax.bar` and `ax.bxp`.
  - **Zero raw resolution rows are collected on the driver.** Bounded collection guards strictly respected. Regression test `test_spark_resolution_histogram_and_box_stats` added.

### 10.5. Temporal Coverage Terminology Clarified
- **Issue:** Reporting `coverage_ratio: 1.0` was misleading because the dataset only covers 12 days of January 2024, not the full 31-day month.
- **Correction:** `spark_eda.py::time_coverage` now provides unambiguous metrics:
  - `observed_span_days`: 12
  - `observed_span_completeness`: 1.0 (all 12 calendar days in observed span have $\ge 1$ record)
  - `full_days_count`: 11
  - `partial_days_count`: 1 (Jan 12)
  - `target_calendar_month`: "2024-01" (31 days)
  - `month_coverage_ratio`: **0.3871** (38.71% of January 2024)
  - `full_day_month_coverage_ratio`: **0.3548** (35.48% of January 2024)
  - Explicit note documenting that this is an incomplete 12-day sample of January 2024. Regression test `test_time_coverage_expanded_metrics` added.

