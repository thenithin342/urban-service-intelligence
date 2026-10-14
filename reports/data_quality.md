# Data quality report - Phase 1 sample (measured, not estimated)

Source: NYC 311 Service Requests from 2020 to Present (dataset erm2-nwe9)
Dataset page: https://data.cityofnewyork.us/d/erm2-nwe9
Metadata snapshot: data/manifests/schema_erm2-nwe9.json
  (retrieved 2026-10-14; 48 columns; all 11 required attrs present)
Acquisition manifest: data/manifests/manifest_sample.json
Raw file: data/raw/nyc311_sample.csv (gitignored)
  SHA256: 7ed7541b56fa816459280483cf0d52f9d98b05bf34e884fc113afb87feafb652

## Acquisition

- Filter: created_date between 2024-01-01T00:00:00 and 2024-01-31T23:59:59
- Order: unique_key (stable); pages: 2 x 50000 (SODA $limit/$offset)
- Acquired at (UTC): 2026-10-14T08:16:32
- Source rowsUpdatedAt at acquisition: 1791942200
- Window population (count(*) at check time): 287186 rows for Jan 2024
- Rows received: 100000; rows written: 100000
- Duplicates removed (unique_key): 0
- Failures: none
- NOTE: ordering by unique_key is stable but not chronological. The 100k
  sample covers 2024-01-01 through 2024-01-12 01:19 UTC (first ~12 days of
  the window), NOT the full month. Full-month coverage needs a larger
  target or date-stratified paging (Phase 2 consideration).

## Profile (n = 100000, unique_key unique = 100000)

Duplicate rate: 0.0 (0 duplicates).

Null rates:
- unique_key 0.0, created_date 0.0, complaint_type 0.0, agency 0.0,
  status 0.0, borough 0.0
- closed_date 0.01104 (1104 open/unreported closures)
- descriptor 0.02869
- incident_zip 0.01094
- latitude/longitude 0.01317 each

Date coverage: 2024-01-01 to 2024-01-12 (12 distinct days, min 191 on the
partial last day 01-12, max 11006 on 01-10). created_nat = 0.

Timestamp anomalies: 19 records with closed_date < created_date (flagged,
not repaired).

Coordinates: 1317 records missing or outside lat 40.49-40.92 /
lon -74.26--73.70 (flagged, not repaired).

Borough (normalized; raw retained):
- BROOKLYN 32470, QUEENS 22836, BRONX 20995, MANHATTAN 20092,
  STATEN ISLAND 3527, UNKNOWN 80 (raw value "Unspecified")

## EDA outputs (artifacts/eda_sample/, deterministic)

Top complaint categories:
1. Illegal Parking 15450
2. HEAT/HOT WATER 14706
3. Noise - Residential 7846
4. Blocked Driveway 5504
5. UNSANITARY CONDITION 3905
(heat complaints consistent with January winter season; volume is
reporting frequency, NOT infrastructure quality.)

Daily volume shows weekday/weekend pattern (01-06 Sat 7790, 01-07 Sun 6703
vs ~9500-11000 weekdays); 01-12 partial (191 rows, sample cutoff).

## Reproducibility

- Re-running save_eda_outputs on the saved CSV yields byte-identical
  outputs (unit test test_save_eda_outputs_deterministic).
- Re-downloading the same window may differ if the source dataset changes
  (documented limitation of offset paging); manifest records exact URLs,
  counts, and SHA256 for reconciliation.

## Risks / Phase 2 input

1. ETL must dedupe by unique_key, keep raw + normalized borough, flag
   closed<created and invalid coords, never silently drop.
2. Decide full-month coverage strategy (larger target or stratified pages).
3. Spark Python-worker ops on this host require Python 3.10 (see
   docs/decisions.md); Phase 2 ETL must run there or fix 3.13 transport.
4. Closure modeling (Phase 6) must handle 1.1% missing closed_date and
   right-censoring; 19 anomalous durations need an audit rule.
