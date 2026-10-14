# Data dictionary (erm2-nwe9 subset used)

Verified against live metadata snapshot in data/manifests/ (see
reports/data_quality.md for retrieval date).

Raw input (CSV, explicit schema, all strings, no inference):
unique_key, created_date, closed_date, complaint_type, descriptor,
agency, status, borough, incident_zip, latitude, longitude.

Cleaned analytical table (Parquet, one row per unique_key,
data/processed/nyc311_clean, partitioned by year/month):

- unique_key: string, nonempty, unique
- created_at: timestamp, nullable (null + invalid_created if unparseable)
- closed_at: timestamp, nullable
- complaint_raw: string, original value retained
- complaint_type: string, trimmed; UNKNOWN when missing
- descriptor: string, nullable trimmed detail
- agency: string, nullable trimmed responder
- status_snapshot: string, nullable (point-in-time value, NOT history)
- borough_raw: string, original value retained
- borough: string, upper-cased; null + missing_borough when empty/Unspecified
- incident_zip_raw: string, original value retained
- incident_zip: string, 5-digit match else null + invalid_zip
- latitude/longitude: double, null + invalid_coords when missing or
  outside lat 40.49-40.92 / lon -74.26--73.70 (never repaired)
- resolution_hours: double, only for valid closed>=created; null +
  unresolved/negative_duration otherwise (never zero-filled)
- created_day: date; created_week: date (Monday); weekday: string;
  month/year/hour: integers (all null when created_at null)
- data_snapshot_date: date (pipeline run date, --snapshot-date override)
- data_quality_flags: array<string> (invalid_created, invalid_closed,
  negative_duration, unresolved, missing_created, invalid_coords,
  missing_borough, missing_complaint, invalid_zip)

Display labels may differ ("Problem" for complaint_type). Always inspect
live metadata before ingestion; fields may be missing or sparse.
