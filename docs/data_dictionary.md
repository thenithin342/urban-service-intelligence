# Data dictionary (erm2-nwe9 subset used)

Verified against live metadata snapshot in data/manifests/ (see
reports/data_quality.md for retrieval date).

- unique_key: string, request id (dedupe key)
- created_date: timestamp (required for analysis; nulls tracked)
- closed_date: timestamp, nullable
- complaint_type: string, nullable -> UNKNOWN bucket for aggregation
- descriptor: string, nullable detail
- agency: string, nullable responder
- status: string, nullable snapshot value (do NOT use as historical state)
- borough: string -> normalized to BRONX/BROOKLYN/MANHATTAN/QUEENS/
  STATEN ISLAND/UNKNOWN (raw retained)
- incident_zip: string, validated/nullable (Phase 2)
- latitude/longitude: doubles, nullable; flagged outside
  lat 40.49-40.92 / lon -74.26--73.70 (not repaired)

Display labels may differ ("Problem" for complaint_type). Always inspect
live metadata before ingestion; fields may be missing or sparse.
