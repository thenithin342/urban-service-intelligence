# Engineering audit - post-Phase 2 (2026-10-14, all numbers measured)

Scope: full read of src/, scripts/, tests/, dashboard/, configs/;
ruff clean; full suite 33 passed in 238s under .venv
(Python 3.10.0 + PySpark 3.5.3).

## 1. Flag reconciliation (requested check)

Question: 2,715 flagged rows vs 1,104 + 19 + 1,317 = 2,440 for the
three named categories. Answer: the three categories were never the
full picture. Measured per-flag occurrence counts from
data/processed/nyc311_clean:

- negative_duration 19, unresolved 1104, invalid_coords 1317,
  missing_borough 80, invalid_zip 1094 (others 0).
- Total occurrences: 19+1104+1317+80+1094 = 3614.
- Flag-count histogram: 0 flags 97285 rows; 1 flag 1889 rows;
  2 flags 755 rows; 3 flags 69 rows; 4 flags 2 rows.
- Check: 1889+755+69+2 = 2715 flagged rows. And
  1889*1+755*2+69*3+2*4 = 3614 occurrences. Reconciles exactly.
- Measured pairwise overlaps (nonzero): invalid_coords&invalid_zip 577,
  unresolved&invalid_zip 216, invalid_coords&missing_borough 73,
  missing_borough&invalid_zip 71, unresolved&invalid_coords 30,
  unresolved&missing_borough 3, negative_duration&invalid_coords 2,
  negative_duration&invalid_zip 2. Pair-sum 974 =
  755*1+69*3+2*6, consistent with the histogram.
- Interpretation: 826 of 2715 flagged rows (30%) carry 2+ flags,
  dominated by geo+zip co-missingness (577 rows) and open complaints
  missing ZIP (216 rows). No unexplained residue remains.

## 2. Row-level idempotency (requested check)

Method: snapshot A = schema + SHA256 of every row (ordered by
unique_key, via toJSON) of data/processed/nyc311_clean; full ETL
rerun; snapshot B; compare.
Result: schema identical, 100000 vs 100000 rows, all 100000 row
hashes identical, 0 differing rows. Stronger than metric equality:
content-level determinism confirmed (same-day snapshot date held
constant by design; --snapshot-date override exists for pinning).

## 3. Bugs found and fixed in this audit

1. CLEAN_COLUMNS omitted complaint_raw/borough_raw/incident_zip_raw:
   null report now covers all 22 data columns (raw zip nulls: 1094).
2. Dedupe tie order nondeterministic on equal created_at: window now
   orders by created_at + all raw string columns (total order).
   New test_dedupe_tie_deterministic passes.
3. dedupe_records collapsed distinct keyless rows into "duplicates":
   blank keys now pass through (ETL still excludes+counts them).
4. fetch_page retried non-retryable 4xx then raised RuntimeError:
   4xx (non-429) now fail fast as HTTPError; transport errors and
   5xx/429 still retry. New tests: 404 single-call, ConnectionError
   retry. (One self-caught slip during the fix: transport errors
   briefly re-raised; corrected before commit-worthy state.)
5. run_etl.py accepted any --snapshot-date string (bad values became
   silent nulls): now validated as YYYY-MM-DD via argparse error.
6. Default config still had 30s timeout / 5 retries with 50k pages:
   aligned to 180s / 6 like sample.yaml.
7. README overclaimed `streamlit run` as verified: dashboard now
   actually smoke-tested headless (HTTP 200, 6463 bytes, no
   exceptions); README states the exact verification level.
8. README venv path was machine-specific: generic `py -V:3.10` now.
9. 13 ruff findings fixed across earlier work; `ruff check` clean.
   (Deliberately NOT applied: UP017 datetime.UTC, needs Python 3.11+.)

## 4. Deferred (not bugs, no output change warranted now)

- Missing-vs-malformed distinction inside invalid_zip/invalid_coords:
  precision improvement for Phase 3 geo joins; would churn the
  reconciled breakdown above, so parked until Phase 3 needs it.
- Phase 1 pandas null inflation (default NA list) stays documented in
  decisions.md; Phase 1 artifacts untouched.
- test file repair note: an edit briefly merged two test defs; caught
  by reading the file and repaired before the suite ran.

## 5. Residual risks (unchanged)

Single-machine Spark; Jan 1-12 sample coverage; external winutils;
3.13 worker-incompatible; status_snapshot is point-in-time.
