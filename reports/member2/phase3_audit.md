# Member 2 Phase 3 audit: quality flags, closure states, backlog

Input: 100,000-record cleaned sample (`data/processed/nyc311_clean`),
created_day 2024-01-01 to 2024-01-12, snapshot 2026-10-25.
Command (run 2026-10-25, ~26 s Spark wall time, local[2]):

```powershell
.\.venv\Scripts\python.exe scripts/run_member2_audit.py --config configs/sample.yaml `
  --input <main checkout>/data/processed/nyc311_clean `
  --member1-tables <main checkout>/artifacts/member1/analytics/tables
```

Outputs (gitignored): `artifacts/member2/audit/audit.json` and
`artifacts/member2/audit/tables/{flag_per_flag, flag_combinations,
flag_pairwise, status_closure, duration_classes, backlog_by_borough,
label_summary}` (+ `.meta.json`). 100,000 distinct `unique_key`, 0 duplicates.

## 1. Flag reconciliation

97,285 records carry no flag; 2,715 carry at least one. Per-flag counts sum
to 3,614 because 826 records carry 2-4 flags (excess over 2,715 = 899 flag
occurrences).

| Flag | Records |
| :--- | ---: |
| invalid_coords | 1,317 |
| unresolved | 1,104 |
| invalid_zip | 1,094 |
| missing_borough | 80 |
| negative_duration | 19 |

Records by number of flags: 1 flag 1,889; 2 flags 755; 3 flags 69; 4 flags 2.

Exclusive combinations (each flagged record counted once; sums to 2,715):

| Combination | Records |
| :--- | ---: |
| unresolved | 861 |
| invalid_coords | 708 |
| invalid_coords + invalid_zip | 506 |
| invalid_zip | 301 |
| invalid_zip + unresolved | 212 |
| invalid_coords + invalid_zip + missing_borough | 65 |
| invalid_coords + unresolved | 26 |
| negative_duration | 17 |
| invalid_coords + missing_borough | 6 |
| invalid_zip + missing_borough | 4 |
| invalid_coords + invalid_zip + missing_borough + unresolved | 2 |
| invalid_coords + invalid_zip + negative_duration | 2 |
| invalid_coords + invalid_zip + unresolved | 2 |
| missing_borough | 2 |
| missing_borough + unresolved | 1 |

Checks (all true): flagged + clean = 100,000; sum over combinations of
(size x records) = 3,614 = sum of per-flag counts. The pairwise overlap
derived from these combinations equals Member 1's
`artifacts/member1/analytics/tables/flag_overlap` row for row (e.g.
invalid_coords & invalid_zip 577, invalid_zip & unresolved 216).

## 2. Closure states at the snapshot

| status_snapshot | has closed_at | Records |
| :--- | :--- | ---: |
| Closed | yes | 98,830 |
| Closed | no | 461 |
| In Progress | no | 494 |
| Open | no / yes | 52 / 11 |
| Assigned | no / yes | 41 / 32 |
| Pending | no / yes | 37 / 19 |
| Started | no | 19 |
| Unspecified | yes | 4 |

- Backlog (no `closed_at` at snapshot): 1,104 records = the `unresolved`
  flag count. 461 of them are `Closed` without a closure timestamp; they are
  labelled "not closed within 7 days" (no verifiable closure time).
- 66 records have a `closed_at` while their status is not `Closed`
  (Assigned 32, Pending 19, Open 11, Unspecified 4): status is a point-in-time
  value, not history.
- No `closed_at` is after the snapshot date (latest 2026-10-16).

Backlog by borough (records without `closed_at` / total): BRONX 78/20,995
(0.37%), BROOKLYN 262/32,470 (0.81%), MANHATTAN 426/20,092 (2.12%), QUEENS
291/22,836 (1.27%), STATEN ISLAND 44/3,527 (1.25%), unknown borough 3/80.

## 3. Resolution duration

Duration classes over all 100,000 records: no closed_at 1,104; negative 19;
exactly zero 1,335; (0, 1 h] 22,564; (1 h, 24 h] 32,013; (24 h, 7 d] 22,927;
(7 d, 30 d] 10,306; > 30 d 9,732.

Cleaned `resolution_hours` (98,877 valid closures; exact interpolated
quantiles): p25 1.07 h, median 15.61 h, p75 88.79 h, p90 699.51 h,
p99 6,977.79 h, mean 340.43 h, max 24,251.74 h. The distribution is extremely
right-skewed; means are not representative.

## 4. 7-day closure label

Eligible 99,981 (all records are mature at the 2026-10-25 snapshot),
excluded negative duration 19, immature 0, missing created 0.
Positives 78,839, i.e. 78.85% of eligible records closed within 7 days.

## 5. Spark vs pandas consistency

On a deterministic bounded sample (19,684 rows with
`pmod(xxhash64(unique_key), 5) = 0`), Spark and the independent pandas
implementations agree exactly on per-flag counts, combinations, pairwise
overlap, status x closure states, duration classes, backlog by borough,
label summary and the borough x complaint_type `resolution_summary`;
quantiles agree within 1e-6 relative tolerance. The same equalities are
asserted on hand-built synthetic edge cases in
`tests/integration/test_member2_spark.py`.
