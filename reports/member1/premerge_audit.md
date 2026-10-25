# Member 1 pre-merge audit

Date: 2026-10-25. Branch: `feature/data-analytics`.
Starting commit: `17648bd`.
Status: PASS for implemented Member 1 pipelines. Full suite: 85 passed in
944.14 seconds; final unit rerun after the last plotting/configuration fixes:
55 passed in 10.73 seconds. Lint passed. The four added final unit cases were
run in that separate unit pass, not counted as part of the earlier 85-test run.

## What this change does

Member 1 downloads NYC 311 records, verifies coverage, cleans them with Spark,
and exports temporal, spatial and clustering analyses with figures. This audit
traced the implemented paths through real downloads, ETL, Parquet consumers,
analytics and the existing dashboard, then corrected the defects below.

Scope is the implemented Member 1 work through Phase 4. Forecasting, mining,
closure modeling, priority scoring and the later dashboard integration are not
claimed implemented by this audit. No merge into `main` was performed.

## Must fix - resolved

### 1. Quoted CSV fields could be read incorrectly

- What this is: the downloader uses Python's standard CSV quoting.
- Problem: Spark's reader did not support embedded newlines and used a different
  quote escape. A valid quoted field could split a record or retain doubled quotes.
- Fix: read multiline records and use doubled-quote escaping in `read_raw`.
- If skipped: later input can be rejected or text values silently changed. The
  regression includes both a newline and literal quotes in one field; it passes.

### 2. Invalid snapshot metadata could produce misleading provenance

- What this is: historical rows retain checkpoint acquisition dates.
- Problem: unvalidated timestamp strings could become null snapshots, while
  mismatched source/monthly metadata could be repeated in the quality report.
- Fix: validate UTC acquisition timestamps, source/date scope and reconciled
  month/total counts before Spark work. Capture the hash of the bytes actually read.
- If skipped: output can carry an incorrect snapshot or provenance declaration.
  Unit cases cover invalid dates, missing timezones, wrong sources and bad totals.

### 3. A failed historical write could destroy the last good export

- What this is: the history ETL replaces its processed Parquet output on reruns.
- Problem: direct overwrite removed the previous output before readback succeeded.
- Fix: stage and verify the new Parquet first. Check that the source manifest has
  not changed, then replace the previous directory with rollback on rename failure.
- If skipped: a failed rerun can leave consumers without their previously valid
  data. Injected write failure and manifest mutation preserve every old output hash.

### 4. Sparse geography and all-unresolved inputs could break exports

- What this is: clustering is optional when valid geographic cells are insufficient.
- Problem: Spark cannot infer all-null ablation or summary columns. The primary
  k sweep also lacked the size guard already present in the ablation sweep.
- Fix: explicit, compatible export schemas; nullable cluster assignments when
  unavailable; bounded k selection; null cluster duration when no resolutions exist.
- If skipped: valid small/filtered inputs can crash after earlier tables were
  written. CLI cases cover zero, one and two cells with only unresolved complaints.

### 5. An input override could inherit the sample's acquisition metadata

- What this is: the analytics CLI supports a custom cleaned Parquet input.
- Problem: it still read the sample manifest from the YAML config.
- Fix: custom input uses only an explicitly selected `--ingestion-manifest`.
  Missing explicit files fail; the exact loaded manifest hash is recorded.
- If skipped: a multi-month report can display a January sample filter. Real
  expanded and freshly downloaded runs now identify their actual manifests.

### 6. Genuine zero durations were not represented faithfully in box plots

- What this is: resolved complaints may legitimately have zero duration.
- Problem: a positive lower-whisker floor invented a nonzero boundary, and a pure
  log scale cannot display zero.
- Fix: bound whiskers by the observed minimum and use a scale that is linear near
  zero and logarithmic above 0.1 hours. The axis states this convention.
- If skipped: all-zero cohorts are misleading or invisible. The zero-only
  regression passes, and the real-data box plots were regenerated and inspected.

## Should fix - resolved

### 7. The KMeans colorbar displayed nonexistent fractional cluster IDs

- What this is: cluster IDs are category labels, not continuous measurements.
- Problem: a continuous ten-color legend accompanied a two-cluster result.
- Fix: discrete colors and ticks for the actual IDs, plus a categorical plotting test.
- If skipped: readers can infer intermediate clusters that do not exist. The
  corrected sample and expanded maps were regenerated and visually inspected.

### 8. Geographic settings and parts of the contract overstated guarantees

- What this is: the implementation uses an approximate local metric projection.
- Problem: unused EPSG settings implied UTM reprojection; docs called cell areas
  exact and declared the grid cluster column as bigint rather than its actual int.
- Fix: remove ignored EPSG settings, qualify area estimates, correct column types,
  null duration behavior and the explicit exclusion of invalid ZIPs from ZIP tables.
- If skipped: downstream code or academic interpretation can rely on guarantees
  the implementation does not provide. Existing k and summary export types remain
  compatible; no dashboard or teammate-owned algorithms were changed.

### 9. Nonfinite geographic parameters passed numeric guards

- What this is: YAML supports special floating-point values such as `.nan` and `.inf`.
- Problem: comparisons such as `value <= 0` do not reject NaN, allowing invalid
  grid, distance or projection parameters into the pipeline.
- Fix: require finite geographic parameters and DBSCAN sweep values before execution.
- If skipped: invalid configuration can produce meaningless spatial outputs or
  downstream failures. Explicit NaN/infinity cases are covered by unit tests.

## Executed real-data verification

| Input | Rows | Days | Valid coordinate rows | Grid cells | Tables | Figures |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Original January sample | 100,000 | 12 | 98,683 | 873 | 16 | 15 |
| December 2023-February 2024 history | 799,439 | 91 | 788,150 | 932 | 16 | 15 |
| Fresh 2024-02-29 download | 9,026 | 1 | 8,833 | 744 | 16 | 13 |

The history rerun verified 91 existing checkpoints against live source daily
counts. A separate complete leap-day acquisition used 5,000-row pages and
actually traversed two data pages. Its ETL output was isolated from the full
history. No synthetic fixture was used for these research counts.

An independent Python CSV stream counted dates, boroughs, categories, valid ZIPs
and spatial cells. All five comparisons matched the exported aggregates for each
of the three inputs. Parquet row counts matched all table manifests; cluster cell
and complaint totals reconciled. Every referenced figure file exists; all PNGs
passed image verification. Clustering maps and corrected box plots were also
visually inspected. Single-day KDE/violin plots were correctly omitted.

### Commands and evidence

Run from the project root with the documented Python 3.10 / PySpark 3.5.3 runtime:

```powershell
.\.venv\Scripts\python.exe scripts/download_history.py --config configs/history.yaml
.\.venv\Scripts\python.exe scripts/run_history_etl.py --config configs/history.yaml
.\.venv\Scripts\python.exe scripts/run_member1_analytics.py --config configs/sample.yaml --outdir artifacts/member1/merge_audit/sample_analytics
.\.venv\Scripts\python.exe scripts/run_member1_analytics.py --config configs/sample.yaml --input data/processed/member1_history --ingestion-manifest data/raw/member1_history/manifest.json --outdir artifacts/member1/merge_audit/history_analytics
.\.venv\Scripts\python.exe -m pytest -q --basetemp artifacts/member1/merge_audit/full_tmp
.\.venv\Scripts\python.exe -m ruff check src scripts tests
```

The fresh acquisition uses an ignored audit YAML copied from `configs/history.yaml`
with start `2024-02-29`, exclusive end `2024-03-01`, page size 5,000, max rows 20,000
and `out_dir: data/raw/member1_audit_day`. Its `process_history` call uses a separate
audit root, then the normal analytics CLI consumes the resulting Parquet and its
explicit manifest. These orchestration commands and independent CSV checks are
saved under `artifacts/member1/merge_audit/`; production logic remains in `src/`.

Saved local evidence, intentionally ignored by Git:

- `sample_analytics.log`, `history_download.log`, `history_etl_final.log`,
  `history_analytics.log`, `live_day_download.log`, `live_day_etl.log`,
  `live_day_analytics.log`: actual pipeline output.
- `sample_analytics/`, `history_analytics/`, `live_day_analytics/`: complete
  manifests, metrics, Parquet tables and figures.
- `independent_reconciliation.json` and its script/log: all three source checks.
- `regressions.log`: initial 1 failed/4 passed run exposed the CSV quote mismatch.
  `csv_regression.log`: corrected quoted-field test passed.
  `two_cells.log`: complete two-cell/all-unresolved CLI test passed.
- `final_unit.log`: 30 unit tests passed in 13.29 seconds.
  `zero_boxplot.log`: additional zero-duration regression passed in 2.77 seconds.
- `pytest.log`, `ruff.log`: full regression and lint evidence.
- `all_final_unit_native.log`: final 55-test unit pass on the final code.
- `dashboard.json`, `dashboard.log`: existing dashboard smoke result.

## Remaining limits

- The existing Phase 1 dashboard rendered three charts with zero exceptions using
  installed Python 3.13, Streamlit 1.48.0 and PyArrow 21.0.0. This is a smoke test
  of the existing app, not the later Member 3 integration or the pinned Streamlit
  1.65.0 environment. Installing the pinned dependencies timed out on the large
  PyArrow download; `environment.log` records the failure. Spark verification used
  Python 3.10.0 and PySpark 3.5.3 throughout.
- Source count checks do not detect equal-count substitutions or historical
  field updates. Resumed checkpoints retain their own acquisition times. A
  transactional source snapshot is not claimed.
- Filesystem staging protects ordinary write/readback failures; it is not a
  database transaction or a guarantee across abrupt power loss during publication.
  Run only one ETL writer per output directory.
- One sandboxed unit run failed at checkpoint replacement with Windows error 5
  (54 passed, 1 failed). The native rerun passed all 55 tests; the full native
  suite also passed. The isolated denial was not reproduced outside the sandbox.
  Both logs remain saved; no permanent filesystem-access guarantee is claimed.
- The expanded window is three winter months, not all of 2022-2025. Annual
  seasonality, staffing optimization and forecasting accuracy are not established.
- Later-phase CLI stubs remain intentional; this is not a completed forecasting,
  mining, prioritization or final release audit.

Verdict: Ship the implemented Member 1 work after reviewing these scope and
environment limits. No unresolved Member 1 code blocker was found in the tested
paths. The fetched `origin/main` is an ancestor of the feature branch; no main
merge was performed. No raw data, processed data, artifacts or models are
included in the code changes.
