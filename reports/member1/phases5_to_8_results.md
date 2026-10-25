# Member 1 results: Phases 5-8

Date: 2026-10-25. Branch: `feature/data-analytics`.

Implemented scope: demand forecasting, forecast/geospatial handoff, local systems benchmarking, and the final Member 1 audit. Full-suite verification and the measured pipeline/artifact audits below passed.

## Data and chronological protocol

Real NYC 311 source: `erm2-nwe9`, 799,439 records over 91 verified days, 2023-12-01 through 2024-02-29. Acquisition snapshot: 2026-10-25. Counts refer to reported complaints, not population-adjusted need or infrastructure quality.

The verified calendar contains six geography groups (five boroughs plus UNKNOWN). Only absent borough counts on source-verified complete days become zeros. Missing/unverified dates fail. There is a 28-day warmup. Features use origin-day counts, past lags, trailing means, observable weekly baselines, horizon and target weekday. Outcomes and snapshot status are excluded.

The forecast origin means the end of a NYC wall-calendar day. Validation training labels stop on 2024-01-31; origins are Jan 31-Feb 7 and targets Feb 1-14. Test models refit only through 2024-02-14; origins are Feb 14-22 and targets Feb 15-29. All seven horizons are evaluated at each origin, so every fit cutoff is no later than its origin. Origins update available observations without refitting the block model. Overlapping targets create correlated errors; no confidence interval or statistical significance is claimed.

## Phase 5: genuine Spark MLlib experiments

StringIndexer, OneHotEncoder and VectorAssembler fit inside training-only Spark pipelines. Linear regression is compared with/without explicit StandardScaler; internal LR standardization is disabled so this is a real scaling ablation. RandomForest uses 32 trees/depth 5; GBT uses 30 iterations/depth 3. Seed is 42. All predictions are nonnegative-clipped without integer rounding; raw predictions remain available.

| Model | Validation MAE | Test MAE | Test RMSE | Test WAPE ratio |
| --- | ---: | ---: | ---: | ---: |
| seasonal_naive | 107.9345 | 137.5212 | 191.3571 | 0.101573 |
| weekday_mean | 157.2329 | 111.4008 | 175.8167 | 0.082280 |
| lr_scaled | 116.0452 | 167.0205 | 232.9072 | 0.123361 |
| lr_unscaled | 125.3931 | 156.7350 | 219.5643 | 0.115764 |
| random_forest | 204.7487 | 129.4891 | 195.7553 | 0.095640 |
| gbt | 215.7935 | 154.3003 | 227.7098 | 0.113966 |

MAE/RMSE are complaint counts per borough-day forecast row, pooled over horizons 1-7 and six geographies. WAPE is sum absolute error / sum actual count over those forecast rows; it is null for a zero denominator. Each candidate has 336 validation and 378 test forecast rows.

**Selection:** `seasonal_naive` wins validation MAE. `weekday_mean` performs better on the test block, but the test ranking does not change the chosen model. This winner reversal and the MLlib models' mixed results are evidence of limited generalization, not grounds to claim superior ML accuracy.

Explicit LR scaling improves validation error here but worsens test error. The feature summaries are correlated; Spark logged a Cholesky-to-Quasi-Newton fallback for LR. Models completed and all exported predictions passed finite-value checks. No claim is made that one preprocessing choice is universally best.

The final refit cutoff is 2024-02-29 with 2,478 supervised rows. `lr_scaled` is the best validation MLlib comparator and is retained as an actual Spark PipelineModel, even though the selected operational artifact uses a baseline. The audit reloaded its StringIndexerModel, OneHotEncoderModel, VectorAssembler, StandardScalerModel and LinearRegressionModel and transformed all 42 future rows.

Two real executions produced identical aggregate MAE/RMSE/WAPE values (maximum absolute difference 0.0); their fit times differ and are measured separately.

| MLlib model | Validation fit seconds | Test refit seconds |
| --- | ---: | ---: |
| lr_scaled | 12.445 | 1.814 |
| lr_unscaled | 1.528 | 1.243 |
| random_forest | 5.129 | 1.924 |
| gbt | 12.770 | 10.225 |

Fit times include the fitted preprocessing pipeline and estimator; they exclude prediction/export, Spark startup and the separate final refit. Baselines have no fitted estimator. Per-borough, per-horizon and per-origin errors are saved alongside the overall table.

## Phase 6: validated handoff for Member 3

The selected historical-origin forecast covers March 1-7, 2024 from origin February 29, 2024. It is not a forecast for today. It contains 42 rows (six geographies x seven horizons), with null actual counts. Its seven-day citywide sum is 58,260 reported complaints under the selected weekly-repeat assumption.

The six-row priority-feature handoff contains predicted/trailing seven-day totals, absolute/percent change, zero-baseline handling, valid-coordinate counts, active grid cells, counts per active cell, geographic observation dates, acquisition snapshot, model version and an explicit uncalibrated-uncertainty marker. A genuine zero denominator yields null percent change. This baseline repeats the previous week, so all observed handoff changes are zero by construction; that is not proof of stable future demand.

Source/forecast populations, horizons, missing values, dates and geography joins are checked. UNKNOWN remains unassigned; ALL means borough totals, not complaint-category forecasts. Active-cell counts can overlap across borough assignments and must not be summed as a unique citywide grid count. No priority scores, weights, ranks or staffing optima are implemented. Contract: `docs/member1/demand_contract.md`; measured example: `tests/fixtures/member1/demand_handoff.json`.

## Phase 7: equivalent local benchmarks

Both engines read the same four selected columns from the same real chronological CSV prefixes, filter the same valid-coordinate box, normalize text/line endings, group by borough/category, sort, and write one CSV result. Every timed output reconciled exactly. Input construction, session startup and result verification are outside the timed region. One warmup precedes three measured repetitions per setting. OS file cache is uncontrolled; these are warm runs. No other experiment or Spark test job ran during measurement.

| Records | Pandas median seconds | Pandas min-max | Spark median seconds (2 partitions) | Spark min-max |
| ---: | ---: | ---: | ---: | ---: |
| 100,000 | 0.7397 | 0.6902-0.7895 | 1.9732 | 1.9112-2.7747 |
| 300,000 | 2.1004 | 2.0593-2.1236 | 2.6579 | 2.5750-2.6999 |
| 799,439 | 5.8992 | 5.5349-5.9003 | 5.2262 | 5.1282-5.2511 |

Pandas is faster at the two smaller measured sizes; Spark is slightly faster at the largest size for this workload. Three points do not establish a universal crossover threshold. The data is genuinely real and smaller than one million rows; no synthetic duplication was used to claim larger scale.

| Spark partitions, largest input | Median seconds | Min-max seconds |
| ---: | ---: | ---: |
| 1 | 5.2955 | 5.2352-5.7788 |
| 2 | 5.2262 | 5.1282-5.2511 |
| 4 | 5.3729 | 5.2782-5.4737 |

The partition differences are small relative to the observed ranges; no statistically optimal partition count is claimed. Multiline CSV is one unsplittable input file per scale: repartitioning happens after reading. These measurements do not predict partitioned-Parquet, multi-node or cold-cache performance.

Hardware/runtime: Windows-10-10.0.26300-SP0; Intel64 Family 6 Model 186 Stepping 2, GenuineIntel; 16 logical CPUs; Python 3.10.0, Spark 3.5.3, local[2]. Pandas uses its normal runtime settings; CPU affinity is not controlled. Comparable Pandas/native/JVM peak RSS was not measured, so no memory-efficiency ranking is made. There are 24 timed trials; all individual times, throughput, standard deviations and exact input SHA256 values are in benchmark.json.

## Phase 8: final audit and reproducible delivery

The real-data audit passed: 4,284 held-out rows checked independently against verified daily counts and observable baselines; all 4 metric tables reconciled including group completeness; 42 forecast rows and 6 handoff rows checked; the retained MLlib artifact reloaded; all 24 benchmark trials and saved result CSVs verified. Source, producer and benchmark-input hashes were checked.

Added unit/integration coverage checks incomplete calendars, illegal split dates, future perturbations, observable weekly baselines, zero-denominator WAPE, nonfinite forecasts, handoff counts/dates, quoted CSV parity, exact real-data prefix sizes and independent audit arithmetic. The initial benchmark regression exposed CRLF/LF differences; both engines now explicitly normalize line endings and write LF. The corrected integration case passed.

**Verification:** full regression suite: 100 passed, 1 upstream PySpark distutils deprecation warning in 1056.32 seconds. A seed-change rejection case added after full-suite collection was included in a separate focused run: 7 passed, 1 upstream warning in 3.19 seconds. Ruff and Git whitespace checks passed. Logs: pytest_full.log, final_units.log and ruff.log under artifacts/member1/demand.

## Follow-up bug audit (2026-10-25)

Reviewed the ingestion/ETL, EDA/geospatial/clustering, forecasting, handoff,
benchmark and dashboard entry points for a single local operator using the
existing bounded data. Confirmed fixes:

1. A failed sample API page previously published a partial/empty replacement and
   returned success. It now raises before changing the published sample or manifest.
   Regression cases cover failure on the first page and after a successful page.
2. The final audit could accept missing model/horizon/trial groups, unchecked future
   baseline values, or absent retained MLlib models. It now requires the complete
   expected output sets and independently checks future baseline counts. It also
   clears an old PASS file before starting, so a failed rerun cannot leave stale success.
3. SQL null comparisons let missing forecast origins, target dates and model
   versions through handoff validation. Required fields now have explicit null checks.
4. ETL evaluated missing-complaint flags after replacing blanks with UNKNOWN.
   Flags now evaluate on the source columns before normalization.
5. A Spark benchmark directory without part files was read as an empty result.
   Missing parts now fail, and audited output totals must match each measured trial.
6. Weekly EDA called a fixed-frequency rounding operation before its weekly branch
   and crashed. Calendar-week grouping now runs directly; a Sunday/Monday boundary
   regression preserves both weeks and their counts.
7. YAML's valid unquoted date values crashed the demand config loader, and accepted
   numeric-string seeds were passed through unchanged. Dates and seeds now normalize
   before reaching the training code; missing split keys are validated explicitly.
8. Spark's CSV reader silently assigned reordered header columns to the fixed
   schema by position. It now validates the header against the canonical ingestion
   schema and rejects mismatches; quoted multiline fields remain supported.
9. The dashboard cached its small CSV tables by path forever, so regenerated
   files stayed stale on rerun. Removing that cache makes each rerun read the
   current aggregates. An AppTest regression rewrites a category and verifies
   that the displayed input updates without exceptions.

Saved pre-fix reproductions: repro_units.log (3 failed), repro_spark.log (2 failed),
repro_config_weekly.log (2 failed), and header_repro.log (1 failed), under
artifacts/member1/bugfix_audit.
Full regression suite: 109 passed, 1 upstream PySpark deprecation warning in
910.88 seconds. The late CSV-header check and repaired ETL/handoff/CSV parity
cases passed in a separate 5-test Spark run (84.11 seconds). The two late
weekly/config cases were included in a separate 72-test unit run; these runs
overlap and must not be added together. UI refresh verification uses the installed
Python 3.13 / Streamlit 1.48 UI runtime only; Spark remains on Python 3.10.
The pinned UI environment remains unverified. Lint and whitespace checks passed.
The standalone AppTest refresh check passed (Noise changed to Road, no exceptions).
Pytest AppTest attempts under output capture timed out on this host; the uncaptured
check passed. No capture-dependent UI test is included. A later unit rerun hit an
intermittent Windows access-denied error replacing a manifest under OneDrive;
repeating the suite in a fresh temporary directory outside OneDrive passed all
72 cases in 37.34 seconds. Logs preserve the failure and the successful rerun.
Unrelated edits made during the paused turn remain outside this audit commit.
The staged demand producer SHA256 matches the producer recorded by the successful
real-data audit; later local forecasting edits require their own rerun before merging.

The refreshed real-data model/handoff/artifact audit passed: 4,284 held-out rows,
42 future rows, 6 handoff rows, 4 metric tables, the retained MLlib model and all
24 saved benchmark trials. All 12 model/split metric groups match the preceding
run exactly (maximum absolute MAE/RMSE/WAPE difference 0.0). A source scan measured
zero missing complaint types in both the 799,439-row history and 100,000-row sample,
so the flag repair does not invalidate their existing quality counts.

These are correctness reruns alongside regression tests, not new performance
measurements. The isolated benchmark and original fit timings above remain the
recorded performance evidence. Logs, source-impact counts and model comparison
evidence are saved in artifacts/member1/bugfix_audit; generated data/models remain ignored.

## Reproduce

Prerequisites: the documented Python 3.10 / Spark 3.5.3 / Java 17 / Windows Hadoop setup and verified Phase 4 history. Raw/processed files and generated artifacts/models are ignored, so a fresh clone must run the acquisition and ETL first.

```powershell
.\.venv\Scripts\python.exe scripts/download_history.py --config configs/history.yaml
.\.venv\Scripts\python.exe scripts/run_history_etl.py --config configs/history.yaml
.\.venv\Scripts\python.exe scripts/build_features.py --config configs/demand.yaml
.\.venv\Scripts\python.exe scripts/train_models.py --config configs/demand.yaml
.\.venv\Scripts\python.exe scripts/build_priority_features.py --config configs/demand.yaml
.\.venv\Scripts\python.exe scripts/benchmark.py --config configs/benchmark.yaml
.\.venv\Scripts\python.exe scripts/audit_member1.py --config configs/demand.yaml --benchmark-config configs/benchmark.yaml
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m ruff check src scripts tests
```

Do not run other Spark/model jobs during the timed benchmark. On this managed host, Spark/Java require approved execution outside the sandbox to create gateway files and local worker sockets; pytest also disables unrelated system plugins with PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 and uses an ignored workspace basetemp.

Saved evidence is under artifacts/member1/demand and artifacts/member1/benchmark: feature/model/handoff manifests, Parquet daily/features/predictions/future/metric tables, saved models, final_audit.json, repeatability.json, raw benchmark trials/input hashes, run logs, and four report PNGs (model_comparison, forecast_7d, runtime_by_scale, partition_sensitivity). Small consumer fixtures are committed; full artifacts are not.

## Interpretation limits and ownership

- Three winter months are enough for this engineering backtest, not annual or multi-season accuracy claims. The full 2022-2025 archive was not acquired.
- Day-end availability is an assumption for retrospective counts; a 2026 acquisition cannot establish historical ingestion timing or remove source backfill bias. Snapshot dates and event/forecast dates remain separate.
- Forecast intervals are not calibrated. Predictions describe reported complaints, not population need, fairness, hazards or optimal staffing.
- The earlier pinned-dashboard dependency download remains unverified in this environment. The existing dashboard smoke result is documented in the prior pre-merge audit; no Member 3 dashboard integration is claimed here.
- Member 2 mining/closure models and Member 3 prioritization/UI/deployment remain their work. No merge into main or final release approval is claimed.
