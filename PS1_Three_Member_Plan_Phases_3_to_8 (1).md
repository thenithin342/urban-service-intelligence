# Urban Service Intelligence
## Three-Member Execution Plan: Phases 3-8

Project: Urban Service Demand Prediction and Intelligent Resource Allocation
Status: Phases 0-2 reported complete; Phase 3 is next.
Team: 3 members (Member A, Member B, Member C)
Schedule: 6 working weeks for Phases 3-8, adjustable to the course deadline.

## 1. The goal

Deliver a reproducible big-data application that (1) ingests and processes NYC 311
complaints with PySpark; (2) describes temporal and geographic complaint patterns;
(3) mines frequent patterns; (4) forecasts complaint demand and estimates closure
outcomes; (5) ranks potential operational priorities; and (6) demonstrates the
results in a tested Streamlit dashboard with system benchmarks.

Existing measured baseline (from the Phase 2 team report):
- 100,000 records in and out of the ETL, 0 excluded and 0 duplicates.
- 97,285 unflagged records; 2,715 records with one or more quality flags.
- 27 tests passed on the verified Python 3.10 / PySpark 3.5.3 environment.
- Current sample spans only January 1-12, 2024, and is NOT suitable for
  seasonal or long-horizon forecasting conclusions.
These values are reported by the team, not independently re-executed in this plan.

## 2. Permanent ownership: one accountable person per workstream

### Member A - Data Platform, Geospatial Analytics, Demand Forecasting

Owns:
- Historical data expansion with year/month coverage and verifiable manifests.
- Existing ingestion and processing modules; PySpark transformations.
- Spatial, temporal, and category aggregates; hot-spot analysis.
- Complaint demand forecasting, including baselines and leakage-safe splits.
- Spark processing performance experiments and data-methodology documentation.

Primary code ownership:
- src/ingestion/, src/processing/, src/analytics/
- src/modeling/demand.py (or equivalent demand-only module)
- scripts for downloads, ETL, and analytics aggregations
- tests for data quality, geospatial outputs, and demand forecasts

Must deliver:
- Versioned/manifested cleaned multi-month data and analytical Parquet outputs.
- Verified trend and hotspot metrics.
- Seven-day complaint demand forecasts with time-based evaluation.
- Reproducible Spark benchmarks and data pipeline documentation.

### Member B - Pattern Mining, Resolution Modeling, Model Evaluation

Owns:
- FP-Growth transaction design and association rules.
- Support/confidence/lift experiments and qualitative rule evaluation.
- Historical resolution/backlog analysis.
- Closure-within-7-days prediction or an appropriately justified alternative.
- Predictive evaluation, leakage checks, and model experiment registry.

Primary code ownership:
- src/mining/
- src/modeling/resolution.py (or equivalent resolution-only module)
- src/evaluation/model_metrics.py and mining evaluation code
- mining/resolution notebooks, associated tests, methodology notes

Must deliver:
- Transaction construction with explicit location-time basket semantics.
- Frequent itemsets and association rules, with tested quality thresholds.
- Valid resolution statistics and closure prediction with chronological splits.
- Explainable model comparisons, leakage/censoring review, and model artifacts.

### Member C - Product Integration, Priority Engine, Dashboard, QA

Owns:
- Streamlit application UX, filters, maps, result panels, and caching.
- Resource prioritization (transparent score plus historical backtest).
- Stable analytics/model output adapters; app integration tests.
- CI/lint/test gate, packaging, end-to-end run instructions, and final demo.
- Benchmark presentation (Member A runs and validates the Spark benchmarks).

Primary code ownership:
- dashboard/
- src/prioritization/
- dashboard data adapters and UI integration tests
- CI/workflow config, integration scripts, final demo material

Must deliver:
- Interactive dashboard for categories, trends, hotspots, patterns, forecasts,
  resolution analysis, and priority recommendations.
- Documented, configurable priority scoring formula and backtest.
- Reproducible end-to-end run, quality gate, final demo and release checklist.

## 3. Shared interfaces - agree before parallel development

All three members must approve the following contracts on Day 1:

### Raw and cleaned records
- Keep raw downloads unchanged; manifests identify source and time coverage.
- Clean records remain partitioned Parquet with stable column names/types.
- `unique_key` is the stable identifier; no duplicates after ETL.
- Missing/invalid coordinates are null + flagged, not silently imputed.
- Negative resolution durations are null + flagged; true zero durations stay 0.
- `closed_date` is absent as a feature at complaint creation time.
- Every analytical output records its coverage dates and filters.

### Proposed analytics output contracts (confirm against existing layout)
- data/analytics/daily_counts/ : date, borough, complaint_type, count
- data/analytics/geo_hotspots/ : spatial_id, geography, count, coverage
- data/analytics/resolution/ : group keys, eligible count, closure metrics
- data/analytics/pattern_rules/ : antecedent, consequent, support,
  confidence, lift, transaction definition, coverage
- data/predictions/demand/ : forecast_date, borough, complaint_type,
  predicted_count, evaluation window, model_version
- data/predictions/closure/ : complaint identifier or aggregate cohort,
  prediction_time, closure_probability, model_version
- data/priorities/ : location, category, normalized components,
  priority_score, rank, coverage, scoring_version

The exact paths are proposals. Adapt to the current repository and freeze one
schema in docs/data_contracts.md. The dashboard must consume aggregates or
small bounded outputs, never all raw rows into memory.

## 4. Phase-by-phase parallel schedule

### Week 1 - Phase 3: Exploratory and Geospatial Analytics

Member A:
- Read and validate Phase 2 Parquet and quality flags.
- Build deterministic Spark category, borough, agency, daily and hourly summaries.
- Build valid-coordinate-only ZIP/grid hot-spot aggregates; preserve ZIP as text.
- Write analytics output schemas, coverage metadata and tests.
- Propose a month-stratified data acquisition strategy.

Member B:
- Independently audit resolution duration, closure states and backlog metrics.
- Reconcile all quality flags including overlapping flagged conditions.
- Specify transaction semantics for Phase 4 (e.g., ZIP + seven-day windows).
- Define closure label, as-of timestamp and censoring policy for Phase 6.
- Add Spark/Pandas consistency checks on bounded sample data.

Member C:
- Build Streamlit Overview, Trends, Hotspots, and Resolution pages.
- Add robust filters, empty states, data coverage notes and cached Parquet readers.
- Smoke-test actual Streamlit launch and interactive navigation.
- Create an integration contract test for aggregate table schemas.

Phase 3 gate:
- Aggregates reconcile with cleaned data; output is deterministic.
- Invalid coordinates do not appear in geospatial maps.
- 2,715 flagged records are reconciled by mutually overlapping flags.
- Dashboard launches and displays validated outputs, explicitly labeled as
  January 1-12 sample analyses, not seasonal conclusions.

### Week 2 - Phase 4: Frequent Pattern Mining

Member A:
- Execute bounded, stratified data expansion across months and years.
- Prevent time sampling bias (do not treat unique_key ordering as random time).
- Run the ETL on expanded data and publish coverage manifests/quality checks.
- Maintain small repeatable fixtures for other members.

Member B:
- Construct precise transactions (e.g., unique complaint categories observed
  within a ZIP and calendar week), avoiding duplicate items in a basket.
- Implement Spark MLlib FP-Growth and association rules.
- Compare thresholds; report support, confidence, lift and basket counts.
- Distinguish correlations/associations from causal relationships.
- Add synthetic-basket and real-data regression tests.

Member C:
- Integrate a Pattern Explorer with filterable rule table and coverage notes.
- Keep UI stable through mocked contract fixtures while mining runs.
- Add CI checks and an end-to-end export/import smoke test.

Phase 4 gate:
- At least one verified threshold experiment and a reproducible rules artifact.
- Rule semantics, basket count and applicability are documented.
- UI reflects exact model output, not invented example metrics.

### Week 3 - Phase 5: Demand Forecasting and Resolution Baselines

Member A:
- Build daily counts by borough and complaint type from multi-month data.
- Train seasonal naive and rolling-average forecasting baselines.
- Train a candidate regression model with lag/rolling/calendar features.
- Evaluate only with chronological splits and report MAE, RMSE and WAPE
  (protect WAPE against zero denominators).
- Save forecasts, artifacts and training cutoff metadata.

Member B:
- Build closure outcome baseline(s) and valid as-of features.
- Define the 7-day closure target on cohorts with at least 7 days of
  observation after their creation time; censor newer unknown outcomes.
- Model precision/recall, PR-AUC, calibration and appropriate baselines.
- Audit leakage: do not use final status, closed_date or future complaint
  information as features available at prediction time.

Member C:
- Build forecast and resolution dashboard panels using contract fixtures.
- Load validated model outputs once delivered; show forecast horizon,
  historical coverage, and evaluation metrics.
- Add automated checks for missing/old model outputs.

Phase 5 gate:
- Forecast outputs were trained using adequate multi-month historical data.
- Both predictive tasks have honest chronological holdouts and baselines.
- Model predictions are reproducible and labeled with model versions.

### Week 4 - Phase 6: Resource Prioritization and Backtesting

Member A:
- Supply location/category forecast slices and robust spatial aggregation.
- Document potential reporting and population-density biases.
- If normalization data is available and validated, provide clearly labeled
  normalized rates; otherwise use absolute volume with caveats.

Member B:
- Supply backlog/closure predictions and explanations.
- Audit model stability by borough/category; document small-sample failures.
- Independently review assumptions behind proposed priority formula.

Member C:
- Implement transparent configurable score from normalized predicted
  demand, unresolved backlog and historical service delay.
- Clearly label the score as operational priority, not a proven staffing optimum.
- Backtest rankings on historical windows and compare simple baselines.
- Add score breakdowns, filters and map/table presentation.

Phase 6 gate:
- Scoring formula and feature timestamps are documented.
- Priority ranks and component contributions are reproducible.
- No claim of optimal staffing absent real budget/capacity data.

### Week 5 - Phase 7: Benchmarking, Integration, System Tests

Member A:
- Run Spark versus Pandas benchmarks under identical transformations,
  data subsets and hardware conditions, on at least three dataset scales
  when resources permit.
- Record environment, input size, wall time, throughput and memory caveats.
- Validate Spark partitions and data-reading performance.

Member B:
- Complete model comparisons, FP-Growth sensitivity analysis and
  statistical error breakdowns.
- Check date leakage, duplicate identifiers, reproducibility and failures.
- Produce experiment result tables and plots from measured metrics.

Member C:
- Integrate all pages, schemas and user flows in Streamlit.
- Run full lint, test suite, real Spark worker checks, ETL smoke tests,
  data-contract tests and app smoke tests.
- Create Docker or a documented Windows local runbook, whichever is feasible.
- Resolve UI/API contract mismatches via small reviewed PRs.

Phase 7 gate:
- End-to-end run works from manifest-backed data to dashboard.
- Benchmarks are measured; comparisons identify configuration and limitations.
- Application does not require loading the complete raw dataset into RAM.

### Week 6 - Phase 8: Final Audit, Report and Demonstration

Member A:
- Write source provenance, ETL and data-quality methodology.
- Provide data coverage/quality graphs and benchmark figures.
- Re-run data checks and confirm final outputs.

Member B:
- Write pattern mining and prediction methodology, results and limitations.
- Provide model evaluation and ablation/comparison figures.
- Re-run model experiments and confirm claims match metrics.

Member C:
- Assemble final professional report, architecture figure and README.
- Prepare clean setup instructions, dashboard demo and final slide narrative.
- Verify links, dependency pins, tests, Git status and release artifacts.

All three:
- Review each other's section for correctness.
- Demonstrate every claimed capability live or using reproducible outputs.
- Approve final release tag after the full tests pass.

Phase 8 gate:
- Complete report and repository accurately match running source code.
- No fake benchmark/model results; limitations are clearly stated.
- A new teammate can follow README and reproduce a small-data run.

## 5. Coordination and Git workflow

- Use one integration branch: main, protected by PR review where possible.
- Each person works on feature branches, not directly on main.
- Suggested branch prefixes: data/, mining/, app/.
- Keep ownership boundaries: A edits data pipeline/demand code; B edits
  mining/resolution code; C edits dashboard/priority/integration code.
- Shared files (requirements, configs, data dictionary, contracts) are changed
  through a small PR and notification to both teammates.
- Never commit raw sensitive exports, large datasets, secrets or credentials.
- Attach actual test output and relevant sample screenshots to PR descriptions.
- Prefer small PRs every 1-2 days; require at least one teammate review.
- Meet briefly twice a week: blockers, interface changes, integration demo.
- Merge order when dependencies matter: schema/contract, data output, model
  output, dashboard consumer.
- Any contract-breaking change requires a versioned migration or coordination.

## 6. Minimum review gates for every PR

[ ] Tests for changed paths passed.
[ ] Formatting/lint checks passed.
[ ] A data-producing change documents schema and grain.
[ ] Input/output row-count reconciliation is shown where relevant.
[ ] The result is deterministic or nondeterminism is explicitly justified.
[ ] Model changes use chronological evaluation without target leakage.
[ ] Real data findings have coverage dates and units.
[ ] No source or measured result is invented.
[ ] README/docs updated where behavior or commands changed.

## 7. Work dependencies and blockers

- A must freeze the Phase 3 cleaned/aggregated data contracts so B and C can
  develop independently using a small fixture.
- B can run FP-Growth on existing 100K sample for engineering tests, but must
  repeat meaningful pattern analysis after A expands historical coverage.
- A cannot claim a validated seasonal forecast from January 1-12 alone.
- B's resolution model needs point-in-time features and mature labels; newer
  unresolved complaints must not become false-negative labels.
- C can implement UI and ranking with test fixtures; real results must replace
  them before declaring a feature complete.
- If one model cannot be validated in time, keep the working baseline and
  report the limitation rather than replacing it with fabricated metrics.

## 8. First 48 hours after the handoff

Member A:
[ ] Inspect current ETL schema and create a dated data/analytics output contract.
[ ] Implement first Spark borough/category/daily aggregation and tests.
[ ] Draft data expansion plan with expected record counts/storage.

Member B:
[ ] Reconcile the 2,715 flagged records into per-flag and overlap counts.
[ ] Create a synthetic transaction fixture and FP-Growth contract.
[ ] Specify valid closure-within-7-days labels and excluded immature cohorts.

Member C:
[ ] Run Streamlit app; document environment and any startup errors.
[ ] Build navigation + mock-tested Overview/Hotspots panels.
[ ] Add schema-contract tests for shared data inputs.

Team:
[ ] Read PROGRESS_PHASE2.md, AGENTS.md, and docs/data_dictionary.md.
[ ] Agree on schema, date coverage, task issues, branches and PR policy.
[ ] Create first integration milestone: Phase 3 demo from existing 100K sample.

## 9. Final ownership summary

Member A: Big data pipeline + spatial/temporal analytics + demand forecast +
          Spark scalability benchmarks.
Member B: Frequent pattern mining + resolution prediction + ML evaluation.
Member C: Priority scoring + dashboard + full-system integration + QA/release.

Each person owns a coherent resume-worthy contribution, and shared milestones
allow all three to explain the complete architecture in a project review.
