# Member 1 demand and prioritization feature contract

These are additive artifacts under `artifacts/member1/demand/`. Existing analytics
schemas and teammate-owned modules stay intact. Scope is daily totals by the five
boroughs plus UNKNOWN; complaint_type=ALL means a total, not a trained category model.

## Time and features

An origin is the end of its NYC wall-calendar day. Targets are origin+1 through
origin+7. Counts, lags and trailing means stop at the origin. Calendar weekdays
are known ahead. Seasonal naive uses target-7; weekday mean uses target-7/-14/-21/-28.
These are observable at every supported horizon. There is a 28-day warmup.
Snapshot dates identify acquisition, not forecast time. No status, closed date
or resolution outcome is used as a feature.

Zero filling is allowed only inside the consecutive calendar whose complete
source manifest and ETL counts reconcile. An absent borough on a verified citywide
day is zero; an unverified/missing day is an error. This retrospective experiment
assumes complete daily counts were available at day end. A later acquisition cannot
prove that availability or exclude backfills. Three winter months cannot establish
annual forecasting accuracy.

## Evaluation

Validation labels follow train_end; validation origins start at that cutoff.
Test models refit only on labels through validation_end; test origins start there.
Only origins with all seven observed targets inside their block are scored.
Later observations can update features for later origins without refitting the
block's model. Every scored row has fit_cutoff <= forecast_origin.

Selection uses validation MAE only. Test scores cannot change the selected model.
Final forecasts refit through observed end. The best validation MLlib model is
retained even when a baseline wins. Negative predictions are clipped to zero,
without rounding; raw predictions are retained. All preprocessing fits inside
training-only Spark pipelines. LR internal standardization is disabled so explicit
scaling is a real ablation. WAPE is a ratio, null for zero total actual demand.
Overlapping horizon errors are correlated; no confidence/significance claim is made.

## Artifacts

- daily/: created_day date, borough string, count_0 bigint; one verified day/borough.
- features/: origin/target dates, borough, ALL category, horizon int, nullable actual
  count, count/lag/trailing/calendar/baseline numeric features.
- predictions/: forecast_origin date, target_date date, borough string,
  complaint_type string, horizon_days int, actual_count bigint, predicted_count
  double, raw_prediction double, model string, model_version string, fit_cutoff date,
  split string. Grain: model/split/origin/borough/horizon.
- future/: same schema for the selected model at the last observed origin;
  actual_count=null, split=future. These are historical-origin forecasts, not today.
- metrics/, metrics_borough/, metrics_horizon/, metrics_origins/: MAE, RMSE, WAPE
  overall, by borough, horizon and rolling origin.
- models/: final retained Spark PipelineModel artifacts, ignored by Git.
- feature_manifest.json and model_manifest.json: source hash, coverage, feature
  schema, cutoffs, selection rule, measured fit times, seeds and producer hash.

Member 3 must join on geography, origin, horizon and version. UNKNOWN is not a
real borough, and ALL is not a category. Priority weights/ranks remain Member 3's work.

## Phase 6 borough handoff

`priority_features/` has one row per borough/origin/model version: forecast_7d
(sum of seven predicted counts), trailing_7d (observed prior seven days),
absolute_change, percent_change (null for zero baseline), baseline_zero,
valid_geo_count, active_grid_cells, complaints_per_active_cell, spatial_period_start,
spatial_period_end, source_snapshot_max, complaint_type=ALL, horizon_days=7 and
uncertainty_method=not_calibrated. Counts per active cell are descriptive intensity,
not per-capita rates. A cell crossing source borough assignments can appear in
multiple boroughs; active-cell counts must not be summed as globally unique cells.
Missing snapshot metadata stays null and is never replaced by an event date.

All spatial observations stop at the historical forecast origin. The handoff
rejects population/geography mismatches, missing horizons, observed future labels
and nonfinite predictions. It supplies no staffing weights, ranks or risk scores.
