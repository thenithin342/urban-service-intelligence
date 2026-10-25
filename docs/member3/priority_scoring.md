# Advisory priority score (Member 3)

Scoring version: `member3-priority-v1`. Code: `src/prioritization/`
(`aggregate.py`, `scoring.py`, `backtest.py`), CLI
`scripts/run_member3_priorities.py`, config block `member3:` in
`configs/sample.yaml`. Output contracts: `priority_scores`,
`priority_backtest` in `docs/data_contracts.md`.

**This is an advisory operational priority for human review, not a proven
staffing optimum.** No budget, crew capacity, travel time, cost or service
level data is used, so the score cannot say how many crews to send where.

## 1. Unit and windows

- Unit: `(location, complaint_type)`; `location` is the borough
  (`--location-type borough`, default) or the 5-digit ZIP (`zip`).
  Records with a null location are excluded and counted in the table meta
  (`records_without_location_excluded`).
- Scoring window: `window_days` calendar days `[window_start, window_end]`
  (inclusive, NYC wall-clock `created_day`). Default 7.
- Cut-off `T = window_end + 1 day` (00:00). The current scoring window is the
  last `window_days` full days of the input.
- Trailing partial days are dropped first: a final day with fewer than
  `partial_day_ratio` (0.5) x the median daily volume is treated as an
  acquisition cut-off (e.g. 2024-01-12 in the 100k sample, 191 records).

## 2. Components and feature timestamps

Every input is computable from records created before `T`
(`docs/data_contracts.md` section 3). `closed_at` comes from the acquisition
snapshot; it is only used to reconstruct the state at `T`.

| Component | Raw value (`*_value`) | Timestamp rule |
| :--- | :--- | :--- |
| demand | Observed: complaints created in the window / `window_days` (mean per day). If a real Member 1 `demand_forecast` exists (borough level, `training_cutoff <= window_end`): mean `predicted_count` over `[T, T + window_days)`. | created_day in window; forecast trained before T |
| backlog | Observed: complaints created in the window that are still open at T, i.e. `closed_at` is null or its date is `>= T`. If real Member 2 `closure_cohort_predictions` exist for a prediction window inside the scoring window: `expected_open_after_7d`. | state at T; only the boolean "open at T" is used, never the later close time |
| delay | Median `resolution_hours` of records closed before T (so created and closed before T), per unit. Null when fewer than `min_delay_records` (5) resolved records; optional `delay_lookback_days` restricts history. | `closed_at` date < T |

The table meta records which source was used (`demand_source`,
`backlog_source`, `delay_source`) and how many units used a model value
(`demand_model_units`, `backlog_model_units`). Units without a model value
keep the observed value. The backtest always uses observed inputs because
model outputs exist for one snapshot only.

The delay median is computed from a fixed resolution-hour histogram
(`scoring.HOUR_EDGES`: 0, 0.5, 1, 2, 3, 4, 6, 8, 12, 18, 24, 36, 48, 72, 96,
120, 168, 240, 336, 504, 720, 1080, 1440, 2160, 4320, 8760 h, open-ended) with
linear interpolation inside the median bin, because the Spark step only ships
small aggregates to pandas. It is exact to within the bin width.

## 3. Formula

For each component `c` in {demand, backlog, delay}, across all units of the
window:

```
c_component = (c_value - min) / (max - min)     # min-max to [0, 1]
            = 0 if max == min, or for null c_value
priority_score = w_demand * demand_component
               + w_backlog * backlog_component
               + w_delay * delay_component
```

Weights (`member3.weights`, default 0.5 / 0.3 / 0.2) must be exactly the three
keys, finite, non-negative and sum to 1 (validated; the CLI refuses invalid
weights). `rank` 1 is the highest score; ties are broken by `location` then
`complaint_type` ascending, so every rank is unique and reproducible.

The Priorities page shows each unit's weighted contributions
(`w_c * c_component`) as a stacked bar, so the score is fully decomposable.

## 4. Pipeline

1. Spark reads only the needed cleaned columns and aggregates them to three
   small tables (`aggregate.aggregate_spark`):
   `created` (day x location x complaint_type x close-lag days, count),
   `delay` (closed day x location x complaint_type x hour bin, count),
   `coords` (mean valid coordinates per location, for the map).
   Close lag is clipped to `lag_cap_days` (60, must be >= `window_days`), which
   preserves the "open at T" predicate for every window.
2. pandas scores the current window and runs the backtest.
3. Both tables are published with `contracts.write_table` plus meta
   (scoring_version, weights, sources, coverage, feature timestamps, timings).
   `priority_scores` additionally carries `window_count`, `delay_records`,
   `center_lat`, `center_lon` (extra columns are backward compatible).

A pandas reference (`aggregate.aggregate_pandas`) is cross-checked against the
Spark aggregation in `tests/integration/test_member3_priority_spark.py`.

## 5. Backtest

Rolling windows of `window_days`, stepping `step_days`, over the full days of
the input. For window `t` the units with complaints in `t` are ranked using
data before `T_t`; the truth is complaints created in
`[T_t, T_t + window_days)`. The next window must lie inside the data.

Methods on the same unit universe:

- `priority_score`: the configured score.
- `volume_only`: complaints created in window `t` (last-window count).
- `random`: uniform random scores, seed `random_seed + window index`.

Metrics: `precision_at_k` = |method top-k intersect true top-k| / k
(true top-k ties broken by location, complaint_type) for each k in
`backtest_k`, and Spearman rank correlation between the method score and the
next-window count (null when constant). Units that appear only in the target
window are outside the universe.

Interpretation: the target is future complaint volume, so `volume_only` is the
natural predictor of it. The priority score intentionally adds backlog and
delay, so a lower precision than `volume_only` means the score departs from
pure volume, not that it is wrong. Beating `random` shows the ranking carries
signal about where complaints keep arriving.

## 6. Limitations

- Reporting bias: 311 volume measures who reports, not the underlying need or
  infrastructure condition. Areas and groups that report less rank lower.
- Absolute volume, not per-capita: larger boroughs/ZIPs rank higher by
  construction. No validated population or housing-unit normalization is used.
- Min-max scaling is relative to the window's units; a score is comparable
  only within one run. Outliers (e.g. a rare type with a very long median
  delay) compress the other units' delay components.
- `closed_at` and status are an acquisition snapshot; closures can be revised
  later, and the snapshot is far after the windows.
- The earliest backtest windows have less resolved history for the delay
  median (history starts at the first day of the input).
- Three winter months (Dec 2023 - Feb 2024) or the 12-day sample cannot show
  seasonal stability of the ranking.
- Weights are a transparent policy choice, not learned or optimized.
