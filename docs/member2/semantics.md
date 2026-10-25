# Member 2 semantics: baskets, closure label, censoring, leakage

Document version: `1.0.0` (2026-10-25). Owner: Member 2 (B).
Code: `src/mining/baskets.py`, `src/modeling/resolution.py`,
`src/evaluation/quality_audit.py`. Output contracts: `docs/data_contracts.md`
section 2.1-2.2 (frozen, not changed by Member 2).

All inputs are the Phase 2 cleaned table (`docs/data_dictionary.md`).

## 1. Time conventions

- `created_day`, `created_week` (Monday) and `hour` are the ETL's wall-clock
  calendar keys. Member 2 uses them for every calendar decision (baskets,
  chronological split, maturity) so results do not depend on the reader's
  time zone.
- Durations are epoch-second differences `closed_at - created_at`, which are
  time-zone invariant. (Note: pandas reads the Parquet timestamps as naive
  UTC instants while Spark renders them in the session zone; differences and
  orderings are identical, wall-clock renderings are not, which is why
  calendar keys come from the ETL columns.)
- `data_snapshot_date` is the acquisition date. For the current sample and
  history it is 2026-10-25, so every 2024 complaint has far more than 7 days
  of observation.

## 2. Transaction (basket) semantics for FP-Growth

Primary basket: **the set of distinct `complaint_type` values reported in one
`incident_zip` during one `created_week` (Monday-start calendar week).**

- Basket key = (`incident_zip`, `created_week`). Items = `complaint_type`
  strings.
- Items are deduplicated (`collect_set`): 40 noise complaints and 1 rodent
  complaint in the same ZIP-week give the basket `{Noise..., Rodent}`. Volume
  is therefore NOT represented; a basket answers "which categories were
  reported at least once".
- Records with null `incident_zip` (the `invalid_zip` flag) or null
  `created_week` are excluded from baskets and counted in the mining
  metadata (`excluded_null_zip_rows`, `excluded_null_week_rows`).
- `complaint_type = 'UNKNOWN'` (missing complaint) would be kept as an item
  and counted; the current inputs have none.
- Partial weeks: a week cut by the coverage window (the sample's
  week of 2024-01-08 has 5 of 7 days) is kept by default for the sample and
  reported in `weeks` metadata (days observed per week). The history config
  sets `drop_partial_weeks: true` so only complete Monday-Sunday weeks are
  mined.
- Secondary (sensitivity) basket: (`incident_zip`, `created_day`), same item
  rules. It is reported as a separate experiment, not as the primary rules.
- Support is a fraction of baskets; `rule_baskets = round(support *
  n_baskets)`. Rule antecedents are sorted lists.

Interpretation: rules are **co-reporting associations** between categories in
the same ZIP and week. They are not causal and they are dominated by ZIP
reporting volume: busy ZIPs report most categories every week, so very
frequent categories co-occur almost everywhere and their lift is close to 1.
Lift, not confidence alone, indicates association beyond the item's own
prevalence.

## 3. Closure-within-7-days label

- Prediction (as-of) time: `created_at` of the complaint.
- Horizon: `H = 7` days (`member2.closure_horizon_days`).
- `duration = closed_at - created_at` (hours).
- `closed_within_7d = 1` iff `closed_at` is not null AND `duration >= 0` AND
  `duration <= 168 h` (exactly 168 h counts as closed within 7 days; zero
  durations count as closed).
- `closed_within_7d = 0` for eligible records with `closed_at` null at the
  snapshot or `duration > 168 h`. This includes records whose
  `status_snapshot` says `Closed` but which carry no `closed_at` (no closure
  time can be verified); their count is reported in the audit.
- Excluded (no label, counted per reason in `label_status`):
  - `excluded_negative_duration`: `closed_at < created_at`;
  - `excluded_missing_created`: null `created_at`/`created_day`;
  - `excluded_immature` (censoring): fewer than `H` full days of observation,
    i.e. `data_snapshot_date - created_day < H + 1` days. Such records are
    excluded, never labelled 0, because their outcome is unknown. The `+1`
    makes the rule conservative for complaints created late on their
    created day.
- `resolution_hours` from the ETL (null for unresolved/negative) is used only
  for descriptive delay statistics and the 7-day label consistency check.

## 4. Leakage rules for closure models

Features must be computable at the complaint's `created_at` from that
complaint's creation-time fields and from records whose outcomes were known
before `created_at`.

- Forbidden as features of the predicted record (enforced by
  `resolution.FORBIDDEN_FEATURES` and a leakage test): `closed_at`,
  `closed_date`, `status_snapshot`, `status`, `resolution_hours`,
  `duration_hours`, `data_snapshot_date`, `data_quality_flags`, the label and
  `label_status`. Quality flags are excluded because `unresolved` and
  `negative_duration` encode the outcome.
- Allowed creation-time features: `complaint_type`, `agency`, `borough`,
  `hour`, `weekday`, ZIP frequency (computed from the training window only)
  and strictly historical category closure rates (below).
- Historical (as-of) category closure rate for a record created at `t`: uses
  only records `j` of the same category with `created_at_j + H <= t`, so the
  full 7-day outcome of `j` was observable at `t`. Using records closed
  before `t` but created less than `H` ago would bias the rate upward
  (only fast closures are visible yet), so they are not used. The rate is
  Beta-smoothed toward the as-of global rate computed the same way.
- Chronological split: train on `created_day < train_cutoff -
  label_embargo_days`, test on `created_day >= train_cutoff`. With an
  embargo of `H` days every training label was knowable at the cutoff. The
  12-day sample is too short for a 7-day embargo, so the sample run uses
  embargo 0 and states that limitation; the history run uses 7.
- Future complaint information (counts after `t`, later statuses) is never
  used. Baselines are fitted on the training split only.
