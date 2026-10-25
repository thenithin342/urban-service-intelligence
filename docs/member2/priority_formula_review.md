# Independent review of the proposed priority score (Member 2)

Reviewer: Member 2 (B). Subject: Member 3's operational priority score
(`docs/data_contracts.md` 2.4, `priority_scores`). Inputs supplied by
Member 2: `closure_cohort_predictions.expected_open_after_7d` (backlog) and
`resolution_summary.median_resolution_hours` (delay). This is a review of
assumptions, not an implementation; Member 3 owns the formula and code.

## 1. Formula under review

For each unit u = (location, complaint_type) in one scoring window:

```
score(u) = w_d * N(demand_u) + w_b * N(backlog_u) + w_s * N(delay_u)
N(x)     = (x - min x) / (max x - min x)          (min-max over units)
w_d + w_b + w_s = 1, configurable
demand_u  = predicted (or trailing observed) complaint count
backlog_u = expected_open_after_7d = sum over complaints of (1 - p_close_7d)
delay_u   = historical median resolution hours
```

## 2. Assumptions and how they can fail

1. **Commensurability.** The three terms are different units (complaints,
   expected open complaints, hours). Min-max makes them dimensionless but
   the weights then express a policy trade-off, not a measured exchange rate.
   Document the weights as a value judgment and show the component breakdown
   next to every rank.
2. **Demand and backlog are not independent.** `expected_open_after_7d` is a
   sum over complaints, so it scales with volume: a unit with 10x the
   complaints and the same closure probability has ~10x the backlog. With
   both terms weighted, volume is effectively counted twice. Options: use
   the backlog *rate* `1 - mean_closure_probability` for the backlog term, or
   keep the count but state that volume dominates the score. Recommend
   reporting a sensitivity run with each.
3. **Delay is a historical median.** `median_resolution_hours` covers valid
   closures only (unresolved records have no duration), so units with many
   never-closed complaints look faster than they are (survivorship). Pair it
   with `unresolved_count / total_count` or use the 7-day closure rate.
   It is also agency-driven: on the 91-day history the median
   `resolution_hours` is 0.83 h for NYPD (314,887 closures) but 63.75 h for
   HPD (239,540) and 2,009 h for TLC (8,317), so the delay term largely
   ranks agencies rather than locations.
4. **Timing.** Every component must be computable at the scoring time T
   from records created before T (`docs/data_contracts.md` section 3).
   `median_resolution_hours` in `resolution_summary` is computed over the
   whole coverage window; for a backtest it must be recomputed on the
   pre-T window only, otherwise the delay term leaks future closures.
   The closure probabilities in `closure_cohort_predictions` come from a
   model trained before the test window and are valid as-of inputs; the
   `observed_*` columns are outcomes and must never enter the score.

## 3. Scale and normalization issues

- **Min-max is outlier-driven.** One extreme unit (e.g. a very high-volume
  borough x category) compresses all others toward 0; the ranking among the
  rest is preserved but component contributions become tiny and unstable
  between windows. Alternatives: rank-percentile normalization, or min-max
  on `log1p(x)` for count terms. Recommend log1p for demand/backlog and
  percentile for delay (heavy right tail: see the audit's p90/p99).
- **Degenerate ranges.** If max = min (single unit, or a constant term)
  N is 0/0; define N = 0 and record it.
- **Window dependence.** Min-max is relative to the units scored in that
  window, so scores are not comparable across windows or filters; only ranks
  within a window are. The dashboard must not trend raw scores over time.
- **Grain mismatch.** Backlog arrives at borough x complaint_type; ZIP-level
  scoring would need ZIP cohorts (not produced) or an explicit allocation
  rule. Null-borough complaints form their own cohort and should be shown
  but not ranked as a location.

## 4. Small-sample risk

Cohorts with few complaints have noisy observed rates and their predicted
backlog relies heavily on smoothing toward the category/global rate. The
cohort table carries `small_sample = n_complaints < 30`; the measured share
of such cohorts is in section 6. Recommend: exclude or down-weight
`small_sample` units from the top-k list, or shrink each component toward
the borough or category mean before normalizing, and always show n.

## 5. Reporting bias

311 counts measure *reporting*, not need. Reporting propensity differs by
neighbourhood (awareness, language, trust, digital access) and by category
(noise and parking are reported by passers-by; housing conditions by
tenants). A demand-weighted score therefore channels attention toward
high-reporting areas. Closure outcomes also reflect agency procedures
(e.g. complaints closed administratively), not resolution of the
underlying problem. The score is an advisory operational priority, not a
measure of need or a staffing optimum (no capacity or cost data exist).

## 6. Measured inputs (from Member 2 artifacts)

History run (799,439 records, created 2023-12-01 to 2024-02-29; test window
2024-02-01 to 2024-02-29, 240,229 complaints; full detail in
`reports/member2/results.md` section 4):

- `closure_cohort_predictions` has 738 borough x complaint_type cohorts;
  339 (46%) have n < 30 but together hold only 2,978 complaints (1.2%).
  Half the units a ranking would see are small-sample units.
- Backlog is concentrated: the top cohorts by `expected_open_after_7d` are
  HPD housing categories (UNSANITARY CONDITION in the Bronx: 1,951.6
  expected vs 1,835 observed open) plus a few never-closed categories
  (Noise - Helicopter in Manhattan: 1,436.4 expected, all 1,455 open). A
  count-based backlog term therefore mostly ranks by volume x category.
- Summed over cohorts the selected model expects 48,050 complaints still
  open after 7 days versus 46,826 observed (+2.6%); the mean absolute
  cohort error is 9.0 complaints (15.6 for cohorts with n >= 30).
- The selected backlog model (`category_rate`, chosen by validation Brier)
  assigns one probability per complaint type, so it cannot separate
  boroughs within a category (UNSANITARY CONDITION: predicted 0.304
  everywhere, observed 0.236 in Manhattan and 0.345 in the Bronx). The
  learned logistic model includes borough but its permutation importance
  for borough is ~0 (agency, as-of category rate and complaint type
  dominate). Borough differences in the backlog term are therefore driven
  by volume, not by modelled differences in closure speed.
- Per-borough calibration gaps of the selected model on the test window
  are within 0.031 (Staten Island -0.031 is the largest); per-category gaps
  reach -0.32 (Outdoor Dining, n = 398), so category-level backlog for
  rarer categories is the least reliable input.

## 7. Recommendations (summary)

1. Show component contributions and n with every rank; weights in config.
2. Use log1p (counts) / percentile (delay) normalization, N = 0 on
   degenerate ranges; compare against plain min-max in the backtest.
3. Avoid double-counting volume: run a variant with the backlog rate.
4. Recompute delay on pre-T data in backtests; never use `observed_*`.
5. Flag or shrink `small_sample` cohorts; keep null-borough separate.
6. Label the score "operational priority (advisory)" with coverage dates.
