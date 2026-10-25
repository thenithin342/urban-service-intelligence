# Member 2 results: pattern mining, closure prediction, evaluation (Phases 3-7)

All numbers below were produced by the commands in section 7 on 2026-10-25
(local Spark 3.5.3, local[2], Python 3.10, Windows 11). Nothing is
estimated or copied from fixtures. Semantics: `docs/member2/semantics.md`;
mining method: `docs/member2/mining.md`; priority review:
`docs/member2/priority_formula_review.md`; Phase 3 sample audit:
`reports/member2/phase3_audit.md`.

## 1. Data coverage

| Dataset | Path (main checkout) | Rows | created_day | Snapshot |
| :--- | :--- | ---: | :--- | :--- |
| History (final results) | `data/processed/member1_history` | 799,439 | 2023-12-01 to 2024-02-29 (91 days) | 2026-10-25 |
| Sample (development) | `data/processed/nyc311_clean` | 100,000 | 2024-01-01 to 2024-01-12 | 2026-10-25 |

History: 0 duplicate `unique_key`; flagged rows 27,935 (per flag: unresolved
14,483, invalid_coords 11,289, invalid_zip 8,573, missing_borough 599,
negative_duration 377; per-flag sum 35,321, overlap excess 7,386; rows with
1/2/3/4 flags: 21,191 / 6,124 / 598 / 22). These equal Member 1's
`artifacts/member1/phase4/etl_quality.json` flag breakdown. Backlog at snapshot
(no `closed_at`): 14,483, of which 4,169 have status `Closed`. Spark vs
pandas consistency on a 20,000-row hash sample: all audit tables equal.
Resolution hours (784,579 valid closures): median 14.20 h, p90 681.97 h,
p99 6,132.15 h. Label: 799,062 eligible (positives 635,001 = 79.5%),
377 excluded for negative duration, 0 immature.

The history covers one winter (December-February). No seasonal conclusion
is drawn from it.

## 2. Pattern mining (FP-Growth)

Primary baskets: distinct complaint types per (incident_zip, created_week),
complete Monday-Sunday weeks only. 2,460 baskets from 264 ZIPs x 12 weeks
(2023-12-04 to 2024-02-25); 8,573 null-ZIP rows and 59,960 partial-week rows
(Dec 1-3 and Feb 26-29) excluded. Mean 39.8 categories per basket (median
45, max 78). Spark and pandas baskets identical.

Chosen thresholds: min_support 0.6, min_confidence 0.8 (fixed in the config
before the history run; see `docs/member2/mining.md` for why ZIP-week
baskets need high support). Result: 1,339 frequent itemsets, 4,768 rules,
3,087 non-redundant, 2,494 with lift > 1.2. Every rule's support,
confidence, lift and rule_baskets recomputed from the saved baskets in pure
Python: max absolute difference 0.0.

Top non-redundant rules by lift (history, ZIP x week):

| Antecedent | Consequent | Support | Confidence | Lift | Baskets |
| :--- | :--- | ---: | ---: | ---: | ---: |
| Blocked Driveway, HEAT/HOT WATER, Noise - Residential, UNSANITARY CONDITION | PAINT/PLASTER | 0.604 | 0.865 | 1.337 | 1485 |
| Blocked Driveway, HEAT/HOT WATER, UNSANITARY CONDITION | PAINT/PLASTER | 0.605 | 0.863 | 1.334 | 1489 |
| Blocked Driveway, Noise - Residential, UNSANITARY CONDITION | PAINT/PLASTER | 0.604 | 0.856 | 1.323 | 1487 |
| HEAT/HOT WATER, Noise - Residential, UNSANITARY CONDITION | PAINT/PLASTER | 0.627 | 0.855 | 1.322 | 1543 |
| HEAT/HOT WATER, Illegal Parking, UNSANITARY CONDITION | PAINT/PLASTER | 0.628 | 0.852 | 1.318 | 1545 |
| HEAT/HOT WATER, UNSANITARY CONDITION | PAINT/PLASTER | 0.629 | 0.851 | 1.316 | 1548 |

Secondary (ZIP x day, support 0.2, confidence 0.8): 16,855 baskets (267 ZIPs
x 91 days, mean 16.2 items), 523 itemsets, 853 rules (826 non-redundant);
top lift 1.894: {HEAT/HOT WATER, Illegal Parking, Noise - Residential,
PAINT/PLASTER, PLUMBING} -> {UNSANITARY CONDITION} (support 0.209,
confidence 0.931). Regression check on 500 rules: max difference 0.0.

Interpretation: the strongest associations link HPD housing-condition
categories (UNSANITARY CONDITION, PAINT/PLASTER, WATER LEAK, DOOR/WINDOW,
PLUMBING, HEAT/HOT WATER) reported in the same ZIP and week or day; high-volume
categories (Illegal Parking, Blocked Driveway, Noise - Residential) appear in
antecedents mostly because they are present in most baskets. These are
co-reporting associations, not causal relationships, and are confounded by
ZIP reporting volume. The same housing cluster tops the sample run
(`docs/member2/mining.md`), so the pattern is stable from a 12-day window to
the 91-day window, while absolute counts are not comparable across runs.

### FP-Growth sensitivity (threshold grid, history)

| granularity | min_support | min_confidence | n_baskets | n_itemsets | n_rules | non-redundant | lift > 1.2 | median_lift | max_lift | runtime_s |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| week | 0.5 | 0.5 | 2460 | 25184 | 128642 | 48863 | 101485 | 1.229 | 1.486 | 17.282 |
| week | 0.5 | 0.7 | 2460 | 25184 | 128557 | 48778 | 101435 | 1.229 | 1.486 | 8.973 |
| week | 0.5 | 0.9 | 2460 | 25184 | 95702 | 30221 | 70097 | 1.223 | 1.486 | 6.949 |
| week | 0.6 | 0.5 | 2460 | 1339 | 4872 | 3189 | 2537 | 1.201 | 1.337 | 2.201 |
| week | 0.6 | 0.7 | 2460 | 1339 | 4872 | 3189 | 2537 | 1.201 | 1.337 | 1.507 |
| week | 0.6 | 0.9 | 2460 | 1339 | 3558 | 2089 | 1642 | 1.199 | 1.304 | 2.238 |
| week | 0.7 | 0.5 | 2460 | 89 | 208 | 184 | 20 | 1.177 | 1.230 | 1.272 |
| week | 0.7 | 0.7 | 2460 | 89 | 208 | 184 | 20 | 1.177 | 1.230 | 1.253 |
| week | 0.7 | 0.9 | 2460 | 89 | 169 | 145 | 20 | 1.180 | 1.230 | 1.035 |
| week | 0.8 | 0.5 | 2460 | 5 | 4 | 4 | 0 | 1.113 | 1.114 | 1.091 |
| week | 0.8 | 0.7 | 2460 | 5 | 4 | 4 | 0 | 1.113 | 1.114 | 1.105 |
| week | 0.8 | 0.9 | 2460 | 5 | 4 | 4 | 0 | 1.113 | 1.114 | 1.054 |
| day | 0.1 | 0.5 | 16855 | 7166 | 26349 | 22027 | 22284 | 1.388 | 3.418 | 16.034 |
| day | 0.1 | 0.7 | 16855 | 7166 | 20691 | 17014 | 16672 | 1.343 | 3.418 | 13.086 |
| day | 0.1 | 0.9 | 16855 | 7166 | 11550 | 9421 | 8058 | 1.311 | 2.774 | 12.275 |
| day | 0.2 | 0.5 | 16855 | 523 | 1314 | 1268 | 926 | 1.272 | 2.324 | 7.215 |
| day | 0.2 | 0.7 | 16855 | 523 | 1119 | 1080 | 742 | 1.253 | 2.324 | 6.724 |
| day | 0.2 | 0.9 | 16855 | 523 | 621 | 617 | 367 | 1.259 | 1.894 | 7.701 |
| day | 0.3 | 0.5 | 16855 | 104 | 162 | 162 | 77 | 1.184 | 1.645 | 3.111 |
| day | 0.3 | 0.7 | 16855 | 104 | 141 | 141 | 59 | 1.168 | 1.645 | 3.284 |
| day | 0.3 | 0.9 | 16855 | 104 | 86 | 86 | 38 | 1.109 | 1.331 | 3.456 |
| day | 0.4 | 0.5 | 16855 | 25 | 47 | 47 | 19 | 1.168 | 1.374 | 1.211 |
| day | 0.4 | 0.7 | 16855 | 25 | 39 | 39 | 13 | 1.166 | 1.301 | 1.278 |
| day | 0.4 | 0.9 | 16855 | 25 | 17 | 17 | 6 | 1.102 | 1.301 | 1.160 |

Findings: (1) support is the dominant control: each 0.1 step of
min_support changes the ZIP-week rule count by one to two orders of magnitude
(128,642 -> 4,872 -> 208 -> 4 at confidence 0.5), while confidence 0.5 vs
0.7 changes almost nothing because frequent ZIP-week rules almost all have
confidence > 0.7. (2) Lift falls as support rises (max lift 1.486 at 0.5,
1.114 at 0.8): the most frequent categories are near-ubiquitous and
therefore near-independent. (3) Day baskets reach higher lifts (up to 3.42 at
support 0.1) because a day basket is less saturated. (4) Runtimes are
single-machine wall times including JVM warm-up (first fit slowest);
indicative only. Figure: `artifacts/member2/figures/threshold_sensitivity.png`
(week confidence 0.5 and 0.7 lines overlap because their counts are nearly
identical).

## 3. Closure-within-7-days prediction

Chronological split on `created_day` with a 7-day label embargo, so every
training label was knowable at the cutoff:

| Split | Window | Rows | Positive rate |
| :--- | :--- | ---: | ---: |
| train (fit + validation) | 2023-12-01 to 2024-01-24 | 495,395 | 0.788 |
| fit | 2023-12-01 to 2024-01-17 | 427,337 | 0.785 |
| validation (model selection) | 2024-01-18 to 2024-01-24 | 68,058 | 0.804 |
| embargo (unused) | 2024-01-25 to 2024-01-31 | - | - |
| test | 2024-02-01 to 2024-02-29 | 240,229 | 0.805 |

train_cutoff = 2024-02-01. Models: `prior_rate` (training positive rate),
`category_rate` (per complaint type, Beta-smoothed, alpha 20),
`asof_category_rate` (strictly historical per-type rate using only
complaints created >= 7 days before each prediction), `logistic_regression`
and `gradient_boosting` (sklearn HistGradientBoosting) on creation-time
features (complaint type top-40, agency, borough, weekday, hour sin/cos,
train-window ZIP frequency, ZIP missing, as-of category and agency rates
and history counts). Selection rule, declared before looking at test:
lowest validation Brier, then refit on the full training window.

| model | split | n | positive_rate | pr_auc | roc_auc | brier | precision@0.5 | recall@0.5 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| prior_rate | validation | 68058 | 0.8039 | 0.8039 | 0.5000 | 0.1580 | 0.8039 | 1.0000 |
| prior_rate | test | 240229 | 0.8051 | 0.8051 | 0.5000 | 0.1572 | 0.8051 | 1.0000 |
| category_rate | validation | 68058 | 0.8039 | 0.9882 | 0.9529 | 0.0674 | 0.9661 | 0.9111 |
| category_rate | test | 240229 | 0.8051 | 0.9874 | 0.9492 | 0.0712 | 0.9554 | 0.9107 |
| asof_category_rate | validation | 68058 | 0.8039 | 0.9890 | 0.9529 | 0.0676 | 0.9662 | 0.9106 |
| asof_category_rate | test | 240229 | 0.8051 | 0.9881 | 0.9494 | 0.0709 | 0.9552 | 0.9112 |
| logistic_regression | validation | 68058 | 0.8039 | 0.9890 | 0.9527 | 0.0684 | 0.9653 | 0.9093 |
| logistic_regression | test | 240229 | 0.8051 | 0.9881 | 0.9495 | 0.0715 | 0.9553 | 0.9109 |
| gradient_boosting | validation | 68058 | 0.8039 | 0.9889 | 0.9521 | 0.0697 | 0.9480 | 0.9215 |
| gradient_boosting | test | 240229 | 0.8051 | 0.9884 | 0.9502 | 0.0743 | 0.9588 | 0.9082 |

(Train-split in-sample rows are in `artifacts/member2/resolution/tables/model_comparison`;
gradient boosting has the best in-sample Brier 0.0620 but the worst learned
test Brier, i.e. it overfits.) PR-AUC is average precision.

Results:
- Selected model: `category_rate` (validation Brier 0.06739 vs 0.06765
  as-of rate, 0.06839 logistic, 0.06971 gradient boosting).
- Every non-trivial model halves the prior's Brier (0.157 -> 0.071-0.074 on
  test). The learned models do **not** beat the per-category baselines: the
  four non-prior models are within 0.0035 Brier and 0.001 PR-AUC of each
  other on test, and the best test Brier is the as-of baseline (0.0709).
  Closure within 7 days is almost entirely determined by complaint type and
  agency.
- PR-AUC is high partly because the positive rate is 0.805; the prior's
  PR-AUC of 0.805 is the reference. Precision/recall at 0.5 are ~0.955 /
  ~0.911 for all non-prior models.
- Calibration (test, 10 bins): expected calibration error 0.0091 for the
  selected model, 0.0082 as-of rate, 0.0099 logistic, 0.0295 gradient
  boosting, 0.0171 prior. The selected model's largest bin gap is bin
  0.6-0.7 (mean prediction 0.688, observed 0.571, n = 3,123). Figures:
  `calibration_test.png`, `pr_curves_test.png`, `model_comparison_brier.png`.
- Explanation (permutation importance of the best learned model,
  logistic regression, Brier increase on 20,000 test rows): agency 0.0657,
  as-of category rate 0.0543, complaint type 0.0386, as-of agency rate
  0.0104; hour, weekday, ZIP frequency and borough < 0.0001 (borough
  slightly negative, i.e. no measurable contribution).

Sample run (development only; cutoff 2024-01-09, no embargo, test
2024-01-09..12, 29,761 rows, positive rate 0.754): test Brier prior 0.1882,
category_rate 0.0926 (selected), as-of 0.0976, logistic 0.0976,
gradient boosting 0.1062. The same ordering (category baselines >= learned
models) holds.

## 4. Closure cohorts, stability and error breakdown (Phase 6)

`closure_cohort_predictions` (selected model, test window): 738 borough x
complaint_type cohorts covering all 240,229 test complaints; 339 cohorts
(46%) have n < 30 (`small_sample`), holding 2,978 complaints (1.2%).
Expected open after 7 days (sum of 1 - p) 48,050 vs observed 46,826 (+2.6%).
Cohort-level backlog error (mean |expected - observed open| per cohort; all
cohorts / cohorts with n >= 30): prior 84.2 / 153.1, category_rate 9.0 /
15.6, as-of 8.4 / 14.7, logistic 10.2 / 17.8, gradient boosting 13.9 / 24.9
(`predictions/closure_cohorts_all_models`).

By borough (selected model vs prior Brier, test): Brooklyn 0.0705 vs 0.1448
(n 74,318), Queens 0.0640 vs 0.1372 (58,072), Manhattan 0.0715 vs 0.1867
(50,148), Bronx 0.0767 vs 0.1709 (49,343), Staten Island 0.0928 vs 0.1445
(8,140), unknown borough 0.1004 vs 0.3218 (208). Borough calibration gaps
are within +/-0.031 (largest Staten Island -0.031). Stable across boroughs.

By complaint type: 172 types in the test window, 53 with n < 30 (377
complaints). Among types with n >= 30 the selected model is worse than the
prior for 13 (e.g. Lost Property n 424, Outdoor Dining n 398, Illegal Tree
Damage n 217, Emergency Response Team n 294); the largest calibration gaps
are Outdoor Dining -0.32 and Lost Property -0.29: their February closure
rates (0.67 and 1.00) are far above the training-window rates the model
uses (predicted 0.34 and 0.71). For the large HPD categories the
model explains about half of the prior's error (UNSANITARY CONDITION Brier
0.220 vs 0.433, n 8,488; PLUMBING 0.249 vs 0.357) but these remain the
least predictable large categories (observed 7-day closure 0.30-0.46).

By agency: NYPD (98,779) is near-deterministic (observed 0.99996, Brier
0.0007); HPD (65,914) observed 0.656, Brier 0.124, ROC-AUC 0.869; DOB
observed 0.469, Brier 0.183; DHS Brier 0.259 with ROC-AUC 0.646. OTI has
n = 8 (small sample). Tables under `artifacts/member2/evaluation/tables`
(`error_by_agency`, `error_by_cohort`, `mining_sensitivity`) and
`artifacts/member2/resolution/tables` (`stability_by_*`).

## 5. Integrity, leakage and reproducibility (Phase 7)

- Contracts: all five published Member 2 tables pass `contracts.validate`;
  every `.meta.json` has `is_fixture: false`.
- Duplicates: 0 duplicate `unique_key` in the input, the model frame and the
  test predictions.
- Date leakage: last training day 2024-01-24 < first test day 2024-02-01
  (7-day embargo gap); 0 train/test key overlap; no forbidden column
  (`closed_at`, `status_snapshot`, `resolution_hours`,
  `data_snapshot_date`, flags, label) in the feature list (also asserted by
  unit tests, including a future-label perturbation test for the as-of
  features).
- Reproducibility: re-running mining and resolution into a fresh root gave
  identical content hashes for pattern_rules (4,768 rows), frequent_itemsets
  (1,339), resolution_summary (826), model_comparison (15),
  closure_cohorts (738), baskets_week (2,460), test_predictions (240,229),
  calibration, stability_by_borough and stability_by_category (rerun 159 s).
  `threshold_experiments` is excluded from the hash check because it records
  wall-clock runtimes.

## 6. Limitations

- One winter of history (Dec 2023 - Feb 2024): no seasonality, and closure
  rates of some categories differ between the training months and February
  (section 4).
- Labels come from one snapshot (2026-10-25). Records with status `Closed`
  but no `closed_at` (4,169 in history) are labelled "not closed within 7
  days"; this is a conservative choice, not a verified outcome.
- The 7-day task is easy at the type/agency level; models add little beyond
  per-category rates, and nothing distinguishes boroughs within a category.
  Cohort backlog differences across boroughs therefore mostly reflect volume.
- ZIP-week baskets are saturated; rules have modest lift and are dominated
  by HPD housing categories and ubiquitous items. Rules are associations,
  not causation, and reflect reporting behaviour.
- Single-machine local Spark; runtimes indicative only.
- Complaint counts are reported demand, not need (reporting bias).

## 7. Commands to reproduce

From the repository root (`<main>` = directory holding the cleaned data):

```powershell
$H = "<main>/data/processed/member1_history"
.\.venv\Scripts\python.exe scripts/run_member2_audit.py      --config configs/history.yaml --input $H
.\.venv\Scripts\python.exe scripts/run_member2_mining.py     --config configs/history.yaml --input $H
.\.venv\Scripts\python.exe scripts/run_member2_resolution.py --config configs/history.yaml --input $H
.\.venv\Scripts\python.exe scripts/run_member2_evaluation.py --config configs/history.yaml --input $H --repro
# sample (development):
.\.venv\Scripts\python.exe scripts/run_member2_audit.py --config configs/sample.yaml
```

Measured wall times (history): audit 50 s, mining incl. 24-point grid
3 min 32 s, resolution 1 min 45 s, evaluation with re-run 2 min 47 s.
Tests: `python -m pytest -q tests/unit` and
`python -m pytest -q tests/integration/test_member2_spark.py`.
