# Member 2 frequent pattern mining (Phase 4)

Code: `src/mining/baskets.py`, `src/mining/fpgrowth.py`,
`src/evaluation/mining_eval.py`, `scripts/run_member2_mining.py`.
Basket semantics: `docs/member2/semantics.md` section 2.
Final measured results (dataset used for the report): `reports/member2/results.md`.

## Method

1. Baskets (Spark): `groupBy(incident_zip, created_week)` and
   `array_sort(collect_set(complaint_type))`. Null-ZIP rows are excluded and
   counted. A pandas reference builder must produce identical baskets
   (checked on every CLI run: `spark_vs_pandas_baskets_equal`).
2. Spark MLlib `pyspark.ml.fpm.FPGrowth` (`minSupport`, `minConfidence`)
   gives frequent itemsets and association rules with Spark's support,
   confidence and lift (lift = confidence / support(consequent)).
3. Post-processing: antecedents/consequents sorted;
   `rule_baskets = round(support * n_baskets)`; deterministic order (lift,
   confidence, support desc, then antecedent/consequent text; floats rounded
   to 12 decimals for ordering only). An additive `is_redundant` column marks
   a rule whose antecedent has a strict subset with the same consequent and
   at least the same confidence (the longer rule adds no information).
4. Regression check: every published rule's support, confidence, lift and
   `rule_baskets` are recomputed in pure Python from the saved baskets
   (`artifacts/member2/mining/tables/baskets_week`); the CLI exits non-zero
   on any difference above 1e-9. Synthetic tests check FP-Growth against
   hand-computed values and an exhaustive itemset enumeration.
5. Threshold grid: every (min_support, min_confidence) pair is fitted
   separately; the table records n_baskets, n_itemsets, n_rules, median/max
   lift and wall-clock runtime (fit + materialization, local Spark).
   A secondary ZIP x day basket definition is run as a sensitivity
   experiment in the same table (`granularity` column, additive).

## Why the thresholds are high

ZIP-week baskets are dense: on the 100k sample (397 baskets from 207 ZIPs x
2 weeks) a basket holds 37.9 distinct categories on average (median 41,
max 73), and 33 categories appear in at least half of all baskets. Lower
supports explode combinatorially: at min_support 0.5 FP-Growth returns 9,953
itemsets and 45,500 rules; at 0.6 it returns 689 itemsets. The primary run
therefore uses min_support 0.6 and min_confidence 0.8 (configs/sample.yaml),
which keeps the rule table reviewable while still above the 0.7 support
level where no rule exceeds lift 1.2. The day-level baskets (2,137 baskets,
mean 15.9 items) support lower thresholds and give higher lifts (up to 3.50
at support 0.1); they are reported as a sensitivity result, not the primary
artifact.

## Sample run (100k records, created 2024-01-01 to 2024-01-12)

Primary (ZIP x week, support 0.6, confidence 0.8): 397 baskets (1,094 null-ZIP
rows excluded), 689 itemsets, 2,153 rules, 1,047 non-redundant, 531 with
lift > 1.2. Regression check: max abs difference 0.0 for support,
confidence, lift and rule_baskets over all 2,153 rules.

Top non-redundant rules by lift:

| Antecedent | Consequent | Support | Confidence | Lift | Baskets |
| :--- | :--- | ---: | ---: | ---: | ---: |
| HEAT/HOT WATER, UNSANITARY CONDITION, WATER LEAK | PAINT/PLASTER | 0.602 | 0.895 | 1.372 | 239 |
| UNSANITARY CONDITION, WATER LEAK | PAINT/PLASTER | 0.602 | 0.888 | 1.362 | 239 |
| HEAT/HOT WATER, Noise - Residential, WATER LEAK | PAINT/PLASTER | 0.602 | 0.882 | 1.352 | 239 |
| HEAT/HOT WATER, PAINT/PLASTER, UNSANITARY CONDITION | WATER LEAK | 0.602 | 0.948 | 1.345 | 239 |
| HEAT/HOT WATER, WATER LEAK | PAINT/PLASTER | 0.607 | 0.876 | 1.343 | 241 |
| PAINT/PLASTER | WATER LEAK | 0.610 | 0.934 | 1.325 | 242 |

Secondary (ZIP x day, support 0.2, confidence 0.8): 2,137 baskets, 565
itemsets, 964 rules (803 non-redundant); top lift 2.42
({PLUMBING, UNSANITARY CONDITION, WATER LEAK} -> {PAINT/PLASTER}, support
0.201, confidence 0.867).

Threshold grid (sample):

| granularity | min_support | min_confidence | n_baskets | n_itemsets | n_rules | non-redundant | lift > 1.2 | median_lift | max_lift | runtime_s |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| week | 0.5 | 0.5 | 397 | 9953 | 45500 | 10687 | 25137 | 1.203 | 1.536 | 8.735 |
| week | 0.5 | 0.7 | 397 | 9953 | 45387 | 10597 | 25097 | 1.203 | 1.536 | 3.407 |
| week | 0.5 | 0.9 | 397 | 9953 | 28962 | 4787 | 13263 | 1.197 | 1.474 | 3.082 |
| week | 0.6 | 0.5 | 397 | 689 | 2236 | 1125 | 539 | 1.182 | 1.372 | 1.573 |
| week | 0.6 | 0.7 | 397 | 689 | 2233 | 1122 | 539 | 1.182 | 1.372 | 1.525 |
| week | 0.6 | 0.9 | 397 | 689 | 1486 | 617 | 257 | 1.180 | 1.345 | 1.365 |
| week | 0.7 | 0.5 | 397 | 58 | 119 | 95 | 0 | 1.150 | 1.200 | 1.183 |
| week | 0.7 | 0.7 | 397 | 58 | 119 | 95 | 0 | 1.150 | 1.200 | 1.138 |
| week | 0.7 | 0.9 | 397 | 58 | 96 | 74 | 0 | 1.155 | 1.200 | 1.186 |
| week | 0.8 | 0.5 | 397 | 5 | 4 | 4 | 0 | 1.092 | 1.093 | 0.966 |
| week | 0.8 | 0.7 | 397 | 5 | 4 | 4 | 0 | 1.092 | 1.093 | 1.098 |
| week | 0.8 | 0.9 | 397 | 5 | 4 | 4 | 0 | 1.092 | 1.093 | 0.775 |
| day | 0.1 | 0.5 | 2137 | 9596 | 39693 | 22792 | 34748 | 1.568 | 3.505 | 3.892 |
| day | 0.1 | 0.7 | 2137 | 9596 | 31156 | 16297 | 26287 | 1.407 | 3.505 | 3.683 |
| day | 0.1 | 0.9 | 2137 | 9596 | 17341 | 7090 | 12632 | 1.375 | 2.808 | 3.654 |
| day | 0.2 | 0.5 | 2137 | 565 | 1537 | 1351 | 1206 | 1.344 | 2.422 | 2.050 |
| day | 0.2 | 0.7 | 2137 | 565 | 1281 | 1109 | 963 | 1.324 | 2.422 | 1.813 |
| day | 0.2 | 0.9 | 2137 | 565 | 669 | 532 | 401 | 1.306 | 1.908 | 1.956 |
| day | 0.3 | 0.5 | 2137 | 102 | 184 | 177 | 121 | 1.274 | 1.832 | 1.405 |
| day | 0.3 | 0.7 | 2137 | 102 | 146 | 139 | 86 | 1.245 | 1.770 | 1.096 |
| day | 0.3 | 0.9 | 2137 | 102 | 80 | 74 | 34 | 1.142 | 1.368 | 0.844 |
| day | 0.4 | 0.5 | 2137 | 24 | 44 | 42 | 22 | 1.198 | 1.397 | 0.553 |
| day | 0.4 | 0.7 | 2137 | 24 | 37 | 35 | 17 | 1.193 | 1.335 | 0.572 |
| day | 0.4 | 0.9 | 2137 | 24 | 15 | 14 | 4 | 1.131 | 1.335 | 0.567 |

Runtimes are single-machine local Spark (local[2]) wall times and include
JVM warm-up effects (the first fit is slowest); they are indicative only.

## Interpretation and applicability

- Rules are **associations between categories co-reported in the same ZIP
  and week**, not causal links. "PAINT/PLASTER -> WATER LEAK" means ZIP-weeks
  with a paint/plaster complaint also had a water-leak complaint 93% of the
  time, 1.32x more often than water-leak ZIP-weeks overall. A plausible
  shared driver (housing-maintenance complaints routed to HPD in residential
  ZIPs) is a hypothesis, not a finding.
- Co-occurrence is confounded by ZIP reporting volume: high-volume ZIPs report
  almost every category every week. Lift partly corrects for item
  prevalence but not for basket size.
- The sample spans 12 days (2 basket weeks, one of them 5 days long);
  support values are not stable seasonal estimates. Results must be
  repeated on multi-month history before drawing conclusions
  (see `reports/member2/results.md` for the dataset finally used).
