"""Spark MLlib FP-Growth itemsets, association rules and threshold grid. ASCII only.

Outputs are collected to pandas only after counting against a guard, and
sorted deterministically:
- itemsets: support desc, item_count asc, items text asc;
- rules: lift desc, confidence desc, support desc, antecedent text asc,
  consequent text asc.

Association rules describe co-reporting within the same basket; they are
not causal statements.
"""

from __future__ import annotations

import time
from itertools import combinations
from typing import Any

import pandas as pd
from pyspark.ml.fpm import FPGrowth
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

JOIN = " | "


class ItemsetExplosion(RuntimeError):
    """Raised when a threshold yields more itemsets than the guard allows."""


def _fit(baskets: DataFrame, min_support: float, min_confidence: float):
    return FPGrowth(itemsCol="items", minSupport=float(min_support),
                    minConfidence=float(min_confidence)).fit(baskets)


def _text(v: Any) -> str:
    return JOIN.join(list(v))


# Float metrics are rounded for ordering only, so values that differ in the
# last ulp (e.g. 0.5/0.6 vs (2/3)/0.8) tie and fall back to the text keys.
SORT_DECIMALS = 12


def sort_itemsets(pdf: pd.DataFrame) -> pd.DataFrame:
    key = pdf["items"].map(_text)
    return (pdf.assign(_k=key, _s=pdf["support"].round(SORT_DECIMALS))
            .sort_values(["_s", "item_count", "_k"], ascending=[False, True, True],
                         ignore_index=True, kind="mergesort")
            .drop(columns=["_k", "_s"]))


def sort_rules(pdf: pd.DataFrame) -> pd.DataFrame:
    keys = {f"_{c}": pdf[c].round(SORT_DECIMALS) for c in ("lift", "confidence", "support")}
    return (pdf.assign(_a=pdf["antecedent"].map(_text), _c=pdf["consequent"].map(_text),
                       **keys)
            .sort_values(["_lift", "_confidence", "_support", "_a", "_c"],
                         ascending=[False, False, False, True, True], ignore_index=True,
                         kind="mergesort")
            .drop(columns=["_a", "_c", "_lift", "_confidence", "_support"]))


def flag_redundant(rules: pd.DataFrame) -> pd.Series:
    """True when a rule with a strict subset antecedent and the same consequent
    has at least the same confidence (the longer rule adds no information)."""
    conf = {
        (frozenset(a), tuple(c)): float(v)
        for a, c, v in zip(rules["antecedent"], rules["consequent"], rules["confidence"])
    }
    out = []
    for a, c, v in zip(rules["antecedent"], rules["consequent"], rules["confidence"]):
        items = list(a)
        red = False
        for k in range(1, len(items)):
            for sub in combinations(items, k):
                other = conf.get((frozenset(sub), tuple(c)))
                if other is not None and other >= float(v) - 1e-12:
                    red = True
                    break
            if red:
                break
        out.append(red)
    return pd.Series(out, index=rules.index, dtype=bool)


def mine(baskets: DataFrame, n_baskets: int, min_support: float, min_confidence: float,
         max_itemsets: int = 200000) -> dict[str, Any]:
    """Fit FP-Growth and return itemsets/rules as sorted pandas frames."""
    t0 = time.time()
    model = _fit(baskets, min_support, min_confidence)
    itemsets_sdf = model.freqItemsets
    n_itemsets = itemsets_sdf.count()
    if n_itemsets > max_itemsets:
        raise ItemsetExplosion(
            f"{n_itemsets} itemsets at min_support={min_support} exceeds guard {max_itemsets}")
    rules_sdf = model.associationRules
    itemsets = itemsets_sdf.select(
        F.array_sort("items").alias("items"),
        F.size("items").cast("long").alias("item_count"),
        F.col("freq").cast("long").alias("freq"),
    ).toPandas()
    rules = rules_sdf.select(
        F.array_sort("antecedent").alias("antecedent"),
        F.array_sort("consequent").alias("consequent"),
        F.col("support").cast("double").alias("support"),
        F.col("confidence").cast("double").alias("confidence"),
        F.col("lift").cast("double").alias("lift"),
    ).toPandas()
    runtime = time.time() - t0
    itemsets["support"] = itemsets["freq"] / float(n_baskets)
    itemsets["n_baskets"] = int(n_baskets)
    for frame, cols in ((itemsets, ["items"]), (rules, ["antecedent", "consequent"])):
        for c in cols:
            frame[c] = frame[c].map(lambda v: [str(x) for x in list(v)])
    rules["rule_baskets"] = (rules["support"] * n_baskets).round().astype("int64")
    rules["n_baskets"] = int(n_baskets)
    rules["is_redundant"] = flag_redundant(rules)
    return {
        "itemsets": sort_itemsets(itemsets),
        "rules": sort_rules(rules),
        "n_itemsets": int(n_itemsets),
        "runtime_seconds": float(runtime),
    }


def threshold_grid(baskets: DataFrame, n_baskets: int, supports: list[float],
                   confidences: list[float], max_itemsets: int = 200000,
                   ) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    """Run FP-Growth for every (min_support, min_confidence) pair.

    Returns the experiment table and a list of skipped pairs (guard hits).
    Runtime covers fitting plus materializing itemsets and rules.
    """
    rows: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for s in sorted(supports):
        for c in sorted(confidences):
            try:
                out = mine(baskets, n_baskets, s, c, max_itemsets)
            except ItemsetExplosion as exc:
                skipped.append({"min_support": s, "min_confidence": c, "reason": str(exc)})
                continue
            lifts = out["rules"]["lift"]
            rows.append({
                "min_support": float(s),
                "min_confidence": float(c),
                "n_baskets": int(n_baskets),
                "n_itemsets": out["n_itemsets"],
                "n_rules": len(out["rules"]),
                "median_lift": float(lifts.median()) if len(lifts) else None,
                "max_lift": float(lifts.max()) if len(lifts) else None,
                "runtime_seconds": round(out["runtime_seconds"], 3),
                "n_rules_lift_gt_1_2": int((lifts > 1.2).sum()),
                "n_rules_non_redundant": int((~out["rules"]["is_redundant"]).sum()),
            })
    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame["median_lift"] = frame["median_lift"].astype("float64")
        frame["max_lift"] = frame["max_lift"].astype("float64")
    return frame, skipped
