"""Independent checks and summaries for FP-Growth outputs. ASCII only.

- ``rule_metrics`` recomputes support / confidence / lift for one rule
  directly from baskets (pure Python), so Spark's numbers can be verified
  on real data and on synthetic fixtures.
- ``brute_force_itemsets`` enumerates frequent itemsets exhaustively for
  small synthetic basket sets (reference for FP-Growth).
- ``sensitivity_summary`` condenses the threshold grid for reporting.
"""

from __future__ import annotations

from collections.abc import Iterable
from itertools import combinations
from typing import Any

import pandas as pd


def _sets(baskets: Iterable[Iterable[str]]) -> list[frozenset[str]]:
    return [frozenset(b) for b in baskets]


def rule_metrics(baskets: list[frozenset[str]], antecedent: Iterable[str],
                 consequent: Iterable[str]) -> dict[str, float]:
    a, c = frozenset(antecedent), frozenset(consequent)
    n = len(baskets)
    n_a = sum(1 for b in baskets if a <= b)
    n_c = sum(1 for b in baskets if c <= b)
    n_ac = sum(1 for b in baskets if (a | c) <= b)
    support = n_ac / n
    confidence = n_ac / n_a if n_a else float("nan")
    lift = confidence / (n_c / n) if n_c else float("nan")
    return {"support": support, "confidence": confidence, "lift": lift,
            "rule_baskets": n_ac}


def verify_rules(baskets: Iterable[Iterable[str]], rules: pd.DataFrame,
                 max_rules: int | None = None) -> dict[str, Any]:
    """Max absolute difference between reported and recomputed rule metrics."""
    sets = _sets(baskets)
    subset = rules if max_rules is None else rules.head(max_rules)
    diffs = {"support": 0.0, "confidence": 0.0, "lift": 0.0, "rule_baskets": 0}
    for row in subset.itertuples(index=False):
        m = rule_metrics(sets, row.antecedent, row.consequent)
        for k in ("support", "confidence", "lift"):
            diffs[k] = max(diffs[k], abs(float(getattr(row, k)) - m[k]))
        diffs["rule_baskets"] = max(diffs["rule_baskets"],
                                    abs(int(row.rule_baskets) - m["rule_baskets"]))
    return {"rules_checked": len(subset), "n_baskets": len(sets),
            "max_abs_diff": diffs}


def brute_force_itemsets(baskets: Iterable[Iterable[str]], min_support: float,
                         max_len: int = 6) -> dict[frozenset[str], int]:
    """Exhaustive frequent itemsets (support >= min_support) for tiny inputs."""
    sets = _sets(baskets)
    n = len(sets)
    items = sorted(set().union(*sets)) if sets else []
    out: dict[frozenset[str], int] = {}
    for k in range(1, min(max_len, len(items)) + 1):
        found = False
        for combo in combinations(items, k):
            fs = frozenset(combo)
            freq = sum(1 for b in sets if fs <= b)
            if freq / n >= min_support - 1e-12:
                out[fs] = freq
                found = True
        if not found:
            break
    return out


def sensitivity_summary(grid: pd.DataFrame) -> pd.DataFrame:
    """Per granularity x min_support: rule counts across confidence levels."""
    if grid.empty:
        return grid
    keys = [k for k in ("granularity", "min_support") if k in grid.columns]
    g = grid.groupby(keys, sort=True)
    out = g.agg(
        n_baskets=("n_baskets", "first"),
        n_itemsets=("n_itemsets", "first"),
        n_rules_min_conf=("n_rules", "max"),
        n_rules_max_conf=("n_rules", "min"),
        max_lift=("max_lift", "max"),
        mean_runtime_seconds=("runtime_seconds", "mean"),
    ).reset_index()
    return out
