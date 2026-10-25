"""Member 2 Phase 4 unit tests: hand-computed rule metrics, redundancy, ordering.

Also a real-data regression check over the published mining artifacts
(recompute every rule from the saved baskets) when they exist locally.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from src.evaluation import mining_eval
from src.mining import baskets as bk
from src.mining import fpgrowth as fp
from src.utils import contracts
from tests.member2_synthetic import records_from_baskets, to_pandas

ROOT = Path(__file__).resolve().parents[2]

# Five baskets after excluding the null-ZIP one:
# {A,B,C}, {A,B}, {A,C}, {B,C}, {A,B}
W1, W2 = date(2024, 1, 1), date(2024, 1, 8)
SPEC = [
    ("10001", W1, ["A", "B", "C", "A"]),
    ("10002", W1, ["A", "B"]),
    ("10003", W1, ["A", "C"]),
    ("10001", W2, ["B", "C", "C"]),
    ("10004", W2, ["A", "B"]),
    (None, W1, ["A", "B"]),
]
BASKETS = [frozenset(x) for x in ({"A", "B", "C"}, {"A", "B"}, {"A", "C"}, {"B", "C"},
                                  {"A", "B"})]


def test_hand_computed_rule_metrics():
    m = mining_eval.rule_metrics(BASKETS, ["A"], ["B"])
    assert m["support"] == pytest.approx(3 / 5)
    assert m["confidence"] == pytest.approx(3 / 4)
    assert m["lift"] == pytest.approx((3 / 4) / (4 / 5))
    assert m["rule_baskets"] == 3
    m = mining_eval.rule_metrics(BASKETS, ["C"], ["A"])
    assert m["confidence"] == pytest.approx(2 / 3)
    assert m["lift"] == pytest.approx((2 / 3) / (4 / 5))


def test_brute_force_itemsets_hand_computed():
    got = mining_eval.brute_force_itemsets(BASKETS, 0.4)
    assert got == {frozenset("A"): 4, frozenset("B"): 4, frozenset("C"): 3,
                   frozenset("AB"): 3, frozenset("AC"): 2, frozenset("BC"): 2}


def test_pandas_baskets_dedupe_and_exclude_null_zip():
    pdf = to_pandas(records_from_baskets(SPEC))
    out = bk.build_baskets_pandas(pdf, "week")
    assert len(out) == 5
    assert [list(x) for x in out["items"]] == [["A", "B", "C"], ["A", "B"], ["A", "C"],
                                              ["B", "C"], ["A", "B"]]
    assert out["n_items"].tolist() == [3, 2, 2, 2, 2]


def test_sort_rules_breaks_float_ties_by_text():
    rules = pd.DataFrame({
        "antecedent": [["B"], ["A"], ["C"]],
        "consequent": [["C"], ["C"], ["A"]],
        "support": [0.4, 0.4, 0.4],
        "confidence": [0.5, 0.5, 2 / 3],
        "lift": [0.5 / 0.6, 0.5 / 0.6, (2 / 3) / 0.8],
    })
    out = fp.sort_rules(rules)
    assert [x[0] for x in out["antecedent"]] == ["C", "A", "B"]


def test_flag_redundant_marks_longer_equal_confidence_rules():
    rules = pd.DataFrame({
        "antecedent": [["A"], ["A", "B"], ["A", "C"]],
        "consequent": [["D"], ["D"], ["D"]],
        "confidence": [0.8, 0.8, 0.9],
    })
    assert fp.flag_redundant(rules).tolist() == [False, True, False]


def test_sensitivity_summary_groups_grid():
    grid = pd.DataFrame({
        "granularity": ["week"] * 2, "min_support": [0.5, 0.5], "min_confidence": [0.5, 0.9],
        "n_baskets": [10, 10], "n_itemsets": [7, 7], "n_rules": [9, 4],
        "max_lift": [1.2, 1.1], "runtime_seconds": [1.0, 3.0]})
    out = mining_eval.sensitivity_summary(grid)
    row = out.iloc[0]
    assert row["n_rules_min_conf"] == 9 and row["n_rules_max_conf"] == 4
    assert row["mean_runtime_seconds"] == 2.0


def _artifacts_present() -> bool:
    return (contracts.contract_path(ROOT, "pattern_rules").exists()
            and (ROOT / "artifacts/member2/mining/tables/baskets_week").exists())


@pytest.mark.skipif(not _artifacts_present(),
                    reason="real-data mining artifacts not generated in this checkout "
                           "(run scripts/run_member2_mining.py first)")
def test_real_data_rules_recompute_from_saved_baskets():
    rules = pd.read_parquet(contracts.contract_path(ROOT, "pattern_rules"))
    baskets = pd.read_parquet(ROOT / "artifacts/member2/mining/tables/baskets_week")
    assert contracts.validate(rules, "pattern_rules") == []
    assert rules["n_baskets"].nunique() == 1
    assert int(rules["n_baskets"].iloc[0]) == len(baskets)
    assert all(list(a) == sorted(a) for a in rules["antecedent"])
    check = mining_eval.verify_rules(baskets["items"].map(list), rules)
    assert max(check["max_abs_diff"][k] for k in ("support", "confidence", "lift")) < 1e-9
    assert check["max_abs_diff"]["rule_baskets"] == 0
    assert (rules["support"] >= rules["min_support"] - 1e-12).all()
    assert (rules["confidence"] >= rules["min_confidence"] - 1e-12).all()
    meta = json.loads(contracts.manifest_path(ROOT, "pattern_rules").read_text())
    assert meta["is_fixture"] is False and meta["n_baskets"] == len(baskets)
