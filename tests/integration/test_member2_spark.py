"""Member 2 Spark integration tests over small synthetic records.

Spark implementations are checked against independent pandas references and
hand-computed values. One module-scoped session keeps the suite fast.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.evaluation import quality_audit as qa
from src.modeling import resolution as res
from src.utils.spark import ensure_worker_env, get_spark
from tests.member2_synthetic import (
    EXPECTED_LABELS,
    label_cases,
    spark_schema,
    to_pandas,
)


@pytest.fixture(scope="module")
def spark():
    ensure_worker_env()
    s = get_spark("member2-tests", master="local[2]")
    yield s
    s.stop()


@pytest.fixture(scope="module")
def cases(spark):
    rows = label_cases()
    return spark.createDataFrame(rows, schema=spark_schema()), to_pandas(rows)


def test_closure_label_spark_matches_expected_and_pandas(cases):
    sdf, pdf = cases
    got = res.add_closure_label_spark(sdf).select(
        "unique_key", "label_status", "closed_within_7d").collect()
    assert {r[0]: (r[1], r[2]) for r in got} == EXPECTED_LABELS
    assert qa.frames_equal(
        res.label_summary_spark(res.add_closure_label_spark(sdf)),
        res.label_summary_pandas(res.add_closure_label_pandas(pdf)),
    )


def test_flag_reconciliation_spark_equals_pandas(cases):
    sdf, pdf = cases
    s, p = qa.flag_reconciliation_spark(sdf), qa.flag_reconciliation_pandas(pdf)
    assert qa.compare_spark_pandas(s, p, ["per_flag", "combinations", "pairwise"]) == {
        "per_flag": True, "combinations": True, "pairwise": True}
    assert s["summary"] == p["summary"]


def test_resolution_audit_spark_equals_pandas(cases):
    sdf, pdf = cases
    s, p = qa.resolution_audit_spark(sdf), qa.resolution_audit_pandas(pdf)
    keys = ["status_closure", "duration_classes", "backlog_by_borough", "quantiles"]
    assert all(qa.compare_spark_pandas(s, p, keys).values())
    assert s["closed_after_snapshot_rows"] == 0


def test_resolution_summary_spark_equals_pandas(cases):
    sdf, pdf = cases
    s = res.resolution_summary_spark(res.add_closure_label_spark(sdf)).toPandas()
    p = res.resolution_summary_pandas(res.add_closure_label_pandas(pdf))
    for frame in (s, p):
        frame["coverage_start"] = frame["coverage_start"].astype(str)
        frame["coverage_end"] = frame["coverage_end"].astype(str)
    assert qa.frames_equal(s, p, float_tol=1e-9)
    from src.utils import contracts

    assert contracts.validate(s, "resolution_summary") == []
    assert isinstance(s, pd.DataFrame)


# ---------------------------------------------------------------------------
# Phase 4: baskets and FP-Growth
# ---------------------------------------------------------------------------

def _basket_frames(spark):
    from tests.member2_synthetic import records_from_baskets
    from tests.unit.test_member2_mining import SPEC

    rows = records_from_baskets(SPEC)
    return spark.createDataFrame(rows, schema=spark_schema()), to_pandas(rows)


def test_spark_baskets_match_pandas_and_hand_counts(spark):
    from src.mining import baskets as bk

    sdf, pdf = _basket_frames(spark)
    sb, stats = bk.build_baskets_spark(sdf, "week")
    got = sb.toPandas()
    ref = bk.build_baskets_pandas(pdf, "week")
    assert stats["n_baskets"] == 5 and stats["excluded_null_zip_rows"] == 2
    assert stats["basket_rows"] == stats["input_rows"] - 2
    assert got["incident_zip"].tolist() == ref["incident_zip"].tolist()
    assert [list(x) for x in got["items"]] == [list(x) for x in ref["items"]]


def test_fpgrowth_hand_computed_support_confidence_lift(spark):
    from src.mining import baskets as bk
    from src.mining import fpgrowth as fp

    sdf, _ = _basket_frames(spark)
    sb, stats = bk.build_baskets_spark(sdf, "week")
    out = fp.mine(sb, stats["n_baskets"], 0.4, 0.5)
    assert out["n_itemsets"] == 6
    r = out["rules"]
    got = [(a[0], c[0], round(s, 6), round(cf, 6), round(lf, 6), rb)
           for a, c, s, cf, lf, rb in zip(r["antecedent"], r["consequent"], r["support"],
                                         r["confidence"], r["lift"], r["rule_baskets"])]
    assert got == [
        ("A", "B", 0.6, 0.75, round(0.75 / 0.8, 6), 3),
        ("B", "A", 0.6, 0.75, round(0.75 / 0.8, 6), 3),
        ("C", "A", 0.4, round(2 / 3, 6), round((2 / 3) / 0.8, 6), 2),
        ("C", "B", 0.4, round(2 / 3, 6), round((2 / 3) / 0.8, 6), 2),
        ("A", "C", 0.4, 0.5, round(0.5 / 0.6, 6), 2),
        ("B", "C", 0.4, 0.5, round(0.5 / 0.6, 6), 2),
    ]
    its = out["itemsets"]
    assert dict(zip(its["items"].map(tuple), its["freq"])) == {
        ("A",): 4, ("B",): 4, ("C",): 3, ("A", "B"): 3, ("A", "C"): 2, ("B", "C"): 2}


def test_fpgrowth_equals_brute_force_on_random_baskets(spark):
    import random

    from src.evaluation import mining_eval
    from src.mining import fpgrowth as fp

    rng = random.Random(42)
    items = list("ABCDEFG")
    baskets = [sorted(set(rng.sample(items, rng.randint(1, 5)))) for _ in range(60)]
    sb = spark.createDataFrame([(b,) for b in baskets], "items array<string>")
    out = fp.mine(sb, len(baskets), 0.15, 0.3)
    got = dict(zip(out["itemsets"]["items"].map(frozenset), out["itemsets"]["freq"]))
    assert got == mining_eval.brute_force_itemsets(baskets, 0.15)
    check = mining_eval.verify_rules(baskets, out["rules"])
    assert check["rules_checked"] == len(out["rules"]) > 0
    assert max(check["max_abs_diff"][k] for k in ("support", "confidence", "lift")) < 1e-12


def test_drop_partial_weeks_and_threshold_grid(spark):
    from datetime import date, timedelta

    from src.mining import baskets as bk
    from src.mining import fpgrowth as fp
    from tests.member2_synthetic import records_from_baskets

    full = [("10001", date(2024, 1, 1) + timedelta(days=i), ["A", "B"]) for i in range(7)]
    part = [("10001", date(2024, 1, 8), ["A"]), ("10002", date(2024, 1, 9), ["B"])]
    sdf = spark.createDataFrame(records_from_baskets(full + part), schema=spark_schema())
    _kept, stats = bk.build_baskets_spark(sdf, "week", drop_partial_weeks=True)
    assert stats["n_baskets"] == 1 and stats["excluded_partial_week_rows"] == 2
    assert [w["days_observed"] for w in stats["weeks"]] == [7, 2]
    allb, s2 = bk.build_baskets_spark(sdf, "week")
    grid, skipped = fp.threshold_grid(allb, s2["n_baskets"], [0.3, 0.6], [0.5, 0.9])
    assert len(grid) == 4 and skipped == []
    from src.utils import contracts

    assert contracts.validate(grid, "mining_threshold_experiments") == []
    assert (grid.groupby("min_support")["n_itemsets"].nunique() == 1).all()
