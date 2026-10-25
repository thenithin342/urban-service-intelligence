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
