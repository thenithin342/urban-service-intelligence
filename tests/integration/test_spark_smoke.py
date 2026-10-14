"""Spark smoke test (JVM-safe, no Python worker needed). ASCII only."""

from __future__ import annotations

from src.utils.spark import get_spark


def test_spark_session_range_count():
    spark = get_spark("phase0-smoke", master="local[1]")
    try:
        assert spark.range(10).count() == 10
        assert isinstance(spark.version, str) and len(spark.version) > 0
    finally:
        spark.stop()
