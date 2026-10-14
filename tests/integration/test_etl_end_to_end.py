"""End-to-end ETL on the real 100k Phase 1 sample. ASCII only."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import functions as F

from src.processing.clean import clean, read_raw
from src.utils.spark import get_spark

ROOT = Path(__file__).resolve().parents[2]
RAW = str(ROOT / "data" / "raw" / "nyc311_sample.csv")


def test_end_to_end_100k_sample():
    assert Path(RAW).exists(), "Phase 1 sample missing; run download first"
    spark = get_spark("etl-e2e", master="local[2]")
    try:
        df, m = clean(read_raw(spark, RAW), snapshot="2024-02-01")
        # reconciliation: input = output + excluded + duplicates
        assert m["input_rows"] == 100000
        assert (
            m["output_rows"]
            + m["rows_excluded_missing_key"]
            + m["duplicates_removed"]
            == m["input_rows"]
        )
        # unique_key unique in output
        assert df.select("unique_key").distinct().count() == m["output_rows"]
        # unresolved rows have null (never zero) resolution
        bad = df.filter(
            F.array_contains(F.col("data_quality_flags"), "unresolved")
            & F.col("resolution_hours").isNotNull()
        ).count()
        assert bad == 0
        zero = df.filter(F.col("resolution_hours") == 0)
        # genuine zeros exist only when source closed==created to the second
        assert zero.filter(F.col("closed_at") != F.col("created_at")).count() == 0
        # every output row carries a snapshot date and a flags array
        assert df.filter(F.col("data_snapshot_date").isNull()).count() == 0
        assert df.filter(F.col("data_quality_flags").isNull()).count() == 0
    finally:
        spark.stop()
