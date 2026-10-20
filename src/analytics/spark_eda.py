"""Distributed (PySpark) EDA aggregations for Member 1 Phase 3. ASCII only.

Principles:
- Spark performs all transformations; only bounded *aggregates* are
  collected to the driver (see ``bounded_pandas``).
- Every aggregation is deterministic: explicit ordering after groupBy.
- Unknown borough / ZIP / category are surfaced as-is (null or
  ``UNKNOWN``), never silently redistributed into a real value.
- Counts describe *reported complaints*, not infrastructure quality.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

WEEKDAY_ORDER = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]


def load_clean(spark: SparkSession, path: str) -> DataFrame:
    """Read the Phase 2 cleaned Parquet table (partitioned year/month)."""
    return spark.read.parquet(path)


def bounded_pandas(df: DataFrame, max_rows: int) -> Any:
    """Collect to pandas only if the frame is within the safety bound.

    Aggregates are expected to be small. Exceeding the bound signals a
    mistake (e.g. collecting raw rows), so this fails loudly.
    """
    n = df.count()
    if n > max_rows:
        raise ValueError(
            f"refusing to collect {n} rows (> max_collect_rows={max_rows}); "
            "aggregate or bound the frame first"
        )
    return df.toPandas()


def _flag_rows(df: DataFrame) -> DataFrame:
    return df.filter(F.size(F.col("data_quality_flags")) > 0)


def verify_inputs(df: DataFrame) -> dict[str, Any]:
    """Input integrity + quality reconciliation for the cleaned table."""
    row_count = df.count()
    distinct_keys = df.select("unique_key").distinct().count()

    bounds = df.agg(
        F.min("created_at").alias("created_min"),
        F.max("created_at").alias("created_max"),
        F.min("closed_at").alias("closed_min"),
        F.max("closed_at").alias("closed_max"),
        F.min("data_snapshot_date").alias("snapshot_min"),
        F.max("data_snapshot_date").alias("snapshot_max"),
        F.countDistinct("created_day").alias("days_covered"),
    ).collect()[0]

    flag_rows = (
        _flag_rows(df)
        .select(F.explode("data_quality_flags").alias("flag"))
        .groupBy("flag")
        .count()
        .orderBy("flag")
        .collect()
    )
    flag_counts = {r["flag"]: int(r["count"]) for r in flag_rows}

    hist_rows = (
        df.select(F.size("data_quality_flags").alias("n_flags"))
        .groupBy("n_flags")
        .count()
        .orderBy("n_flags")
        .collect()
    )
    flag_row_hist = {int(r["n_flags"]): int(r["count"]) for r in hist_rows}

    flagged_rows = df.filter(F.size(F.col("data_quality_flags")) > 0).count()
    valid_coords = df.filter(
        F.col("latitude").isNotNull() & F.col("longitude").isNotNull()
    ).count()

    return {
        "row_count": int(row_count),
        "distinct_unique_key": int(distinct_keys),
        "duplicate_keys": int(row_count - distinct_keys),
        "schema": [f"{f.name}:{f.dataType.simpleString()}" for f in df.schema],
        "created_min": str(bounds["created_min"]),
        "created_max": str(bounds["created_max"]),
        "closed_min": str(bounds["closed_min"]),
        "closed_max": str(bounds["closed_max"]),
        "snapshot_min": str(bounds["snapshot_min"]),
        "snapshot_max": str(bounds["snapshot_max"]),
        "days_covered": int(bounds["days_covered"]),
        "flag_counts": flag_counts,
        "flag_row_histogram": flag_row_hist,
        "flagged_rows": int(flagged_rows),
        "clean_rows": int(row_count - flagged_rows),
        "valid_coords_rows": int(valid_coords),
        "invalid_coords_rows": int(row_count - valid_coords),
    }


def flag_overlap(df: DataFrame) -> DataFrame:
    """Pairwise co-occurrence of quality flags (rows carrying both)."""
    exploded = (
        df.filter(F.size(F.col("data_quality_flags")) >= 2)
        .select("unique_key", F.explode("data_quality_flags").alias("flag"))
    )
    a = exploded.alias("a")
    b = exploded.alias("b")
    return (
        a.join(b, F.col("a.unique_key") == F.col("b.unique_key"))
        .filter(F.col("a.flag") < F.col("b.flag"))
        .groupBy(F.col("a.flag").alias("flag_a"), F.col("b.flag").alias("flag_b"))
        .count()
        .withColumnRenamed("count", "rows")
        .orderBy(F.col("rows").desc(), "flag_a", "flag_b")
    )


def complaints_by_borough(df: DataFrame) -> DataFrame:
    return (
        df.groupBy(F.col("borough").alias("borough"))
        .count()
        .orderBy(F.col("count").desc(), F.col("borough").asc())
        .withColumnRenamed("count", "complaint_count")
    )


def complaints_by_agency(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("agency")
        .count()
        .orderBy(F.col("count").desc(), F.col("agency").asc())
        .withColumnRenamed("count", "complaint_count")
    )


def complaints_by_category(df: DataFrame) -> DataFrame:
    return (
        df.groupBy(F.col("complaint_type").alias("complaint_type"))
        .count()
        .orderBy(F.col("count").desc(), F.col("complaint_type").asc())
        .withColumnRenamed("count", "complaint_count")
    )


def complaints_by_zip(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("incident_zip").isNotNull())
        .groupBy(F.col("incident_zip").alias("incident_zip"))
        .count()
        .orderBy(F.col("count").desc(), F.col("incident_zip").asc())
        .withColumnRenamed("count", "complaint_count")
    )


def borough_category(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("borough", "complaint_type")
        .count()
        .orderBy(F.col("count").desc(), "borough", "complaint_type")
        .withColumnRenamed("count", "complaint_count")
    )


def complaints_daily(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("created_day").isNotNull())
        .groupBy(F.col("created_day").alias("date"))
        .count()
        .orderBy("date")
        .withColumnRenamed("count", "complaint_count")
    )


def complaints_daily_borough(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("created_day").isNotNull())
        .groupBy(F.col("created_day").alias("date"), "borough")
        .count()
        .orderBy("date", "borough")
        .withColumnRenamed("count", "complaint_count")
    )


def complaints_weekly(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("created_week").isNotNull())
        .groupBy(F.col("created_week").alias("week_start"))
        .count()
        .orderBy("week_start")
        .withColumnRenamed("count", "complaint_count")
    )


def hour_weekday_heatmap(df: DataFrame) -> DataFrame:
    """Long-format hour x weekday counts (weekday ordered Mon..Sun)."""
    idx = F.lit(len(WEEKDAY_ORDER))
    for i, day in enumerate(WEEKDAY_ORDER, start=1):
        idx = F.when(F.col("weekday") == day, F.lit(i)).otherwise(idx)
    return (
        df.filter(F.col("created_at").isNotNull())
        .groupBy("weekday", "hour")
        .count()
        .withColumn("_wd", idx)
        .orderBy("_wd", "hour")
        .drop("_wd")
        .withColumnRenamed("count", "complaint_count")
    )


def resolution_by_borough(df: DataFrame) -> DataFrame:
    """Resolution-hour quantiles over *resolved* rows only (null excluded)."""
    return (
        df.filter(F.col("resolution_hours").isNotNull())
        .groupBy("borough")
        .agg(
            F.count(F.lit(1)).alias("resolved_rows"),
            F.round(F.mean("resolution_hours"), 3).alias("mean_hours"),
            F.round(F.expr("percentile_approx(resolution_hours, 0.5)"), 3).alias(
                "median_hours"
            ),
            F.round(F.expr("percentile_approx(resolution_hours, 0.9)"), 3).alias(
                "p90_hours"
            ),
            F.round(F.expr("percentile_approx(resolution_hours, 0.99)"), 3).alias(
                "p99_hours"
            ),
            F.round(F.max("resolution_hours"), 3).alias("max_hours"),
        )
        .orderBy("borough")
    )


def time_coverage(df: DataFrame) -> dict[str, Any]:
    """Observed daily coverage inside the [min, max] created_day window."""
    row = df.agg(
        F.min("created_day").alias("start"),
        F.max("created_day").alias("end"),
        F.countDistinct("created_day").alias("observed_days"),
    ).collect()[0]
    start, end, observed = row["start"], row["end"], int(row["observed_days"])
    expected = (end - start).days + 1 if start is not None and end is not None else 0
    return {
        "period_start": str(start),
        "period_end": str(end),
        "expected_days": int(expected),
        "observed_days": observed,
        "coverage_ratio": float(observed / expected) if expected else 0.0,
    }
