"""PySpark ETL: typed schema, dedupe, validation, Parquet output. ASCII only.

Rules (see reports/methodology.md and docs/data_dictionary.md):
- Raw CSV is never modified; it is read with an explicit schema.
- Dedupe by unique_key, keeping the earliest created_at (documented).
- Rows with missing unique_key are excluded and counted, never silent.
- Invalid ZIP/coords become null plus a quality flag; never repaired.
- resolution_hours only for valid closed>=created; unresolved stays null.
  Unknown durations are NEVER zero-filled and closure is NEVER fabricated.
- Timestamp interpretation: source strings are NYC wall time without
  offset (e.g. 2024-01-01T00:04:14.000); parsed as naive timestamps,
  no timezone conversion.
- Output is overwritten (idempotent reruns, no silent appends).
"""

from __future__ import annotations

from datetime import date
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from pyspark.sql.window import Window

RAW_FIELDS = [
    "unique_key",
    "created_date",
    "closed_date",
    "complaint_type",
    "descriptor",
    "agency",
    "status",
    "borough",
    "incident_zip",
    "latitude",
    "longitude",
]

RAW_SCHEMA = StructType([StructField(c, StringType(), True) for c in RAW_FIELDS])

TS_MILLIS = "yyyy-MM-dd'T'HH:mm:ss.SSS"
TS_PLAIN = "yyyy-MM-dd'T'HH:mm:ss"

LAT_MIN, LAT_MAX = 40.49, 40.92
LON_MIN, LON_MAX = -74.26, -73.70

CLEAN_COLUMNS = [
    "unique_key",
    "created_at",
    "closed_at",
    "complaint_raw",
    "complaint_type",
    "descriptor",
    "agency",
    "status_snapshot",
    "borough_raw",
    "borough",
    "incident_zip_raw",
    "incident_zip",
    "latitude",
    "longitude",
    "resolution_hours",
    "created_day",
    "created_week",
    "weekday",
    "month",
    "year",
    "hour",
    "data_snapshot_date",
    "data_quality_flags",
]


def parse_ts(col: F.Column) -> F.Column:
    return F.coalesce(
        F.to_timestamp(col, TS_MILLIS),
        F.to_timestamp(col, TS_PLAIN),
    )


def _flag(cond: F.Column, name: str) -> F.Column:
    return F.when(cond, F.lit(name))


def clean(df_raw: DataFrame, snapshot: str | None = None) -> tuple[DataFrame, dict[str, Any]]:
    """Clean raw frame. Returns (clean_df, metrics). Runs actions for metrics."""
    snap = snapshot or date.today().isoformat()
    input_rows = df_raw.count()

    w = (
        df_raw.withColumn("created_at", parse_ts(F.col("created_date")))
        .withColumn("closed_at", parse_ts(F.col("closed_date")))
        .withColumn("unique_key", F.trim(F.col("unique_key")))
        .withColumn(
            "unique_key",
            F.when(F.col("unique_key") == "", F.lit(None)).otherwise(F.col("unique_key")),
        )
    )
    missing_key = w.filter(F.col("unique_key").isNull()).count()
    w = w.filter(F.col("unique_key").isNotNull())

    # Dedupe: keep earliest created_at per unique_key (nulls last).
    # Secondary raw-string keys make survivor selection deterministic
    # on created_at ties (row_number needs a total order for reruns to
    # be byte-identical, not just count-identical).
    win = Window.partitionBy("unique_key").orderBy(
        F.col("created_at").asc_nulls_last(),
        F.col("created_date").asc_nulls_last(),
        F.col("closed_date").asc_nulls_last(),
        F.col("complaint_type").asc_nulls_last(),
        F.col("descriptor").asc_nulls_last(),
        F.col("agency").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
        F.col("borough").asc_nulls_last(),
        F.col("incident_zip").asc_nulls_last(),
        F.col("latitude").asc_nulls_last(),
        F.col("longitude").asc_nulls_last(),
    )
    w = w.withColumn("_rn", F.row_number().over(win))
    duplicates_removed = w.filter(F.col("_rn") > 1).count()
    w = w.filter(F.col("_rn") == 1).drop("_rn")

    # Parse coordinates into temp columns first: reusing the names
    # "latitude"/"longitude" in expressions while overwriting them in the
    # same projection creates duplicate attributes (AnalysisException).
    w = w.withColumn("_lat", w["latitude"].cast(DoubleType())).withColumn(
        "_lon", w["longitude"].cast(DoubleType())
    )
    lat, lon = F.col("_lat"), F.col("_lon")
    coords_ok = (
        lat.isNotNull()
        & lon.isNotNull()
        & (lat >= LAT_MIN) & (lat <= LAT_MAX)
        & (lon >= LON_MIN) & (lon <= LON_MAX)
    )
    borough_trim = F.upper(F.trim(F.col("borough")))
    borough_std = F.when(
        borough_trim.isNull() | (borough_trim == "") | (borough_trim == "UNSPECIFIED"),
        F.lit(None),
    ).otherwise(borough_trim)
    complaint_trim = F.trim(F.col("complaint_type"))
    complaint_std = F.when(
        complaint_trim.isNull() | (complaint_trim == ""), F.lit("UNKNOWN")
    ).otherwise(complaint_trim)
    zip_trim = F.trim(F.col("incident_zip"))
    zip_std = F.when(zip_trim.rlike(r"^[0-9]{5}$"), zip_trim).otherwise(F.lit(None))

    created = F.col("created_at")
    closed = F.col("closed_at")
    resolution = F.when(
        created.isNotNull() & closed.isNotNull() & (closed >= created),
        (F.unix_timestamp(closed) - F.unix_timestamp(created)) / 3600.0,
    )

    flags = F.array_compact(
        F.array(
            _flag(F.col("created_date").isNotNull() & created.isNull(), "invalid_created"),
            _flag(F.col("closed_date").isNotNull() & closed.isNull(), "invalid_closed"),
            _flag(
                created.isNotNull() & closed.isNotNull() & (closed < created),
                "negative_duration",
            ),
            _flag(closed.isNull(), "unresolved"),
            _flag(created.isNull(), "missing_created"),
            _flag(~coords_ok, "invalid_coords"),
            _flag(borough_std.isNull(), "missing_borough"),
            _flag(
                F.col("complaint_type").isNull() | (F.trim(F.col("complaint_type")) == ""),
                "missing_complaint",
            ),
            _flag(zip_std.isNull(), "invalid_zip"),
        )
    )

    clean_df = (
        w.withColumn("complaint_raw", F.col("complaint_type"))
        .withColumn("complaint_type", complaint_std)
        .withColumn("descriptor", F.trim(F.col("descriptor")))
        .withColumn("agency", F.trim(F.col("agency")))
        .withColumn("status_snapshot", F.trim(F.col("status")))
        .withColumn("borough_raw", F.col("borough"))
        .withColumn("borough", borough_std)
        .withColumn("incident_zip_raw", F.col("incident_zip"))
        .withColumn("incident_zip", zip_std)
        .withColumn("latitude", F.when(coords_ok, lat))
        .withColumn("longitude", F.when(coords_ok, lon))
        .withColumn("resolution_hours", resolution)
        .withColumn("created_day", F.to_date(created))
        .withColumn("created_week", F.to_date(F.date_trunc("week", created)))
        .withColumn("weekday", F.date_format(created, "EEEE"))
        .withColumn("month", F.month(created))
        .withColumn("year", F.year(created))
        .withColumn("hour", F.hour(created))
        .withColumn("data_snapshot_date", F.to_date(F.lit(snap)))
        .withColumn("data_quality_flags", flags)
        .select(
            "unique_key", "created_at", "closed_at",
            "complaint_raw", "complaint_type",
            "descriptor", "agency", "status_snapshot",
            "borough_raw", "borough",
            "incident_zip_raw", "incident_zip",
            "latitude", "longitude",
            "resolution_hours",
            "created_day", "created_week", "weekday",
            "month", "year", "hour",
            "data_snapshot_date", "data_quality_flags",
        )
    )

    output_rows = clean_df.count()
    null_exprs = [
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in CLEAN_COLUMNS
        if c != "data_quality_flags"
    ]
    null_row = clean_df.agg(*null_exprs).collect()[0].asDict()
    null_counts = {k: int(v or 0) for k, v in null_row.items()}
    null_pct = {
        k: (v / output_rows if output_rows else 0.0) for k, v in null_counts.items()
    }
    invalid_created = clean_df.filter(
        F.array_contains(F.col("data_quality_flags"), "invalid_created")
    ).count()
    invalid_closed = clean_df.filter(
        F.array_contains(F.col("data_quality_flags"), "invalid_closed")
    ).count()
    neg_dur = clean_df.filter(
        F.array_contains(F.col("data_quality_flags"), "negative_duration")
    ).count()
    invalid_coords = clean_df.filter(
        F.array_contains(F.col("data_quality_flags"), "invalid_coords")
    ).count()
    unresolved = clean_df.filter(
        F.array_contains(F.col("data_quality_flags"), "unresolved")
    ).count()
    flagged = clean_df.filter(F.size(F.col("data_quality_flags")) > 0).count()
    flag_rows = (
        clean_df.select(F.explode("data_quality_flags").alias("flag"))
        .groupBy("flag")
        .count()
        .collect()
    )
    flag_breakdown = {r["flag"]: int(r["count"]) for r in flag_rows}

    metrics: dict[str, Any] = {
        "input_rows": int(input_rows),
        "rows_excluded_missing_key": int(missing_key),
        "duplicates_removed": int(duplicates_removed),
        "output_rows": int(output_rows),
        "null_counts": null_counts,
        "null_percentages": null_pct,
        "invalid_created": int(invalid_created),
        "invalid_closed": int(invalid_closed),
        "closed_before_created": int(neg_dur),
        "invalid_coords": int(invalid_coords),
        "unresolved": int(unresolved),
        "flagged_rows": int(flagged),
        "clean_rows": int(output_rows - flagged),
        "flag_breakdown": flag_breakdown,
        "snapshot_date": snap,
    }
    return clean_df, metrics


def write_clean(df: DataFrame, path: str) -> None:
    """Overwrite partitioned Parquet (year/month). Idempotent."""
    (
        df.write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(path)
    )


def read_raw(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.schema(RAW_SCHEMA)
        .option("header", "true")
        .csv(path)
    )
