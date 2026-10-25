"""Small aggregates for priority scoring, built from cleaned Parquet. ASCII only.

The cleaned record-level table (docs/data_dictionary.md) is reduced in Spark
to three small tables before any pandas work:

  created  day (created_day), location, complaint_type,
           close_lag_days = days from created_day to the closed_at date,
           clipped to [0, lag_cap_days], null when closed_at is null
           (open at acquisition snapshot), n
  delay    closed_day (closed_at date), location, complaint_type,
           hours_bin (scoring.HOUR_EDGES bin of resolution_hours), n;
           resolved records only (resolution_hours not null)
  coords   location, center_lat, center_lon, n_coords
           (mean of valid in-bbox coordinates; cleaned invalid ones are null)

Feature timing: a record created on day d with close lag L is open at
cut-off T iff L is null or d + L >= T. Clipping L at lag_cap_days preserves
that predicate for every T within lag_cap_days of d, which covers every
scoring window (window_days <= lag_cap_days is enforced).
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from src.prioritization.scoring import HOUR_EDGES, hours_bin

LOCATION_COLUMNS = {"borough": "borough", "zip": "incident_zip"}
SOURCE_COLUMNS = [
    "created_at",
    "closed_at",
    "created_day",
    "complaint_type",
    "borough",
    "incident_zip",
    "latitude",
    "longitude",
    "resolution_hours",
]


def _loc_col(location_type: str) -> str:
    if location_type not in LOCATION_COLUMNS:
        raise ValueError(f"location_type must be one of {sorted(LOCATION_COLUMNS)}")
    return LOCATION_COLUMNS[location_type]


def aggregate_pandas(
    records: pd.DataFrame, location_type: str = "borough", lag_cap_days: int = 60
) -> dict[str, Any]:
    """Reference implementation on a small record-level frame (tests only)."""
    col = _loc_col(location_type)
    r = records.copy()
    r = r[r["created_day"].notna()]
    total = len(r)
    r = r[r[col].notna()]
    r = r.assign(location=r[col].astype(str), day=pd.to_datetime(r["created_day"]).dt.date)
    closed_day = pd.to_datetime(r["closed_at"]).dt.normalize()
    lag = (closed_day - pd.to_datetime(r["day"])).dt.days
    r["close_lag_days"] = lag.clip(lower=0, upper=lag_cap_days).astype("Int64")
    created = (
        r.groupby(["day", "location", "complaint_type", "close_lag_days"], dropna=False)
        .size()
        .reset_index(name="n")
    )
    res = r[r["resolution_hours"].notna()].copy()
    res["closed_day"] = pd.to_datetime(res["closed_at"]).dt.date
    res["hours_bin"] = hours_bin(res["resolution_hours"])
    delay = (
        res.groupby(["closed_day", "location", "complaint_type", "hours_bin"])
        .size()
        .reset_index(name="n")
    )
    geo = r[r["latitude"].notna() & r["longitude"].notna()]
    coords = (
        geo.groupby("location")
        .agg(center_lat=("latitude", "mean"), center_lon=("longitude", "mean"),
             n_coords=("latitude", "size"))
        .reset_index()
    )
    return {
        "created": created,
        "delay": delay,
        "coords": coords,
        "records": total,
        "records_without_location": total - len(r),
    }


def _bin_sql(column: str = "resolution_hours") -> str:
    inner = HOUR_EDGES[1:-1]
    return " + ".join(f"CASE WHEN {column} >= {e!r} THEN 1 ELSE 0 END" for e in inner)


def aggregate_spark(
    spark, input_path: str, location_type: str = "borough", lag_cap_days: int = 60
) -> dict[str, Any]:
    """Spark aggregation over cleaned Parquet; returns small pandas frames."""
    from pyspark.sql import functions as F

    col = _loc_col(location_type)
    df = spark.read.parquet(input_path).select(*SOURCE_COLUMNS)
    df = df.filter(F.col("created_day").isNotNull())
    total = df.count()
    df = df.filter(F.col(col).isNotNull()).withColumn("location", F.col(col).cast("string"))
    df = df.cache()
    with_loc = df.count()
    lag = F.datediff(F.to_date("closed_at"), F.col("created_day"))
    lag = F.when(F.col("closed_at").isNull(), F.lit(None)).otherwise(
        F.least(F.greatest(lag, F.lit(0)), F.lit(int(lag_cap_days)))
    )
    created = (
        df.withColumn("close_lag_days", lag)
        .groupBy(F.col("created_day").alias("day"), "location", "complaint_type", "close_lag_days")
        .agg(F.count(F.lit(1)).alias("n"))
        .orderBy("day", "location", "complaint_type", "close_lag_days")
        .toPandas()
    )
    delay = (
        df.filter(F.col("resolution_hours").isNotNull())
        .withColumn("closed_day", F.to_date("closed_at"))
        .withColumn("hours_bin", F.expr(_bin_sql()))
        .groupBy("closed_day", "location", "complaint_type", "hours_bin")
        .agg(F.count(F.lit(1)).alias("n"))
        .orderBy("closed_day", "location", "complaint_type", "hours_bin")
        .toPandas()
    )
    coords = (
        df.filter(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
        .groupBy("location")
        .agg(
            F.avg("latitude").alias("center_lat"),
            F.avg("longitude").alias("center_lon"),
            F.count(F.lit(1)).alias("n_coords"),
        )
        .orderBy("location")
        .toPandas()
    )
    df.unpersist()
    created["close_lag_days"] = created["close_lag_days"].astype("Int64")
    return {
        "created": created,
        "delay": delay,
        "coords": coords,
        "records": int(total),
        "records_without_location": int(total - with_loc),
    }


def daily_totals(created: pd.DataFrame) -> pd.Series:
    """Complaints created per day (all units), indexed by date."""
    s = created.assign(day=pd.to_datetime(created["day"]).dt.date).groupby("day")["n"].sum()
    return s.sort_index()


def trim_partial_tail(totals: pd.Series, ratio: float = 0.5) -> tuple[pd.Series, list]:
    """Drop trailing days whose volume is below ratio x median daily volume.

    A short final day (acquisition cut-off, e.g. 2024-01-12 with 191 records in
    the sample) would otherwise look like a collapse in demand.
    """
    dropped = []
    s = totals.copy()
    med = float(s.median()) if len(s) else 0.0
    while len(s) > 1 and float(s.iloc[-1]) < ratio * med:
        dropped.append(s.index[-1])
        s = s.iloc[:-1]
    return s, dropped
