"""Member 2 closure-within-7-days labels, summaries and models. ASCII only.

Label (docs/member2/semantics.md):
- prediction time = ``created_at``;
- ``closed_within_7d`` = 1 when ``closed_at`` is not null, ``closed_at >=
  created_at`` and ``closed_at - created_at <= horizon``; 0 otherwise;
- negative durations (``closed_at < created_at``) are excluded and counted;
- censoring: a record is label-eligible only when the snapshot leaves at
  least ``horizon`` full days of observation after its created day
  (``data_snapshot_date - created_day >= horizon_days + 1``); younger
  records are excluded (never labelled 0);
- missing ``created_at`` is excluded.

Durations use epoch-second differences, so they do not depend on the
session time zone. Calendar keys use the ETL's wall-clock ``created_day``.
"""

from __future__ import annotations

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

ELIGIBLE = "eligible"
EXCL_MISSING_CREATED = "excluded_missing_created"
EXCL_NEGATIVE = "excluded_negative_duration"
EXCL_IMMATURE = "excluded_immature"
LABEL_STATUSES = (ELIGIBLE, EXCL_MISSING_CREATED, EXCL_NEGATIVE, EXCL_IMMATURE)

# Columns that describe the outcome or the acquisition snapshot. They may be
# used to build labels and strictly-historical aggregates, never as features
# of the record being predicted.
FORBIDDEN_FEATURES = frozenset(
    {
        "closed_at",
        "closed_date",
        "status_snapshot",
        "status",
        "resolution_hours",
        "data_snapshot_date",
        "data_quality_flags",
        "closed_within_7d",
        "label_status",
        "duration_hours",
    }
)


def add_closure_label_spark(df: DataFrame, horizon_days: int = 7) -> DataFrame:
    """Append ``duration_hours``, ``label_status`` and ``closed_within_7d``."""
    dur = (F.col("closed_at").cast("long") - F.col("created_at").cast("long")) / 3600.0
    mature = F.datediff(F.col("data_snapshot_date"), F.col("created_day")) >= horizon_days + 1
    status = (
        F.when(F.col("created_at").isNull() | F.col("created_day").isNull(),
               EXCL_MISSING_CREATED)
        .when(F.col("closed_at").isNotNull() & (dur < 0), EXCL_NEGATIVE)
        .when(~mature, EXCL_IMMATURE)
        .otherwise(ELIGIBLE)
    )
    label = F.when(
        status == ELIGIBLE,
        (F.col("closed_at").isNotNull() & (dur >= 0) & (dur <= horizon_days * 24.0)).cast(
            "int"),
    )
    return (
        df.withColumn("duration_hours", dur)
        .withColumn("label_status", status)
        .withColumn("closed_within_7d", label)
    )


def add_closure_label_pandas(pdf: pd.DataFrame, horizon_days: int = 7) -> pd.DataFrame:
    """Independent pandas implementation of ``add_closure_label_spark``."""
    out = pdf.copy()
    dur = (out["closed_at"] - out["created_at"]).dt.total_seconds() / 3600.0
    created_day = pd.to_datetime(out["created_day"])
    snap = pd.to_datetime(out["data_snapshot_date"])
    mature = (snap - created_day).dt.days >= horizon_days + 1
    status = pd.Series(ELIGIBLE, index=out.index, dtype=object)
    status[~mature.fillna(False)] = EXCL_IMMATURE
    status[out["closed_at"].notna() & (dur < 0)] = EXCL_NEGATIVE
    status[out["created_at"].isna() | out["created_day"].isna()] = EXCL_MISSING_CREATED
    positive = out["closed_at"].notna() & (dur >= 0) & (dur <= horizon_days * 24.0)
    label = positive.astype("Int64").where(status == ELIGIBLE)
    out["duration_hours"] = dur
    out["label_status"] = status
    out["closed_within_7d"] = label
    return out


def label_summary_spark(labelled: DataFrame) -> pd.DataFrame:
    rows = (
        labelled.groupBy("label_status")
        .agg(F.count(F.lit(1)).alias("rows"),
             F.sum(F.coalesce("closed_within_7d", F.lit(0))).alias("positives"))
        .orderBy("label_status")
        .collect()
    )
    out = pd.DataFrame([r.asDict() for r in rows], columns=["label_status", "rows", "positives"])
    return out.astype({"rows": "int64", "positives": "int64"})


def label_summary_pandas(labelled: pd.DataFrame) -> pd.DataFrame:
    frame = pd.DataFrame({"label_status": labelled["label_status"],
                          "pos": labelled["closed_within_7d"].fillna(0).astype("int64")})
    out = frame.groupby("label_status").agg(rows=("pos", "size"), positives=("pos", "sum"))
    out = out.reset_index().sort_values("label_status", ignore_index=True)
    return out.astype({"rows": "int64", "positives": "int64"})


def resolution_summary_spark(labelled: DataFrame) -> DataFrame:
    """Contract ``resolution_summary``: borough x complaint_type closure stats.

    ``median``/``p90_resolution_hours`` are exact interpolated percentiles
    over the cleaned ``resolution_hours`` (valid closures only);
    ``unresolved_count`` counts records without ``closed_at`` at snapshot.
    """
    elig = F.col("label_status") == ELIGIBLE
    cov = labelled.agg(F.min("created_day").alias("lo"), F.max("created_day").alias("hi"))
    grouped = labelled.groupBy("borough", "complaint_type").agg(
        F.count(F.lit(1)).cast("long").alias("total_count"),
        F.sum(elig.cast("long")).alias("eligible_count"),
        F.sum(F.when(elig, F.col("closed_within_7d")).cast("long")).alias("closed_7d_count"),
        F.expr("percentile(resolution_hours, 0.5)").alias("median_resolution_hours"),
        F.expr("percentile(resolution_hours, 0.9)").alias("p90_resolution_hours"),
        F.sum(F.col("closed_at").isNull().cast("long")).alias("unresolved_count"),
    )
    return (
        grouped.crossJoin(cov)
        .select(
            "borough",
            "complaint_type",
            "total_count",
            "eligible_count",
            F.coalesce("closed_7d_count", F.lit(0)).cast("long").alias("closed_7d_count"),
            F.when(F.col("eligible_count") > 0,
                   F.coalesce("closed_7d_count", F.lit(0)) / F.col("eligible_count"))
            .alias("closure_rate_7d"),
            "median_resolution_hours",
            "p90_resolution_hours",
            "unresolved_count",
            F.col("lo").alias("coverage_start"),
            F.col("hi").alias("coverage_end"),
        )
        .orderBy(F.col("total_count").desc(), F.col("borough").asc_nulls_last(),
                 "complaint_type")
    )


def resolution_summary_pandas(labelled: pd.DataFrame) -> pd.DataFrame:
    """Independent pandas reference for ``resolution_summary_spark``."""
    elig = labelled["label_status"] == ELIGIBLE
    frame = pd.DataFrame(
        {
            "borough": labelled["borough"],
            "complaint_type": labelled["complaint_type"],
            "elig": elig.astype("int64"),
            "pos": labelled["closed_within_7d"].where(elig).fillna(0).astype("int64"),
            "rh": labelled["resolution_hours"],
            "unres": labelled["closed_at"].isna().astype("int64"),
        }
    )
    g = frame.groupby(["borough", "complaint_type"], dropna=False)
    out = g.agg(
        total_count=("elig", "size"),
        eligible_count=("elig", "sum"),
        closed_7d_count=("pos", "sum"),
        median_resolution_hours=("rh", "median"),
        p90_resolution_hours=("rh", lambda s: s.quantile(0.9)),
        unresolved_count=("unres", "sum"),
    ).reset_index()
    out["closure_rate_7d"] = (out["closed_7d_count"] / out["eligible_count"]).where(
        out["eligible_count"] > 0)
    days = pd.to_datetime(labelled["created_day"])
    out["coverage_start"] = days.min().date()
    out["coverage_end"] = days.max().date()
    out = out[
        ["borough", "complaint_type", "total_count", "eligible_count", "closed_7d_count",
         "closure_rate_7d", "median_resolution_hours", "p90_resolution_hours",
         "unresolved_count", "coverage_start", "coverage_end"]
    ]
    out = out.astype({c: "int64" for c in ("total_count", "eligible_count",
                                           "closed_7d_count", "unresolved_count")})
    out["_b"] = out["borough"].isna()
    out = out.sort_values(["total_count", "_b", "borough", "complaint_type"],
                          ascending=[False, True, True, True], ignore_index=True)
    return out.drop(columns="_b")
