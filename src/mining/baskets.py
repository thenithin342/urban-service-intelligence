"""Location-time transaction (basket) construction for FP-Growth. ASCII only.

Basket = distinct ``complaint_type`` values reported in one
``incident_zip`` during one calendar period (``created_week`` Monday-start
week, or ``created_day``). See docs/member2/semantics.md section 2.

Records with null ZIP or null period are excluded and counted. Items are
deduplicated and sorted, so a basket never contains an item twice and the
representation is deterministic.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

PERIOD_COLUMN = {"week": "created_week", "day": "created_day"}

BASKET_DEFINITIONS = {
    "week": "ZIP x ISO week: distinct complaint_type values per (incident_zip, created_week)",
    "day": "ZIP x day: distinct complaint_type values per (incident_zip, created_day)",
}


def basket_definition(granularity: str, drop_partial_weeks: bool = False) -> str:
    text = BASKET_DEFINITIONS[granularity]
    if granularity == "week" and drop_partial_weeks:
        text += "; complete Monday-Sunday weeks only"
    return text


def _complete_weeks_spark(df: DataFrame) -> DataFrame:
    """Weeks whose 7 calendar days all appear in the input."""
    return (
        df.filter(F.col("created_week").isNotNull())
        .groupBy("created_week")
        .agg(F.countDistinct("created_day").alias("days"))
        .filter(F.col("days") == 7)
        .select("created_week")
    )


def week_coverage_spark(df: DataFrame) -> list[dict[str, Any]]:
    rows = (
        df.filter(F.col("created_week").isNotNull())
        .groupBy("created_week")
        .agg(F.countDistinct("created_day").alias("days_observed"),
             F.count(F.lit(1)).alias("rows"))
        .orderBy("created_week")
        .collect()
    )
    return [{"created_week": str(r["created_week"]), "days_observed": int(r["days_observed"]),
             "rows": int(r["rows"])} for r in rows]


def build_baskets_spark(df: DataFrame, granularity: str = "week",
                        drop_partial_weeks: bool = False) -> tuple[DataFrame, dict[str, Any]]:
    """Return (baskets, stats). Baskets: incident_zip, period, items, n_items."""
    if granularity not in PERIOD_COLUMN:
        raise ValueError(f"unknown basket granularity {granularity}")
    period = PERIOD_COLUMN[granularity]
    total = df.count()
    null_zip = df.filter(F.col("incident_zip").isNull()).count()
    null_period = df.filter(F.col("incident_zip").isNotNull() & F.col(period).isNull()).count()
    usable = df.filter(F.col("incident_zip").isNotNull() & F.col(period).isNotNull())
    dropped_partial = 0
    if drop_partial_weeks:
        before = usable.count()
        usable = usable.join(F.broadcast(_complete_weeks_spark(df)), on="created_week",
                             how="inner")
        dropped_partial = before - usable.count()
    baskets = (
        usable.groupBy("incident_zip", F.col(period).alias("period"))
        .agg(F.array_sort(F.collect_set("complaint_type")).alias("items"))
        .withColumn("n_items", F.size("items"))
        .orderBy("period", "incident_zip")
    )
    agg = baskets.agg(
        F.count(F.lit(1)).alias("n"),
        F.mean("n_items").alias("mean_items"),
        F.expr("percentile(n_items, 0.5)").alias("median_items"),
        F.max("n_items").alias("max_items"),
        F.countDistinct("incident_zip").alias("zips"),
        F.countDistinct("period").alias("periods"),
    ).collect()[0]
    cov = usable.agg(F.min("created_day").alias("lo"), F.max("created_day").alias("hi")
                     ).collect()[0]
    stats = {
        "granularity": granularity,
        "basket_definition": basket_definition(granularity, drop_partial_weeks),
        "input_rows": int(total),
        "excluded_null_zip_rows": int(null_zip),
        "excluded_null_period_rows": int(null_period),
        "excluded_partial_week_rows": int(dropped_partial),
        "basket_rows": int(total - null_zip - null_period - dropped_partial),
        "n_baskets": int(agg["n"]),
        "distinct_zips": int(agg["zips"]),
        "distinct_periods": int(agg["periods"]),
        "mean_items": float(agg["mean_items"]) if agg["mean_items"] is not None else None,
        "median_items": float(agg["median_items"]) if agg["median_items"] is not None else None,
        "max_items": int(agg["max_items"]) if agg["max_items"] is not None else 0,
        "coverage_start": str(cov["lo"]),
        "coverage_end": str(cov["hi"]),
    }
    if granularity == "week":
        stats["weeks"] = week_coverage_spark(df)
    return baskets, stats


def build_baskets_pandas(pdf: pd.DataFrame, granularity: str = "week",
                         drop_partial_weeks: bool = False) -> pd.DataFrame:
    """Independent pandas reference: incident_zip, period, items, n_items."""
    period = PERIOD_COLUMN[granularity]
    usable = pdf[pdf["incident_zip"].notna() & pdf[period].notna()]
    if drop_partial_weeks:
        days = pdf.dropna(subset=["created_week"]).groupby("created_week")["created_day"].nunique()
        complete = set(days[days == 7].index)
        usable = usable[usable["created_week"].isin(complete)]
    grouped = usable.groupby(["incident_zip", period])["complaint_type"].agg(
        lambda s: sorted(set(s)))
    out = grouped.reset_index().rename(columns={period: "period", "complaint_type": "items"})
    out["n_items"] = out["items"].map(len).astype("int64")
    out["period"] = pd.to_datetime(out["period"]).dt.date
    return out.sort_values(["period", "incident_zip"], ignore_index=True)[
        ["incident_zip", "period", "items", "n_items"]]
