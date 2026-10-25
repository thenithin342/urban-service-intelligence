"""Member 1 forecast handoff; no priority scores, weights or recommendations."""

from pyspark.sql import functions as F

from src.analytics.geospatial import _cell_columns, filter_valid_coords


def build_handoff(daily, future, raw, coverage):
    end = coverage["end"]
    snapshots = coverage.get("snapshot_dates") or []
    bad = future.filter((F.col("forecast_origin") != F.lit(end).cast("date"))
                        | F.col("actual_count").isNotNull()
                        | (F.col("horizon_days") < 1) | (F.col("horizon_days") > 7)
                        | (F.col("target_date") != F.expr("date_add(forecast_origin, horizon_days)"))
                        | F.col("predicted_count").isNull() | F.isnan("predicted_count")
                        | (F.abs("predicted_count") == float("inf")) | (F.col("predicted_count") < 0))
    if bad.limit(1).count() or future.select("model_version").distinct().count() != 1:
        raise ValueError("handoff requires one valid unobserved forecast version at observed end")
    groups = future.groupBy("borough").agg(F.count("horizon_days").alias("n"),
                                           F.countDistinct("horizon_days").alias("h"))
    if groups.filter((F.col("n") != 7) | (F.col("h") != 7)).limit(1).count():
        raise ValueError("each borough requires exactly seven distinct forecast horizons")
    expected = daily.select("borough").distinct()
    if (groups.count() != expected.count()
            or groups.select("borough").exceptAll(expected).limit(1).count()):
        raise ValueError("forecast and observed borough sets differ")
    if raw.filter(F.col("created_day") > F.lit(end).cast("date")).limit(1).count():
        raise ValueError("spatial input extends beyond the forecast origin")
    if raw.count() != daily.agg(F.sum("count_0")).first()[0]:
        raise ValueError("spatial and demand source populations differ")
    totals = future.groupBy("borough", "forecast_origin", "model_version").agg(
        F.sum("predicted_count").alias("forecast_7d"))
    trailing = daily.filter((F.col("created_day") > F.date_sub(F.lit(end).cast("date"), 7))
                            & (F.col("created_day") <= F.lit(end).cast("date")))
    trailing = trailing.groupBy("borough").agg(F.sum("count_0").alias("trailing_7d"))
    grid = _cell_columns(0.01)
    spatial = (filter_valid_coords(raw)
               .select(F.coalesce("borough", F.lit("UNKNOWN")).alias("borough"), grid["cell_id"])
               .groupBy("borough").agg(F.count("cell_id").alias("valid_geo_count"),
                                       F.countDistinct("cell_id").alias("active_grid_cells")))
    return (totals.join(trailing, "borough", "left").join(spatial, "borough", "left")
            .fillna(0, subset=["valid_geo_count", "active_grid_cells"])
            .withColumn("absolute_change", F.col("forecast_7d") - F.col("trailing_7d"))
            .withColumn("percent_change", F.when(F.col("trailing_7d") > 0,
                        F.col("absolute_change") / F.col("trailing_7d") * 100.0))
            .withColumn("baseline_zero", F.col("trailing_7d") == 0)
            .withColumn("complaints_per_active_cell", F.col("valid_geo_count") /
                        F.when(F.col("active_grid_cells") > 0, F.col("active_grid_cells")))
            .withColumn("spatial_period_start", F.lit(coverage["start"]).cast("date"))
            .withColumn("spatial_period_end", F.lit(end).cast("date"))
            .withColumn("source_snapshot_max", F.lit(max(snapshots) if snapshots else None).cast("date"))
            .withColumn("complaint_type", F.lit("ALL"))
            .withColumn("horizon_days", F.lit(7))
            .withColumn("uncertainty_method", F.lit("not_calibrated")))
