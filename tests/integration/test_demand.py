"""Real Spark chronology and metric tests on explicitly synthetic daily counts."""

from datetime import date, timedelta

import pytest
from pyspark.sql import functions as F

from src.modeling.demand import (
    NUMERIC,
    build_features,
    error_metrics,
    evaluation_rows,
    pipeline,
    predict,
    training_rows,
)
from src.utils.spark import get_spark


@pytest.fixture(scope="module")
def spark():
    session = get_spark("demand-tests")
    yield session
    session.stop()


def test_features_stop_at_origin_and_baselines_are_observable(spark):
    start = date(2024, 1, 1)
    rows = [(start + timedelta(days=i), "QUEENS", i) for i in range(70)]
    daily = spark.createDataFrame(rows, "created_day date, borough string, count_0 bigint")
    frame = build_features(daily).localCheckpoint(eager=True)
    origin = start + timedelta(days=35)
    selected = frame.filter(F.col("forecast_origin") == F.lit(origin)).orderBy("horizon_days").collect()
    assert len(selected) == 7
    for row in selected:
        h = row["horizon_days"]
        assert row["seasonal_naive"] == 35 + h - 7
        assert row["weekday_mean"] == sum(35 + h - d for d in (7, 14, 21, 28)) / 4
        assert row["lag_28"] == 7 and row["mean_7"] == 32
    altered = daily.withColumn("count_0", F.when(F.col("created_day") > F.lit(origin), 999999)
                               .otherwise(F.col("count_0")))
    changed = build_features(altered).filter(F.col("forecast_origin") == F.lit(origin))
    assert frame.filter(F.col("forecast_origin") == F.lit(origin)).select(*NUMERIC).exceptAll(
        changed.select(*NUMERIC)).count() == 0
    validation = evaluation_rows(frame, "2024-02-10", "2024-03-01")
    assert validation.filter(F.col("forecast_origin") < F.lit("2024-02-10").cast("date")).count() == 0
    assert training_rows(frame, "2024-02-10").agg(F.max("target_date")).first()[0] <= date(2024, 2, 10)


def test_zero_actual_wape_is_null_and_models_are_mllib(spark):
    frame = spark.createDataFrame([("x", 0, 2.0), ("x", 0, 0.0)],
                                  "model string, actual_count bigint, predicted_count double")
    metrics = error_metrics(frame, ["model"]).first()
    assert metrics["mae"] == 1 and metrics["wape"] is None
    for name in ("lr_scaled", "lr_unscaled", "random_forest", "gbt"):
        assert pipeline(name, 42).getStages()[-1].getLabelCol() == "label"
    invalid = spark.createDataFrame([(float("nan"),), (None,)], "seasonal_naive double")
    with pytest.raises(ValueError, match="nonfinite"):
        predict(invalid, "seasonal_naive", None, "2024-01-01", "future")
