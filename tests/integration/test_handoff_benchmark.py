"""Real Spark checks on small synthetic handoff and benchmark fixtures."""

import csv
from datetime import date, timedelta

import pytest
from pyspark.sql import functions as F

from src.analytics.priority_features import build_handoff
from src.evaluation.benchmark import pandas_task, read_counts, spark_task
from src.ingestion.downloader import DEFAULT_FIELDS
from src.utils.spark import get_spark


@pytest.fixture(scope="module")
def spark():
    session = get_spark("handoff-benchmark-tests")
    yield session
    session.stop()


def test_handoff_zero_denominator_and_geographic_scope(spark):
    end = date(2024, 1, 7)
    daily = spark.createDataFrame([(end - timedelta(days=i), b, 10 if b == "QUEENS" else 0)
                                   for b in ("QUEENS", "UNKNOWN") for i in range(7)],
                                  "created_day date, borough string, count_0 bigint")
    raw = spark.createDataFrame([(end, "QUEENS", 40.745, -73.945)] * 70,
                                "created_day date, borough string, latitude double, longitude double")
    future = spark.createDataFrame([(end, end + timedelta(days=h), b, h, None,
                                    12.0 if b == "QUEENS" else 0.0, "v1")
                                   for b in ("QUEENS", "UNKNOWN") for h in range(1, 8)],
                                  "forecast_origin date, target_date date, borough string, horizon_days int, actual_count bigint, predicted_count double, model_version string")
    coverage = {"start": "2024-01-01", "end": "2024-01-07"}
    rows = {r["borough"]: r for r in build_handoff(daily, future, raw, coverage).collect()}
    assert rows["QUEENS"]["forecast_7d"] == 84 and rows["QUEENS"]["percent_change"] == 20
    assert rows["UNKNOWN"]["percent_change"] is None and rows["UNKNOWN"]["baseline_zero"]
    assert rows["QUEENS"]["valid_geo_count"] == 70
    assert rows["QUEENS"]["source_snapshot_max"] is None
    with pytest.raises(ValueError, match="unobserved"):
        build_handoff(daily, future.withColumn("actual_count", F.lit(1)), raw, coverage)
    for column in ("forecast_origin", "target_date", "model_version"):
        with pytest.raises(ValueError, match="unobserved"):
            build_handoff(daily, future.withColumn(column, F.lit(None).cast(future.schema[column].dataType)),
                          raw, coverage)


def test_benchmark_engines_match_on_quoted_missing_invalid_fields(spark, tmp_path):
    path = tmp_path / "input.csv"
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=DEFAULT_FIELDS)
        writer.writeheader()
        for borough, category, lat in [(" queens ", 'Noise "quoted"\nline', "40.7"),
                                       ("QUEENS", 'Noise "quoted"\r\nline', "40.7"),
                                       ("Unspecified", "", "40.8"), ("BRONX", "Noise", "99")]:
            writer.writerow({"borough": borough, "complaint_type": category, "latitude": lat,
                             "longitude": "-73.9"})
    pandas_task(path, tmp_path / "pandas.csv")
    spark_task(spark, path, tmp_path / "spark", 2)
    actual = read_counts(tmp_path / "spark")
    assert actual == read_counts(tmp_path / "pandas.csv")
    assert sum(actual.values()) == 3 and actual[("UNKNOWN", "UNKNOWN")] == 1
