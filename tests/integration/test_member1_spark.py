"""Member 1 Phase 3 Spark integration tests over a small synthetic fixture.

These exercise real Python-worker transformations and cross-check selected
Spark aggregations against an independently computed pandas reference on
the same records. The fixture is synthetic (explicitly not real data).
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pandas as pd
import pytest
from pyspark.sql.types import (
    ArrayType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.analytics import clustering, geospatial, spark_eda
from src.utils.spark import ensure_worker_env, get_spark

SCHEMA = StructType(
    [
        StructField("unique_key", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("closed_at", TimestampType(), True),
        StructField("complaint_type", StringType(), True),
        StructField("agency", StringType(), True),
        StructField("borough", StringType(), True),
        StructField("incident_zip", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("resolution_hours", DoubleType(), True),
        StructField("created_day", DateType(), True),
        StructField("created_week", DateType(), True),
        StructField("weekday", StringType(), True),
        StructField("month", IntegerType(), True),
        StructField("year", IntegerType(), True),
        StructField("hour", IntegerType(), True),
        StructField("data_snapshot_date", DateType(), True),
        StructField("data_quality_flags", ArrayType(StringType()), True),
    ]
)

WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]


def _rows() -> list[tuple]:
    rows: list[tuple] = []
    clusters = [(40.70, -73.95), (40.76, -73.90), (40.60, -74.00)]
    boroughs = ["BROOKLYN", "QUEENS", "BRONX"]
    cats = ["Noise - Residential", "Water System", "Street Light Condition", "Rodent"]
    key = 0
    for ci, (clat, clon) in enumerate(clusters):
        for j in range(8):
            key += 1
            day = date(2024, 1, 1) + timedelta(days=j % 5)
            ts = datetime(2024, 1, 1, (7 + j) % 24, 0, 0)
            created = datetime.combine(day, ts.time())
            closed = created + timedelta(hours=2 * (j + 1))
            flags: list[str] = []
            if j == 0:
                flags = ["invalid_zip", "invalid_coords"]
            rows.append(
                (
                    str(key),
                    created,
                    closed,
                    cats[(ci + j) % len(cats)],
                    "DSNY",
                    boroughs[ci % len(boroughs)],
                    f"1120{j}",
                    clat + 0.02 * j,
                    clon + 0.02 * j,
                    float(2 * (j + 1)),
                    day,
                    day - timedelta(days=day.weekday()),
                    WEEKDAYS[day.weekday()],
                    1,
                    2024,
                    (7 + j) % 24,
                    date(2024, 1, 13),
                    flags,
                )
            )
    # rows with missing/null fields for null handling
    rows.append(
        (
            "900",
            datetime(2024, 1, 2, 5, 0, 0),
            None,
            "UNKNOWN",
            "HPD",
            None,
            None,
            None,
            None,
            None,
            date(2024, 1, 2),
            date(2024, 1, 1),
            "Tuesday",
            1,
            2024,
            5,
            date(2024, 1, 13),
            ["missing_borough", "invalid_coords", "invalid_zip", "unresolved"],
        )
    )
    rows.append(
        (
            "901",
            datetime(2024, 1, 3, 9, 0, 0),
            datetime(2024, 1, 3, 11, 0, 0),
            "Noise - Residential",
            "DEP",
            "BROOKLYN",
            "11201",
            40.71,
            -73.96,
            2.0,
            date(2024, 1, 3),
            date(2024, 1, 1),
            "Wednesday",
            1,
            2024,
            9,
            date(2024, 1, 13),
            [],
        )
    )
    return rows


@pytest.fixture(scope="module")
def spark():
    ensure_worker_env()
    session = get_spark("member1-it", master="local[2]")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def fixture_df(spark):
    return spark.createDataFrame(_rows(), schema=SCHEMA)


def _pandas() -> pd.DataFrame:
    cols = [f.name for f in SCHEMA.fields]
    return pd.DataFrame(_rows(), columns=cols)


def test_verify_inputs_reconciles(spark, fixture_df):
    v = spark_eda.verify_inputs(fixture_df)
    pdf = _pandas()
    assert v["row_count"] == len(pdf)
    assert v["distinct_unique_key"] == pdf["unique_key"].nunique()
    assert sum(v["flag_row_histogram"].values()) == v["row_count"]
    assert v["flagged_rows"] == v["row_count"] - v["clean_rows"]
    assert v["valid_coords_rows"] + v["invalid_coords_rows"] == v["row_count"]


def test_flag_overlap_pairs(spark, fixture_df):
    pairs = {
        (r["flag_a"], r["flag_b"]): r["rows"]
        for r in spark_eda.flag_overlap(fixture_df).collect()
    }
    assert pairs.get(("invalid_coords", "invalid_zip")) == 4
    assert pairs.get(("missing_borough", "unresolved")) == 1
    assert pairs.get(("invalid_coords", "unresolved")) == 1


def test_borough_counts_match_pandas(spark, fixture_df):
    got = {
        (r["borough"] if r["borough"] is not None else None): r["complaint_count"]
        for r in spark_eda.complaints_by_borough(fixture_df).collect()
    }
    ref = _pandas()["borough"].value_counts(dropna=False)
    ref = {(None if pd.isna(k) else k): int(v) for k, v in ref.items()}
    for k, v in ref.items():
        assert got.get(k) == v


def test_category_counts_match_pandas(spark, fixture_df):
    got = {
        r["complaint_type"]: r["complaint_count"]
        for r in spark_eda.complaints_by_category(fixture_df).collect()
    }
    ref = _pandas()["complaint_type"].value_counts().to_dict()
    assert got == {k: int(v) for k, v in ref.items()}


def test_zip_preserved_as_text(spark, fixture_df):
    zips = {
        r["incident_zip"]: r["complaint_count"]
        for r in spark_eda.complaints_by_zip(fixture_df).collect()
    }
    assert all(isinstance(z, str) for z in zips)
    assert zips["11201"] == 4


def test_invalid_coords_excluded_from_spatial_only(spark, fixture_df):
    v = spark_eda.verify_inputs(fixture_df)
    hot = geospatial.spatial_hotspots(fixture_df, cell_deg=0.05, min_count=1)
    assert sum(r["complaint_count"] for r in hot.collect()) == v["valid_coords_rows"]
    assert v["valid_coords_rows"] < v["row_count"]
    # non-spatial counts still include the missing-coordinate row
    assert spark_eda.complaints_by_category(fixture_df).count() == 5


def test_time_coverage_daily(spark, fixture_df):
    cov = spark_eda.time_coverage(fixture_df)
    assert cov["period_start"] == "2024-01-01"
    assert cov["period_end"] == "2024-01-05"
    assert cov["observed_days"] == 5
    assert cov["coverage_ratio"] == 1.0


def test_deterministic_sample_is_stable(spark, fixture_df):
    a = geospatial.deterministic_sample(fixture_df, 5).select("unique_key").collect()
    b = geospatial.deterministic_sample(fixture_df, 5).select("unique_key").collect()
    assert [r["unique_key"] for r in a] == [r["unique_key"] for r in b]


def test_kmeans_and_summary(spark, fixture_df):
    cells = geospatial.cell_features(fixture_df, 0.01, 40.7, -73.95, 1)
    n = cells.count()
    assert n >= 3
    frame, feature_col = clustering.build_variant(cells, use_log=True, scale=True)
    preds, metrics = clustering.select_k(frame, feature_col, [2, 3], seed=42)
    assert {m["k"] for m in metrics} == {2, 3}
    assert all(-1.0 <= m["silhouette"] <= 1.0 for m in metrics)
    summary = clustering.cluster_summary(preds, cells, 40.7, -73.95)
    assert summary["cells"].sum() == n


def test_preprocessing_ablation_runs(spark, fixture_df):
    cells = geospatial.cell_features(fixture_df, 0.01, 40.7, -73.95, 1)
    out = clustering.preprocessing_ablation(cells, [2, 3], seed=42)
    variants = {row["variant"] for row in out}
    assert variants == {"log_count_scaled", "log_count_unscaled", "count_scaled"}
    assert all(row["silhouette"] is not None for row in out)


def test_dbscan_on_synthetic_clusters():
    pts = pd.DataFrame(
        {
            "latitude": [40.70, 40.701, 40.7005, 40.80, 40.8005, 40.801],
            "longitude": [-73.95, -73.9505, -73.9502, -73.90, -73.9003, -73.9001],
        }
    )
    labelled, summary = geospatial.dbscan_clusters(pts, eps_meters=500.0, min_pts=2)
    assert set(labelled["cluster"]) == {0, 1}
    assert summary["n_clusters_excluding_noise"] == 2
    assert summary["noise_points"] == 0


def test_hour_weekday_heatmap_normalized(spark, fixture_df):
    hm = spark_eda.hour_weekday_heatmap(fixture_df).collect()
    assert len(hm) > 0
    first = hm[0]
    assert "complaint_count" in first
    assert "days_observed" in first
    assert "avg_complaints_per_hour" in first
    for r in hm:
        assert r["days_observed"] >= 1
        assert r["avg_complaints_per_hour"] == round(r["complaint_count"] / r["days_observed"], 2)


def test_spark_resolution_histogram_and_box_stats(spark, fixture_df):
    hist_sdf, summary = spark_eda.resolution_histogram_spark(fixture_df, n_bins=10)
    hist_rows = hist_sdf.collect()
    assert len(hist_rows) <= 10
    total_binned = sum(r["count"] for r in hist_rows)
    assert total_binned == summary["resolved_rows"]
    assert "median_hours" in summary
    assert "p90_hours" in summary
    assert "p99_hours" in summary

    box_sdf = spark_eda.resolution_box_stats_spark(fixture_df)
    box_rows = box_sdf.collect()
    assert len(box_rows) >= 1
    for r in box_rows:
        assert r["resolved_rows"] >= 1
        assert r["min_hours"] <= r["q1"] <= r["med"] <= r["q3"] <= r["max_hours"]


def test_time_coverage_expanded_metrics(spark, fixture_df):
    cov = spark_eda.time_coverage(fixture_df)
    assert cov["period_start"] == "2024-01-01"
    assert cov["period_end"] == "2024-01-05"
    assert cov["observed_span_days"] == 5
    assert cov["observed_span_completeness"] == 1.0
    assert "month_coverage_ratio" in cov
    assert "full_days_count" in cov
    assert "partial_days_count" in cov
    assert cov["target_month_days"] == 31  # January 2024 dynamically
    assert cov["target_calendar_month"] == "2024-01"
    assert cov["month_coverage_ratio"] == round(5 / 31, 4)


def test_time_coverage_dynamic_arbitrary_span(spark):
    # Test arbitrary month: February 2024 (leap year = 29 days)
    feb_rows = [
        (date(2024, 2, 1), datetime(2024, 2, 1, 0, 0, 0), 0),
        (date(2024, 2, 10), datetime(2024, 2, 10, 23, 0, 0), 23),
    ]
    feb_df = spark.createDataFrame(feb_rows, ["created_day", "created_at", "hour"])
    feb_cov = spark_eda.time_coverage(feb_df)
    assert feb_cov["target_month_days"] == 29
    assert feb_cov["target_calendar_month"] == "2024-02"

    # Test multi-month span: 2024-01-15 to 2024-02-15 (31 + 29 = 60 days)
    span_rows = [
        (date(2024, 1, 15), datetime(2024, 1, 15, 0, 0, 0), 0),
        (date(2024, 2, 15), datetime(2024, 2, 15, 23, 0, 0), 23),
    ]
    span_df = spark.createDataFrame(span_rows, ["created_day", "created_at", "hour"])
    span_cov = spark_eda.time_coverage(span_df)
    assert span_cov["target_month_days"] == 60
    assert span_cov["target_calendar_month"] == "2024-01, 2024-02"


def test_dominant_deterministic_with_shuffled_partitions(spark):
    # Create deliberate ties in counts:
    # cell 1: 'Noise' (2 records), 'Heat' (2 records) -> 'Heat' < 'Noise' alphabetically, so 'Heat' must win
    # cell 2: 'Water' (3 records), 'Illegal Parking' (1 record) -> 'Water' wins on count
    raw = [
        ("cell_1", "Noise"),
        ("cell_1", "Noise"),
        ("cell_1", "Heat"),
        ("cell_1", "Heat"),
        ("cell_2", "Water"),
        ("cell_2", "Water"),
        ("cell_2", "Water"),
        ("cell_2", "Illegal Parking"),
    ]
    df = spark.createDataFrame(raw, ["spatial_id", "complaint_type"])

    # Test with multiple shuffle partition counts and explicit random repartitions
    for partitions in [1, 3, 7]:
        spark.conf.set("spark.sql.shuffle.partitions", str(partitions))
        shuffled = df.repartition(partitions)
        dom = geospatial._dominant(shuffled, "spatial_id", "complaint_type", "dominant_complaint")
        res = {r["spatial_id"]: r["dominant_complaint"] for r in dom.collect()}
        assert res["cell_1"] == "Heat", f"Alphabetical tie-break failed on {partitions} partitions"
        assert res["cell_2"] == "Water", f"Count priority failed on {partitions} partitions"

    # Test _dominant_by_cluster with shuffled partitions and volume weighting
    cluster_raw = [
        (0, "QUEENS", 10),
        (0, "BROOKLYN", 10),  # tie in cell count, BROOKLYN < QUEENS
        (1, "STATEN ISLAND", 5),
    ]
    c_df = spark.createDataFrame(cluster_raw, ["cluster", "borough", "complaint_count"])
    for partitions in [1, 4, 7]:
        spark.conf.set("spark.sql.shuffle.partitions", str(partitions))
        c_shuffled = c_df.repartition(partitions)
        dom_c = clustering._dominant_by_cluster(c_shuffled, "borough", "dominant_borough")
        assert dom_c[0] == "BROOKLYN"
        assert dom_c[1] == "STATEN ISLAND"

