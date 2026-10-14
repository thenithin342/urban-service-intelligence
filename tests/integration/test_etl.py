"""Spark ETL integration tests on synthetic malformed records. ASCII only."""

from __future__ import annotations

from pyspark.sql import functions as F

from src.processing.clean import RAW_SCHEMA, clean, write_clean
from src.utils.spark import get_spark

ROWS = [
    # ok row (Brooklyn lowercase -> BROOKLYN)
    {
        "unique_key": "1", "created_date": "2024-01-02T10:00:00.000",
        "closed_date": "2024-01-03T10:00:00.000", "complaint_type": "Noise",
        "descriptor": "Loud", "agency": "NYPD", "status": "Closed",
        "borough": "Brooklyn", "incident_zip": "11201",
        "latitude": "40.69", "longitude": "-73.99",
    },
    # duplicate of 1, later created -> removed by dedupe
    {
        "unique_key": "1", "created_date": "2024-01-04T10:00:00.000",
        "closed_date": "2024-01-05T10:00:00.000", "complaint_type": "Noise",
        "descriptor": "Loud", "agency": "NYPD", "status": "Closed",
        "borough": "BROOKLYN", "incident_zip": "11201",
        "latitude": "40.69", "longitude": "-73.99",
    },
    # unresolved (null closed) -> resolution null, never zero
    {
        "unique_key": "2", "created_date": "2024-01-02T11:00:00.000",
        "closed_date": None, "complaint_type": "Heat",
        "descriptor": "No heat", "agency": "HPD", "status": "Open",
        "borough": "BRONX", "incident_zip": "10451",
        "latitude": "40.82", "longitude": "-73.91",
    },
    # closed before created -> flagged, resolution null
    {
        "unique_key": "3", "created_date": "2024-01-03T09:00:00.000",
        "closed_date": "2024-01-02T09:00:00.000", "complaint_type": "Parking",
        "descriptor": "Blocked", "agency": "NYPD", "status": "Closed",
        "borough": "QUEENS", "incident_zip": "11375",
        "latitude": "40.72", "longitude": "-73.86",
    },
    # invalid coords + Unspecified borough + bad zip + empty complaint
    {
        "unique_key": "4", "created_date": "2024-01-04T08:00:00.000",
        "closed_date": "2024-01-04T09:00:00.000", "complaint_type": "",
        "descriptor": "x", "agency": "DSNY", "status": "Closed",
        "borough": "Unspecified", "incident_zip": "ABC",
        "latitude": "41.50", "longitude": "-73.00",
    },
    # missing unique_key -> excluded and counted
    {
        "unique_key": None, "created_date": "2024-01-05T08:00:00.000",
        "closed_date": None, "complaint_type": "Noise",
        "descriptor": "x", "agency": "NYPD", "status": "Open",
        "borough": "QUEENS", "incident_zip": "11375",
        "latitude": "40.72", "longitude": "-73.86",
    },
    # garbage created_date -> invalid_created, derived date cols null
    {
        "unique_key": "6", "created_date": "not-a-date",
        "closed_date": None, "complaint_type": "Noise",
        "descriptor": "x", "agency": "NYPD", "status": "Open",
        "borough": "QUEENS", "incident_zip": "11375",
        "latitude": "40.72", "longitude": "-73.86",
    },
]


def _frame(spark):
    return spark.createDataFrame(ROWS, schema=RAW_SCHEMA)


def test_dedupe_and_counts():
    spark = get_spark("etl-dedupe", master="local[2]")
    try:
        df, m = clean(_frame(spark), snapshot="2024-02-01")
        assert m["input_rows"] == 7
        assert m["rows_excluded_missing_key"] == 1
        assert m["duplicates_removed"] == 1
        assert m["output_rows"] == 5
        keys = sorted(r["unique_key"] for r in df.select("unique_key").collect())
        assert keys == ["1", "2", "3", "4", "6"]
        # dedupe keeps earliest created
        kept = df.filter(F.col("unique_key") == "1").collect()[0]
        assert str(kept["created_at"]) == "2024-01-02 10:00:00"
    finally:
        spark.stop()


def test_resolution_never_fabricated():
    spark = get_spark("etl-resolution", master="local[2]")
    try:
        df, m = clean(_frame(spark), snapshot="2024-02-01")
        by_key = {r["unique_key"]: r.asDict() for r in df.collect()}
        assert by_key["1"]["resolution_hours"] == 24.0
        assert by_key["2"]["resolution_hours"] is None  # unresolved
        assert by_key["3"]["resolution_hours"] is None  # negative duration
        assert by_key["2"]["resolution_hours"] != 0
        assert m["unresolved"] == 2  # keys 2 and 6
        assert m["closed_before_created"] == 1
        assert "negative_duration" in by_key["3"]["data_quality_flags"]
        assert "unresolved" in by_key["2"]["data_quality_flags"]
    finally:
        spark.stop()


def test_validation_flags():
    spark = get_spark("etl-flags", master="local[2]")
    try:
        df, m = clean(_frame(spark), snapshot="2024-02-01")
        by_key = {r["unique_key"]: r.asDict() for r in df.collect()}
        assert by_key["1"]["borough"] == "BROOKLYN"  # normalized
        assert by_key["4"]["borough"] is None  # Unspecified -> null
        assert by_key["4"]["complaint_type"] == "UNKNOWN"
        assert by_key["4"]["incident_zip"] is None
        assert by_key["4"]["latitude"] is None  # outside NYC box
        assert "invalid_coords" in by_key["4"]["data_quality_flags"]
        assert "invalid_zip" in by_key["4"]["data_quality_flags"]
        assert "missing_borough" in by_key["4"]["data_quality_flags"]
        assert m["invalid_coords"] == 1
        assert m["invalid_created"] == 1
        assert by_key["6"]["created_day"] is None
        assert by_key["6"]["month"] is None
    finally:
        spark.stop()


def test_dedupe_tie_deterministic():
    # Same key AND same created_at: survivor must be identical across runs.
    rows = [
        {**ROWS[0], "unique_key": "9", "descriptor": "bbb"},
        {**ROWS[0], "unique_key": "9", "descriptor": "aaa"},
    ]
    spark = get_spark("etl-tie", master="local[2]")
    try:
        outs = []
        for _ in range(2):
            df, m = clean(spark.createDataFrame(rows, schema=RAW_SCHEMA),
                          snapshot="2024-02-01")
            assert m["duplicates_removed"] == 1
            outs.append(sorted(str(r.asDict()) for r in df.collect()))
        assert outs[0] == outs[1]
        assert len(outs[0]) == 1
    finally:
        spark.stop()


def test_rerun_idempotent(tmp_path):
    spark = get_spark("etl-idem", master="local[2]")
    try:
        out = str(tmp_path / "clean")
        df1, m1 = clean(_frame(spark), snapshot="2024-02-01")
        write_clean(df1, out)
        n1 = spark.read.parquet(out).count()
        df2, m2 = clean(_frame(spark), snapshot="2024-02-01")
        write_clean(df2, out)  # overwrite, must not append
        back = spark.read.parquet(out)
        assert back.count() == n1 == m1["output_rows"] == m2["output_rows"]
        a = sorted(str(r.asDict()) for r in back.collect())
        assert len(a) == n1
    finally:
        spark.stop()
