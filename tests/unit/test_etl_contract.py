"""Unit tests for ETL contract (no Spark needed). ASCII only."""

from __future__ import annotations

from datetime import datetime

from src.processing.clean import CLEAN_COLUMNS, RAW_FIELDS, RAW_SCHEMA


def test_raw_schema_matches_config_fields():
    import yaml

    with open("configs/sample.yaml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    assert [f.name for f in RAW_SCHEMA.fields] == cfg["fields"]
    assert [f.name for f in RAW_SCHEMA.fields] == RAW_FIELDS


def test_clean_columns_cover_contract():
    required = [
        "unique_key", "created_at", "closed_at", "complaint_type",
        "borough", "incident_zip", "latitude", "longitude",
        "resolution_hours", "created_day", "data_snapshot_date",
        "data_quality_flags",
    ]
    for c in required:
        assert c in CLEAN_COLUMNS


def test_soda_timestamp_formats_parse():
    # Pure-Python check that our Spark format strings match real SODA values.
    assert datetime.strptime("2024-01-01T00:04:14.000", "%Y-%m-%dT%H:%M:%S.%f")
    assert datetime.strptime("2024-01-01T00:04:14", "%Y-%m-%dT%H:%M:%S")


def test_sample_config_has_etl_paths():
    import yaml

    with open("configs/sample.yaml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    assert cfg["etl_output"].endswith("nyc311_clean")
    assert cfg["etl_quality_report"].endswith(".json")
    assert cfg["input_csv"].endswith(".csv")
