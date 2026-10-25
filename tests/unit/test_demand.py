"""Demand configuration boundaries."""

import json
from datetime import date

import pytest
import yaml

from src.ingestion.downloader import sha256_file
from src.ingestion.history import daily_windows
from src.modeling.demand import load_demand_config, verified_daily


def test_demand_accepts_yaml_dates_and_normalizes_seed(tmp_path):
    cfg = {"input_parquet": "x", "source_manifest": "x", "quality_report": "x", "output_dir": "x",
           "seed": "42", "train_end": date(2024, 1, 31), "validation_end": date(2024, 2, 14)}
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(cfg))
    result = load_demand_config(path)
    assert result["train_end"] == "2024-01-31" and result["validation_end"] == "2024-02-14"
    assert result["seed"] == 42


def test_demand_config_rejects_reversed_dates(tmp_path):
    cfg = {"input_parquet": "x", "source_manifest": "x", "quality_report": "x",
           "output_dir": "x", "seed": 42, "train_end": "2024-02-14", "validation_end": "2024-01-31"}
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(cfg))
    with pytest.raises(ValueError, match="ordered"):
        load_demand_config(path)


@pytest.mark.parametrize("complete", [False, True])
def test_incomplete_or_missing_day_never_becomes_zero(tmp_path, complete):
    path = tmp_path / "manifest.json"
    days = [{"day": d, "rows": 0} for d, _ in daily_windows("2023-12-01", "2024-03-01")]
    path.write_text(json.dumps({"complete": complete, "start": "2023-12-01", "end": "2024-03-01", "days": days[:-1]}))
    quality = tmp_path / "quality.json"
    quality.write_text(json.dumps({"source_manifest_sha256": sha256_file(path)}))
    with pytest.raises(ValueError, match="complete|consecutive"):
        verified_daily(None, {"source_manifest": "manifest.json", "quality_report": "quality.json"}, tmp_path)
