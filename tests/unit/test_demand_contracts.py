"""Unit tests for Member 1 demand contract exporter and dashboard adapter readback.

ASCII only.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from dashboard import adapters as ad
from scripts.export_demand_contracts import (
    build_demand_forecast_table,
    export_demand_contracts,
    safe_publish_contract,
    validate_study_provenance,
)
from src.utils import contracts

ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT / "configs/demand_categories.yaml"


def test_export_demand_contracts_roundtrip_and_adapter_readback():
    """Verify export runs cleanly and dashboard adapters load real data without fixtures."""
    result = export_demand_contracts(CONFIG_PATH, root=ROOT, study_id="winter_history_91d")
    assert result["status"] == "published"
    assert result["demand_forecast"]["rows"] == 147
    assert result["demand_metrics"]["rows"] == 6

    # 1. Dashboard adapter readback with fixtures strictly forbidden
    fc = ad.load_contract("demand_forecast", allow_fixture=False)
    assert fc.source == ad.REAL
    assert fc.problems == []
    assert len(fc.df) == 147
    assert set(contracts.CONTRACTS["demand_forecast"].columns) <= set(fc.df.columns)
    assert fc.meta["is_fixture"] is False
    assert fc.meta["study_id"] == "winter_history_91d"

    fm = ad.load_contract("demand_metrics", allow_fixture=False)
    assert fm.source == ad.REAL
    assert fm.problems == []
    assert len(fm.df) == 6
    assert set(contracts.CONTRACTS["demand_metrics"].columns) <= set(fm.df.columns)
    assert fm.meta["is_fixture"] is False
    assert fm.meta["split"] == "test"

    # 2. Independent adapter freshness and validity check
    fc_check = ad.check_table("demand_forecast")
    assert fc_check["status"] == "ok"
    assert fc_check["problems"] == []
    assert fc_check["warnings"] == []

    fm_check = ad.check_table("demand_metrics")
    assert fm_check["status"] == "ok"
    assert fm_check["problems"] == []
    assert fm_check["warnings"] == []


def test_demand_forecast_chronological_and_horizon_integrity():
    """Verify 7 consecutive horizons, future ordering, and no lookahead."""
    fc = ad.load_contract("demand_forecast", allow_fixture=False)
    df = fc.df

    # Forecast dates must strictly follow training cutoff
    forecast_dates = pd.to_datetime(df["forecast_date"]).dt.date
    training_cutoffs = pd.to_datetime(df["training_cutoff"]).dt.date
    assert (forecast_dates > training_cutoffs).all()

    # Must contain exactly 7 consecutive days
    unique_dates = sorted(forecast_dates.unique())
    assert len(unique_dates) == 7
    for i in range(len(unique_dates) - 1):
        assert (unique_dates[i + 1] - unique_dates[i]).days == 1

    # Every series must have exactly 7 forecast dates
    series_counts = df.groupby(["borough", "complaint_type"]).size()
    assert (series_counts == 7).all()

    # Category and ALL series coexistence without collapse
    complaint_types = set(df["complaint_type"].unique())
    assert "ALL" in complaint_types
    assert "HEAT/HOT WATER" in complaint_types


def test_demand_forecast_numerical_and_key_invariants():
    """Verify non-negativity, non-null, and key uniqueness."""
    fc = ad.load_contract("demand_forecast", allow_fixture=False)
    df = fc.df

    assert not df.duplicated(subset=["borough", "complaint_type", "forecast_date"]).any()
    assert df["predicted_count"].notna().all()
    assert (df["predicted_count"] >= 0.0).all()
    assert not (df["predicted_count"] == float("inf")).any()


def test_demand_metrics_model_coverage_and_invariants():
    """Verify test metrics include all candidate models with non-negative errors."""
    fm = ad.load_contract("demand_metrics", allow_fixture=False)
    df = fm.df

    expected_models = {
        "seasonal_naive",
        "weekday_mean",
        "lr_scaled",
        "lr_unscaled",
        "random_forest",
        "gbt",
    }
    assert set(df["model"].unique()) == expected_models
    assert (df["mae"] >= 0.0).all()
    assert (df["rmse"] >= 0.0).all()
    assert (df["wape"].dropna() >= 0.0).all()

    # Eval dates must be populated and chronological
    eval_starts = pd.to_datetime(df["eval_start"]).dt.date
    eval_ends = pd.to_datetime(df["eval_end"]).dt.date
    assert (eval_starts <= eval_ends).all()


def test_provenance_validation_rejects_tampered_source(tmp_path: Path):
    """Verify that source manifest hash mismatch triggers fail-closed rejection."""
    fake_cfg = {
        "source_manifest": "fake_manifest.json",
        "output_dir": "fake_output",
    }
    manifest = tmp_path / "fake_manifest.json"
    manifest.write_text(json.dumps({"complete": True}), encoding="utf-8")

    out_dir = tmp_path / "fake_output"
    out_dir.mkdir(parents=True)
    model_manifest = {
        "coverage": {"source_manifest_sha256": "mismatched_sha"},
        "producer_sha256": "any",
    }
    (out_dir / "model_manifest.json").write_text(json.dumps(model_manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="source provenance mismatch"):
        validate_study_provenance(fake_cfg, tmp_path)


def test_provenance_validation_rejects_producer_drift(tmp_path: Path):
    """Verify that modified producer code triggers fail-closed rejection."""
    fake_cfg = {
        "source_manifest": "manifest.json",
        "output_dir": "fake_output",
    }
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}", encoding="utf-8")
    from src.ingestion.downloader import sha256_file

    # Create dummy producer file in tmp_path
    producer_dir = tmp_path / "src" / "modeling"
    producer_dir.mkdir(parents=True)
    producer_file = producer_dir / "demand.py"
    producer_file.write_text("# dummy producer\n", encoding="utf-8")

    out_dir = tmp_path / "fake_output"
    out_dir.mkdir(parents=True)
    model_manifest = {
        "coverage": {"source_manifest_sha256": sha256_file(manifest)},
        "producer_sha256": "stale_producer_hash_12345",
    }
    (out_dir / "model_manifest.json").write_text(json.dumps(model_manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="demand producer code changed"):
        validate_study_provenance(fake_cfg, tmp_path)


def test_build_forecast_table_rejects_negative_or_missing_horizon(tmp_path: Path):
    """Verify table builder rejects negative predictions or incomplete horizons."""
    future_dir = tmp_path / "future"
    future_dir.mkdir(parents=True)

    # DataFrame with missing horizon (only 6 horizons instead of 7)
    dates_6 = pd.date_range("2024-03-01", periods=6)
    bad_df_missing_h = pd.DataFrame(
        {
            "forecast_origin": "2024-02-29",
            "target_date": dates_6.astype(str),
            "borough": "QUEENS",
            "complaint_type": "ALL",
            "horizon_days": list(range(1, 7)),  # missing horizon 7
            "actual_count": [None] * 6,
            "predicted_count": [10.0] * 6,
            "raw_prediction": [10.0] * 6,
            "model": "seasonal_naive",
            "model_version": "v1",
            "fit_cutoff": "2024-02-29",
            "split": "future",
        }
    )
    bad_df_missing_h.to_parquet(future_dir / "part.parquet")

    manifest = {"selected_model": "seasonal_naive"}
    cfg = {"input_parquet": "x"}

    with pytest.raises(ValueError, match="expected \\[1, 2, 3, 4, 5, 6, 7\\]"):
        build_demand_forecast_table(tmp_path, manifest, cfg, "test_study")

    # Complete 7 horizons but with negative count
    dates_7 = pd.date_range("2024-03-01", periods=7)
    bad_df_neg = pd.DataFrame(
        {
            "forecast_origin": "2024-02-29",
            "target_date": dates_7.astype(str),
            "borough": "QUEENS",
            "complaint_type": "ALL",
            "horizon_days": list(range(1, 8)),
            "actual_count": [None] * 7,
            "predicted_count": [10.0, -5.0, 10.0, 10.0, 10.0, 10.0, 10.0],
            "raw_prediction": [10.0, -5.0, 10.0, 10.0, 10.0, 10.0, 10.0],
            "model": "seasonal_naive",
            "model_version": "v1",
            "fit_cutoff": "2024-02-29",
            "split": "future",
        }
    )
    bad_df_neg.to_parquet(future_dir / "part.parquet")
    with pytest.raises(ValueError, match="negative or infinite"):
        build_demand_forecast_table(tmp_path, manifest, cfg, "test_study")


def test_safe_publish_failure_preserves_existing_contract(tmp_path: Path):
    """Verify that publication failure does not destroy or corrupt an existing export."""
    # 1. Publish a good initial table
    valid_df = pd.DataFrame(
        {
            "forecast_date": ["2024-03-01"],
            "borough": ["QUEENS"],
            "complaint_type": ["ALL"],
            "predicted_count": [100.0],
            "training_cutoff": ["2024-02-29"],
            "model_version": ["v1"],
        }
    )
    valid_meta = {
        "producer": "member1",
        "coverage_start": "2024-03-01",
        "coverage_end": "2024-03-01",
    }
    published_dir = safe_publish_contract(valid_df, valid_meta, tmp_path, "demand_forecast")
    assert (published_dir / "part-00000.parquet").is_file()
    assert contracts.manifest_path(tmp_path, "demand_forecast").is_file()

    # 2. Attempt publishing an invalid DataFrame (missing required column)
    invalid_df = valid_df.drop(columns=["predicted_count"])
    with pytest.raises(ValueError, match="validation failed"):
        safe_publish_contract(invalid_df, valid_meta, tmp_path, "demand_forecast")

    # 3. Verify original published files remain completely intact and valid
    restored_df = pd.read_parquet(published_dir / "part-00000.parquet")
    assert len(restored_df) == 1
    assert "predicted_count" in restored_df.columns
    assert contracts.validate(restored_df, "demand_forecast") == []


def test_build_forecast_table_rejects_chronological_leakage(tmp_path: Path):
    """Verify table builder rejects forecast origins after targets or fit cutoffs after origins."""
    future_dir = tmp_path / "future"
    future_dir.mkdir(parents=True, exist_ok=True)

    # target_date before forecast_origin
    bad_dates = pd.DataFrame(
        {
            "forecast_origin": ["2024-03-05"] * 7,
            "target_date": pd.date_range("2024-03-01", periods=7).astype(str),
            "borough": ["QUEENS"] * 7,
            "complaint_type": ["ALL"] * 7,
            "horizon_days": list(range(1, 8)),
            "actual_count": [None] * 7,
            "predicted_count": [10.0] * 7,
            "raw_prediction": [10.0] * 7,
            "model": ["seasonal_naive"] * 7,
            "model_version": ["v1"] * 7,
            "fit_cutoff": ["2024-02-29"] * 7,
            "split": ["future"] * 7,
        }
    )
    bad_dates.to_parquet(future_dir / "part.parquet")
    manifest = {"selected_model": "seasonal_naive"}
    cfg = {"input_parquet": "x"}

    with pytest.raises(ValueError, match="target dates must strictly follow forecast origin"):
        build_demand_forecast_table(tmp_path, manifest, cfg, "test_study")


def test_build_forecast_table_rejects_duplicate_series_keys(tmp_path: Path):
    """Verify table builder rejects duplicate keys on (borough, category, target_date)."""
    future_dir = tmp_path / "future"
    future_dir.mkdir(parents=True, exist_ok=True)

    # 8 rows with one duplicate key
    dates = ["2024-03-01", "2024-03-01"] + [f"2024-03-0{i}" for i in range(2, 8)]
    horizons = [1, 1, 2, 3, 4, 5, 6, 7]
    bad_dups = pd.DataFrame(
        {
            "forecast_origin": ["2024-02-29"] * 8,
            "target_date": dates,
            "borough": ["QUEENS"] * 8,
            "complaint_type": ["ALL"] * 8,
            "horizon_days": horizons,
            "actual_count": [None] * 8,
            "predicted_count": [10.0] * 8,
            "raw_prediction": [10.0] * 8,
            "model": ["seasonal_naive"] * 8,
            "model_version": ["v1"] * 8,
            "fit_cutoff": ["2024-02-29"] * 8,
            "split": ["future"] * 8,
        }
    )
    bad_dups.to_parquet(future_dir / "part.parquet")
    manifest = {"selected_model": "seasonal_naive"}
    cfg = {"input_parquet": "x"}

    with pytest.raises(ValueError, match="duplicate geography/category/date keys"):
        build_demand_forecast_table(tmp_path, manifest, cfg, "test_study")


def test_build_forecast_table_rejects_invalid_borough(tmp_path: Path):
    """Verify table builder rejects unexpected borough names outside NYC contract."""
    future_dir = tmp_path / "future"
    future_dir.mkdir(parents=True, exist_ok=True)

    dates = pd.date_range("2024-03-01", periods=7).astype(str)
    bad_borough = pd.DataFrame(
        {
            "forecast_origin": ["2024-02-29"] * 7,
            "target_date": dates,
            "borough": ["NEWARK"] * 7,  # Not in NYC contract
            "complaint_type": ["ALL"] * 7,
            "horizon_days": list(range(1, 8)),
            "actual_count": [None] * 7,
            "predicted_count": [10.0] * 7,
            "raw_prediction": [10.0] * 7,
            "model": ["seasonal_naive"] * 7,
            "model_version": ["v1"] * 7,
            "fit_cutoff": ["2024-02-29"] * 7,
            "split": ["future"] * 7,
        }
    )
    bad_borough.to_parquet(future_dir / "part.parquet")
    manifest = {"selected_model": "seasonal_naive"}
    cfg = {"input_parquet": "x"}

    with pytest.raises(ValueError, match="unexpected borough values"):
        build_demand_forecast_table(tmp_path, manifest, cfg, "test_study")

