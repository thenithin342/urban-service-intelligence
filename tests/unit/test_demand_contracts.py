"""Unit tests for Member 1 demand contract exporter and dashboard adapter readback.

ASCII only.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from unittest.mock import patch

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


def _real_demand_artifacts_present() -> bool:
    """True when real Member 1 demand model artifacts are present on disk."""
    return (
        (ROOT / "artifacts" / "member1" / "demand_categories" / "future").is_dir()
        and (ROOT / "artifacts" / "member1" / "demand_categories" / "metrics").is_dir()
        and (ROOT / "artifacts" / "member1" / "demand_categories" / "predictions").is_dir()
        and (ROOT / "data" / "raw" / "member1_history" / "manifest.json").is_file()
    )


def _published_demand_contracts_present() -> bool:
    """True when demand contract tables have been exported to artifacts."""
    fc_path = contracts.contract_path(ROOT, "demand_forecast")
    fm_path = contracts.contract_path(ROOT, "demand_metrics")
    fc_meta = contracts.manifest_path(ROOT, "demand_forecast")
    fm_meta = contracts.manifest_path(ROOT, "demand_metrics")
    return (
        fc_path.is_dir()
        and fm_path.is_dir()
        and fc_meta.is_file()
        and fm_meta.is_file()
        and any(fc_path.glob("*.parquet"))
        and any(fm_path.glob("*.parquet"))
    )


@pytest.mark.skipif(
    not _real_demand_artifacts_present(),
    reason="real demand artifacts not generated in this checkout (run train_models.py first)",
)
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


@pytest.mark.skipif(
    not _published_demand_contracts_present(),
    reason="demand contracts not exported in this checkout (run export_demand_contracts.py first)",
)
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


@pytest.mark.skipif(
    not _published_demand_contracts_present(),
    reason="demand contracts not exported in this checkout (run export_demand_contracts.py first)",
)
def test_demand_forecast_numerical_and_key_invariants():
    """Verify non-negativity, non-null, and key uniqueness."""
    fc = ad.load_contract("demand_forecast", allow_fixture=False)
    df = fc.df

    assert not df.duplicated(subset=["borough", "complaint_type", "forecast_date"]).any()
    assert df["predicted_count"].notna().all()
    assert (df["predicted_count"] >= 0.0).all()
    assert not (df["predicted_count"] == float("inf")).any()


@pytest.mark.skipif(
    not _published_demand_contracts_present(),
    reason="demand contracts not exported in this checkout (run export_demand_contracts.py first)",
)
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


def test_build_forecast_table_rejects_observed_future_labels(tmp_path: Path):
    """Verify table builder rejects future split rows that contain observed actual labels."""
    future_dir = tmp_path / "future"
    future_dir.mkdir(parents=True, exist_ok=True)
    dates = pd.date_range("2024-03-01", periods=7).astype(str)
    bad_labels = pd.DataFrame(
        {
            "forecast_origin": ["2024-02-29"] * 7,
            "target_date": dates,
            "borough": ["QUEENS"] * 7,
            "complaint_type": ["ALL"] * 7,
            "horizon_days": list(range(1, 8)),
            "actual_count": [15] * 7,  # leaked future labels!
            "predicted_count": [10.0] * 7,
            "raw_prediction": [10.0] * 7,
            "model": ["seasonal_naive"] * 7,
            "model_version": ["v1"] * 7,
            "fit_cutoff": ["2024-02-29"] * 7,
            "split": ["future"] * 7,
        }
    )
    bad_labels.to_parquet(future_dir / "part.parquet")
    manifest = {"selected_model": "seasonal_naive"}
    cfg = {"input_parquet": "x"}
    with pytest.raises(ValueError, match="missing future labels guard"):
        build_demand_forecast_table(tmp_path, manifest, cfg, "test_study")


def test_build_forecast_table_rejects_non_uniform_origins_and_cutoffs(tmp_path: Path):
    """Verify table builder rejects non-uniform forecast origins or fit cutoffs."""
    future_dir = tmp_path / "future"
    future_dir.mkdir(parents=True, exist_ok=True)
    dates = pd.date_range("2024-03-01", periods=7).astype(str)

    # 1. Non-uniform forecast origins
    mixed_origins = pd.DataFrame(
        {
            "forecast_origin": ["2024-02-29"] * 6 + ["2024-03-01"],
            "target_date": dates,
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
    mixed_origins.to_parquet(future_dir / "part.parquet")
    manifest = {"selected_model": "seasonal_naive"}
    cfg = {"input_parquet": "x"}
    with pytest.raises(ValueError, match="forecast_origin must be uniform"):
        build_demand_forecast_table(tmp_path, manifest, cfg, "test_study")

    # 2. Non-uniform fit cutoffs
    mixed_cutoffs = pd.DataFrame(
        {
            "forecast_origin": ["2024-02-29"] * 7,
            "target_date": dates,
            "borough": ["QUEENS"] * 7,
            "complaint_type": ["ALL"] * 7,
            "horizon_days": list(range(1, 8)),
            "actual_count": [None] * 7,
            "predicted_count": [10.0] * 7,
            "raw_prediction": [10.0] * 7,
            "model": ["seasonal_naive"] * 7,
            "model_version": ["v1"] * 7,
            "fit_cutoff": ["2024-02-29"] * 6 + ["2024-01-31"],
            "split": ["future"] * 7,
        }
    )
    mixed_cutoffs.to_parquet(future_dir / "part.parquet")
    with pytest.raises(ValueError, match="fit_cutoff must be uniform"):
        build_demand_forecast_table(tmp_path, manifest, cfg, "test_study")


def test_build_forecast_table_rejects_manifest_coverage_mismatch(tmp_path: Path):
    """Verify table builder rejects forecast origin differing from manifest coverage end."""
    future_dir = tmp_path / "future"
    future_dir.mkdir(parents=True, exist_ok=True)
    dates = pd.date_range("2024-03-01", periods=7).astype(str)
    df = pd.DataFrame(
        {
            "forecast_origin": ["2024-02-29"] * 7,
            "target_date": dates,
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
    df.to_parquet(future_dir / "part.parquet")
    manifest = {
        "selected_model": "seasonal_naive",
        "coverage": {"end": "2024-03-15"},  # mismatch with 2024-02-29
    }
    cfg = {"input_parquet": "x"}
    with pytest.raises(ValueError, match="does not match manifest coverage end"):
        build_demand_forecast_table(tmp_path, manifest, cfg, "test_study")


def test_build_forecast_table_rejects_non_uniform_model_version(tmp_path: Path):
    """Verify table builder rejects multiple model versions in future prediction rows."""
    future_dir = tmp_path / "future"
    future_dir.mkdir(parents=True, exist_ok=True)
    dates = pd.date_range("2024-03-01", periods=7).astype(str)
    mixed_versions = pd.DataFrame(
        {
            "forecast_origin": ["2024-02-29"] * 7,
            "target_date": dates,
            "borough": ["QUEENS"] * 7,
            "complaint_type": ["ALL"] * 7,
            "horizon_days": list(range(1, 8)),
            "actual_count": [None] * 7,
            "predicted_count": [10.0] * 7,
            "raw_prediction": [10.0] * 7,
            "model": ["seasonal_naive"] * 7,
            "model_version": ["v1"] * 6 + ["v2"],
            "fit_cutoff": ["2024-02-29"] * 7,
            "split": ["future"] * 7,
        }
    )
    mixed_versions.to_parquet(future_dir / "part.parquet")
    manifest = {"selected_model": "seasonal_naive"}
    cfg = {"input_parquet": "x"}
    with pytest.raises(ValueError, match="model_version must be uniform"):
        build_demand_forecast_table(tmp_path, manifest, cfg, "test_study")


def test_build_forecast_table_rejects_inconsistent_horizon_to_date_mapping(tmp_path: Path):
    """Verify table builder rejects target dates that do not match origin + horizon_days."""
    future_dir = tmp_path / "future"
    future_dir.mkdir(parents=True, exist_ok=True)
    bad_dates = [
        "2024-03-01",
        "2024-03-02",
        "2024-03-10",  # wrong date for horizon 3 from 2024-02-29!
        "2024-03-04",
        "2024-03-05",
        "2024-03-06",
        "2024-03-07",
    ]
    df = pd.DataFrame(
        {
            "forecast_origin": ["2024-02-29"] * 7,
            "target_date": bad_dates,
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
    df.to_parquet(future_dir / "part.parquet")
    manifest = {"selected_model": "seasonal_naive"}
    cfg = {"input_parquet": "x"}
    with pytest.raises(
        ValueError, match="target_date does not match forecast_origin \\+ horizon_days mapping"
    ):
        build_demand_forecast_table(tmp_path, manifest, cfg, "test_study")


@pytest.mark.skipif(
    not _published_demand_contracts_present(),
    reason="demand contracts not exported in this checkout (run export_demand_contracts.py first)",
)
def test_demand_forecast_prevents_double_counting_and_separates_totals():
    """Verify that complaint_type=ALL totals and category subsets are not naively summed.

    Citywide total demand must be computed exclusively from complaint_type='ALL' rows.
    Named categories represent modeled subsets of those borough totals.
    """
    fc = ad.load_contract("demand_forecast", allow_fixture=False)
    df = fc.df

    all_rows = df[df["complaint_type"] == "ALL"]
    cat_rows = df[df["complaint_type"] != "ALL"]

    # 1. Total rows check: 42 ALL rows (6 geographies x 7 horizons) and 105 category rows (15 pairs x 7 horizons)
    assert len(all_rows) == 42
    assert len(cat_rows) == 105
    assert len(df) == 147

    # 2. Strict total checks
    # True citywide forecast volume (ALL rows only): exactly 58,260.0
    total_citywide_all = all_rows["predicted_count"].sum()
    assert total_citywide_all == 58260.0

    # Category subset total: 21,016.0
    total_categories = cat_rows["predicted_count"].sum()
    assert total_categories == 21016.0

    # Naive sum of the entire table would yield 79,276.0 which is a double count
    naive_sum = df["predicted_count"].sum()
    assert naive_sum == total_citywide_all + total_categories

    # 3. For every borough with categories, the category sum must not exceed the borough ALL total
    for borough in ["BRONX", "BROOKLYN", "MANHATTAN", "QUEENS", "STATEN ISLAND"]:
        b_all = all_rows[all_rows["borough"] == borough]["predicted_count"].sum()
        b_cat = cat_rows[cat_rows["borough"] == borough]["predicted_count"].sum()
        assert b_cat > 0.0
        assert b_cat < b_all, f"Categories in {borough} ({b_cat}) must be a proper subset of ALL ({b_all})"


def test_safe_publish_simulated_write_failure_restores_backup(tmp_path: Path):
    """Verify that an interrupted publication restores pre-existing valid files."""
    initial_df = pd.DataFrame(
        {
            "forecast_date": ["2024-03-01"],
            "borough": ["QUEENS"],
            "complaint_type": ["ALL"],
            "predicted_count": [100.0],
            "training_cutoff": ["2024-02-29"],
            "model_version": ["v1"],
        }
    )
    initial_meta = {
        "producer": "member1",
        "coverage_start": "2024-03-01",
        "coverage_end": "2024-03-01",
        "note": "original_export",
    }
    published_dir = safe_publish_contract(initial_df, initial_meta, tmp_path, "demand_forecast")
    target_meta_file = contracts.manifest_path(tmp_path, "demand_forecast")

    # Second export attempt that simulates an exception during copy2 to destination
    second_df = initial_df.copy()
    second_df["predicted_count"] = [999.0]
    second_meta = dict(initial_meta, note="failed_export")

    real_copy2 = shutil.copy2
    call_count = 0

    def fail_on_second_copy(src, dst, **kwargs):
        nonlocal call_count
        call_count += 1
        # Allow staged writes, but fail when copying to target meta
        if str(dst) == str(target_meta_file):
            raise IOError("Simulated disk error during meta copy")
        return real_copy2(src, dst, **kwargs)

    with patch("shutil.copy2", side_effect=fail_on_second_copy):
        with pytest.raises(IOError, match="Simulated disk error"):
            safe_publish_contract(second_df, second_meta, tmp_path, "demand_forecast")

    # Target directory and meta must still contain the ORIGINAL valid data, not the failed export
    restored_df = pd.read_parquet(published_dir / "part-00000.parquet")
    assert restored_df["predicted_count"].iloc[0] == 100.0
    restored_meta = json.loads(target_meta_file.read_text())
    assert restored_meta["note"] == "original_export"


def test_safe_publish_first_time_failure_leaves_no_orphans(tmp_path: Path):
    """Verify that a first-time publication failure leaves no partial parquet or meta files."""
    df = pd.DataFrame(
        {
            "forecast_date": ["2024-03-01"],
            "borough": ["QUEENS"],
            "complaint_type": ["ALL"],
            "predicted_count": [100.0],
            "training_cutoff": ["2024-02-29"],
            "model_version": ["v1"],
        }
    )
    meta = {
        "producer": "member1",
        "coverage_start": "2024-03-01",
        "coverage_end": "2024-03-01",
    }
    target_dir = contracts.contract_path(tmp_path, "demand_forecast")
    target_meta = contracts.manifest_path(tmp_path, "demand_forecast")

    real_copy2 = shutil.copy2

    def fail_on_meta_copy(src, dst, **kwargs):
        if str(dst) == str(target_meta):
            raise IOError("Simulated failure copying meta")
        return real_copy2(src, dst, **kwargs)

    with patch("shutil.copy2", side_effect=fail_on_meta_copy):
        with pytest.raises(IOError, match="Simulated failure copying meta"):
            safe_publish_contract(df, meta, tmp_path, "demand_forecast")

    # Neither partial parquet nor orphaned meta should remain
    assert not list(target_dir.glob("*.parquet"))
    assert not target_meta.exists()


def test_incomplete_output_is_classified_as_invalid_never_real(tmp_path: Path):
    """Verify dashboard adapter marks missing meta or partial output as INVALID, never REAL."""
    import os

    target_dir = contracts.contract_path(tmp_path, "demand_forecast")
    target_dir.mkdir(parents=True, exist_ok=True)
    dummy_df = pd.DataFrame(
        {
            "forecast_date": ["2024-03-01"],
            "borough": ["QUEENS"],
            "complaint_type": ["ALL"],
            "predicted_count": [100.0],
            "training_cutoff": ["2024-02-29"],
            "model_version": ["v1"],
        }
    )
    # Write only parquet, without sibling .meta.json
    dummy_df.to_parquet(target_dir / "part-00000.parquet")

    old_root = os.environ.get("USI_ROOT")
    os.environ["USI_ROOT"] = str(tmp_path)
    try:
        res = ad.load_contract("demand_forecast", allow_fixture=False)
        assert res.source == ad.INVALID
        assert any("missing sibling meta" in p for p in res.problems)
    finally:
        if old_root is not None:
            os.environ["USI_ROOT"] = old_root
        else:
            os.environ.pop("USI_ROOT", None)


def test_export_demand_contracts_with_synthetic_fixture(tmp_path: Path):
    """Verify full exporter pipeline, adapter readback, and double-counting checks in a clean environment."""
    import os

    import yaml

    from src.ingestion.downloader import sha256_file

    # 1. Source manifest and dummy producer
    src_manifest = tmp_path / "data" / "raw" / "manifest.json"
    src_manifest.parent.mkdir(parents=True)
    src_manifest.write_text(json.dumps({"complete": True, "start": "2023-12-01", "end": "2024-02-29"}), encoding="utf-8")
    src_hash = sha256_file(src_manifest)

    prod_file = tmp_path / "src" / "modeling" / "demand.py"
    prod_file.parent.mkdir(parents=True)
    prod_file.write_text("# dummy producer\n", encoding="utf-8")
    prod_hash = sha256_file(prod_file)

    # 2. Synthetic future table: 6 boroughs (ALL + 3 categories for the 5 main boroughs) x 7 horizons = 147 rows
    out_dir = tmp_path / "artifacts" / "member1" / "demand_synth"
    out_dir.mkdir(parents=True)
    future_dir = out_dir / "future"
    future_dir.mkdir()

    series_list = []
    for b in ["BRONX", "BROOKLYN", "MANHATTAN", "QUEENS", "STATEN ISLAND"]:
        series_list.append((b, "ALL", 100.0))
        series_list.append((b, "HEAT/HOT WATER", 25.0))
        series_list.append((b, "Illegal Parking", 25.0))
        series_list.append((b, "Noise - Residential", 25.0))
    series_list.append(("UNKNOWN", "ALL", 10.0))
    assert len(series_list) == 21

    future_rows = []
    for b, c, base_cnt in series_list:
        for h in range(1, 8):
            target = (pd.Timestamp("2024-02-29") + pd.Timedelta(days=h)).strftime("%Y-%m-%d")
            future_rows.append(
                {
                    "forecast_origin": "2024-02-29",
                    "target_date": target,
                    "borough": b,
                    "complaint_type": c,
                    "horizon_days": h,
                    "actual_count": None,
                    "predicted_count": base_cnt,
                    "raw_prediction": base_cnt,
                    "model": "seasonal_naive",
                    "model_version": "demand-v2-seasonal_naive-2024-02-29",
                    "fit_cutoff": "2024-02-29",
                    "split": "future",
                }
            )
    assert len(future_rows) == 147
    pd.DataFrame(future_rows).to_parquet(future_dir / "part.parquet")

    # 3. Synthetic metrics and predictions tables
    metrics_dir = out_dir / "metrics"
    metrics_dir.mkdir()
    candidate_models = ["seasonal_naive", "weekday_mean", "lr_scaled", "lr_unscaled", "random_forest", "gbt"]
    metric_rows = [
        {"model": m, "split": "test", "rows": 100, "mae": 50.0, "rmse": 80.0, "wape": 0.12}
        for m in candidate_models
    ]
    pd.DataFrame(metric_rows).to_parquet(metrics_dir / "part.parquet")

    preds_dir = out_dir / "predictions"
    preds_dir.mkdir()
    pred_rows = [
        {
            "model": m,
            "split": "test",
            "target_date": "2024-02-20",
            "model_version": f"demand-v2-{m}-2024-02-14",
        }
        for m in candidate_models
    ]
    pd.DataFrame(pred_rows).to_parquet(preds_dir / "part.parquet")

    # 4. Model manifest
    manifest_data = {
        "selected_model": "seasonal_naive",
        "producer_sha256": prod_hash,
        "coverage": {
            "source_manifest_sha256": src_hash,
            "start": "2023-12-01",
            "end": "2024-02-29",
        },
    }
    (out_dir / "model_manifest.json").write_text(json.dumps(manifest_data), encoding="utf-8")

    # 5. Config YAML
    cfg_data = {
        "source_manifest": "data/raw/manifest.json",
        "output_dir": "artifacts/member1/demand_synth",
        "input_parquet": "data/processed/nyc311",
        "quality_report": "artifacts/member1/etl_quality.json",
        "train_end": "2024-01-31",
        "validation_end": "2024-02-14",
        "seed": 42,
    }
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(yaml.safe_dump(cfg_data), encoding="utf-8")

    # 6. Execute export targeting tmp_path
    res = export_demand_contracts(cfg_file, root=tmp_path, study_id="synth_test")
    assert res["status"] == "published"
    assert res["demand_forecast"]["rows"] == 147
    assert res["demand_metrics"]["rows"] == 6

    # 7. Adapter verification under tmp_path
    old_root = os.environ.get("USI_ROOT")
    os.environ["USI_ROOT"] = str(tmp_path)
    try:
        fc = ad.load_contract("demand_forecast", allow_fixture=False)
        assert fc.source == ad.REAL
        assert len(fc.df) == 147
        assert fc.problems == []

        # Double-counting verification on synthetic data
        all_rows = fc.df[fc.df["complaint_type"] == "ALL"]
        cat_rows = fc.df[fc.df["complaint_type"] != "ALL"]
        assert len(all_rows) == 42
        assert len(cat_rows) == 105
        # 5 boroughs * 100 * 7 days + 1 borough * 10 * 7 days = 3500 + 70 = 3570
        assert all_rows["predicted_count"].sum() == 3570.0
        # 15 category series * 25 * 7 days = 2625
        assert cat_rows["predicted_count"].sum() == 2625.0

        fm = ad.load_contract("demand_metrics", allow_fixture=False)
        assert fm.source == ad.REAL
        assert len(fm.df) == 6
        assert fm.problems == []

        assert ad.check_table("demand_forecast")["status"] == "ok"
        assert ad.check_table("demand_metrics")["status"] == "ok"
    finally:
        if old_root is not None:
            os.environ["USI_ROOT"] = old_root
        else:
            os.environ.pop("USI_ROOT", None)



