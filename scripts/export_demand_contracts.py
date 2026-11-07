"""Export verified Member 1 demand forecasts and metrics to shared contracts.

Maps model outputs from artifacts/member1/demand*/ (future, metrics, predictions,
model_manifest.json) into the shared contract tables:
  - artifacts/member1/forecasts/tables/demand_forecast (part-00000.parquet + .meta.json)
  - artifacts/member1/forecasts/tables/demand_metrics (part-00000.parquet + .meta.json)

Validated with src.utils.contracts. Publication uses staged directory writing,
pre-publication validation, and automated rollback/backup restoration on error.
Note: File copies are sequential (part-00000.parquet and .meta.json) and do not
claim OS-level atomic directory swaps.

Usage:
  python scripts/export_demand_contracts.py [--config configs/demand_categories.yaml]
                                            [--study-id winter_history_91d]
                                            [--root .]

ASCII only.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from src.ingestion.downloader import sha256_file
from src.modeling.demand import load_demand_config
from src.utils import contracts

VALID_BOROUGHS = frozenset(
    {"BRONX", "BROOKLYN", "MANHATTAN", "QUEENS", "STATEN ISLAND", "UNKNOWN"}
)


def validate_study_provenance(cfg: dict[str, Any], root: Path) -> dict[str, Any]:
    """Verify raw source manifest and demand model manifest exist and match."""
    manifest_path = root / cfg["source_manifest"]
    if not manifest_path.is_file():
        raise FileNotFoundError(f"source manifest not found: {manifest_path}")

    source_hash = sha256_file(manifest_path)
    model_dir = root / cfg["output_dir"]
    model_manifest_path = model_dir / "model_manifest.json"
    if not model_manifest_path.is_file():
        raise FileNotFoundError(
            f"model manifest not found in {model_dir}; run train_models.py first"
        )

    model_manifest = json.loads(model_manifest_path.read_text(encoding="utf-8"))
    coverage = model_manifest.get("coverage", {})
    recorded_source_hash = coverage.get("source_manifest_sha256")
    if recorded_source_hash != source_hash:
        raise ValueError(
            f"source provenance mismatch: expected {source_hash}, recorded {recorded_source_hash}"
        )

    producer_path = root / "src/modeling/demand.py"
    current_producer_hash = sha256_file(producer_path)
    recorded_producer_hash = model_manifest.get("producer_sha256")
    if recorded_producer_hash != current_producer_hash:
        raise ValueError(
            "demand producer code changed since training; rebuild features and retrain before export"
        )

    return model_manifest


def build_demand_forecast_table(
    model_dir: Path,
    model_manifest: dict[str, Any],
    cfg: dict[str, Any],
    study_id: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Transform future predictions into the demand_forecast contract table."""
    future_path = model_dir / "future"
    if not future_path.is_dir() and not list(future_path.glob("*.parquet")):
        raise FileNotFoundError(f"future predictions parquet directory not found at {future_path}")

    raw = pd.read_parquet(future_path)
    if raw.empty:
        raise ValueError("future predictions table is empty")

    # 1. Split and model filtering
    if not (raw["split"] == "future").all():
        raise ValueError("future predictions table contains non-future split rows")

    selected_model = model_manifest["selected_model"]
    selected = raw[raw["model"] == selected_model].copy()
    if selected.empty:
        raise ValueError(
            f"no future rows match selected model '{selected_model}' in {future_path}"
        )

    # 2. Key uniqueness and geographic validity
    duplicate_mask = selected.duplicated(
        subset=["borough", "complaint_type", "target_date", "model_version"]
    )
    if duplicate_mask.any():
        dups = selected[duplicate_mask][["borough", "complaint_type", "target_date"]]
        raise ValueError(f"duplicate geography/category/date keys found in future table:\n{dups}")

    invalid_boroughs = set(selected["borough"].dropna().unique()) - VALID_BOROUGHS
    if invalid_boroughs:
        raise ValueError(f"unexpected borough values in forecast: {invalid_boroughs}")

    # 3. Horizon completeness: exactly 1..7 for every series
    grouped_horizons = (
        selected.groupby(["borough", "complaint_type"])["horizon_days"]
        .apply(lambda s: sorted(s.tolist()))
        .to_dict()
    )
    expected_horizons = list(range(1, 8))
    for series, horizons in grouped_horizons.items():
        if horizons != expected_horizons:
            raise ValueError(
                f"series {series} has horizons {horizons}, expected {expected_horizons}"
            )

    # 4. Numerical validity: non-null, finite, non-negative
    pred_counts = selected["predicted_count"]
    if pred_counts.isna().any():
        raise ValueError("predicted_count contains null values")
    if not ((pred_counts >= 0.0) & (pred_counts != float("inf"))).all():
        raise ValueError("predicted_count contains negative or infinite values")

    # 5. Missing future labels guard: actual_count must not contain observed values
    if "actual_count" in selected.columns and not selected["actual_count"].isna().all():
        raise ValueError(
            "future predictions table must not have observed actual_count values (missing future labels guard)"
        )

    # 6. Chronological and series consistency
    origins = pd.to_datetime(selected["forecast_origin"]).dt.date
    if origins.nunique() != 1:
        raise ValueError(
            f"forecast_origin must be uniform across all future rows, found: {origins.unique()}"
        )

    manifest_cov_end = model_manifest.get("coverage", {}).get("end")
    if manifest_cov_end and str(origins.iloc[0]) != str(manifest_cov_end):
        raise ValueError(
            f"forecast_origin {origins.iloc[0]} does not match manifest coverage end {manifest_cov_end}"
        )

    versions = selected["model_version"].unique()
    if len(versions) != 1:
        raise ValueError(
            f"model_version must be uniform across all future rows, found: {versions}"
        )

    fit_cutoffs = pd.to_datetime(selected["fit_cutoff"]).dt.date
    if fit_cutoffs.nunique() != 1:
        raise ValueError(
            f"fit_cutoff must be uniform across all future rows, found: {fit_cutoffs.unique()}"
        )

    if (fit_cutoffs > origins).any():
        raise ValueError("training cutoff cannot exceed forecast origin")

    targets = pd.to_datetime(selected["target_date"]).dt.date
    if (targets <= origins).any():
        raise ValueError("target dates must strictly follow forecast origin")

    # 7. Horizon-to-date mapping: each row's target_date must equal forecast_origin + horizon_days
    origin_dt = pd.to_datetime(origins)
    expected_target_dts = origin_dt + pd.to_timedelta(selected["horizon_days"], unit="D")
    expected_targets = expected_target_dts.dt.date
    if not (targets == expected_targets).all():
        raise ValueError(
            "target_date does not match forecast_origin + horizon_days mapping"
        )

    # 6. Map to contract schema
    df_out = pd.DataFrame(
        {
            "forecast_date": targets.astype(str),
            "borough": selected["borough"].astype(str),
            "complaint_type": selected["complaint_type"].astype(str),
            "predicted_count": pred_counts.astype(float),
            "training_cutoff": fit_cutoffs.astype(str),
            "model_version": selected["model_version"].astype(str),
        }
    )

    # Sort deterministically
    df_out = df_out.sort_values(
        ["forecast_date", "borough", "complaint_type", "model_version"]
    ).reset_index(drop=True)

    meta = {
        "producer": "member1",
        "coverage_start": str(targets.min()),
        "coverage_end": str(targets.max()),
        "forecast_origin": str(origins.iloc[0]),
        "training_cutoff": str(fit_cutoffs.iloc[0]),
        "model_version": str(selected["model_version"].iloc[0]),
        "selected_model": selected_model,
        "horizon_days": 7,
        "study_id": study_id,
        "source_manifest_sha256": model_manifest.get("coverage", {}).get(
            "source_manifest_sha256", ""
        ),
        "input_path": cfg.get("input_parquet", ""),
        "series_count": len(grouped_horizons),
        "note": "Real retrospective 7-day demand forecast from historical origin",
    }

    return df_out, meta


def build_demand_metrics_table(
    model_dir: Path,
    model_manifest: dict[str, Any],
    cfg: dict[str, Any],
    study_id: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Transform test evaluation metrics into the demand_metrics contract table."""
    metrics_path = model_dir / "metrics"
    predictions_path = model_dir / "predictions"

    if not metrics_path.is_dir() or not list(metrics_path.glob("*.parquet")):
        raise FileNotFoundError(f"metrics parquet directory not found at {metrics_path}")
    if not predictions_path.is_dir() or not list(predictions_path.glob("*.parquet")):
        raise FileNotFoundError(f"predictions parquet directory not found at {predictions_path}")

    metrics_df = pd.read_parquet(metrics_path)
    test_metrics = metrics_df[metrics_df["split"] == "test"].copy()
    if test_metrics.empty:
        raise ValueError("no test split rows in metrics table")

    preds_df = pd.read_parquet(predictions_path)
    test_preds = preds_df[preds_df["split"] == "test"]
    if test_preds.empty:
        raise ValueError("no test split rows in predictions table")

    eval_start = str(pd.to_datetime(test_preds["target_date"]).min().date())
    eval_end = str(pd.to_datetime(test_preds["target_date"]).max().date())

    model_versions = test_preds.groupby("model")["model_version"].first().to_dict()
    missing_versions = set(test_metrics["model"]) - set(model_versions)
    if missing_versions:
        raise ValueError(f"models in metrics lack version in predictions: {missing_versions}")

    # Check numerical validity
    for col in ("mae", "rmse"):
        vals = test_metrics[col]
        if vals.isna().any() or (vals < 0.0).any() or (vals == float("inf")).any():
            raise ValueError(f"demand_metrics.{col} contains null, negative, or infinite values")

    wape_vals = test_metrics["wape"].dropna()
    if (wape_vals < 0.0).any() or (wape_vals == float("inf")).any():
        raise ValueError("demand_metrics.wape contains negative or infinite values")

    df_out = pd.DataFrame(
        {
            "model": test_metrics["model"].astype(str),
            "mae": test_metrics["mae"].astype(float),
            "rmse": test_metrics["rmse"].astype(float),
            "wape": test_metrics["wape"].astype(float),
            "eval_start": eval_start,
            "eval_end": eval_end,
            "model_version": test_metrics["model"].map(model_versions).astype(str),
        }
    )

    df_out = df_out.sort_values("model").reset_index(drop=True)

    meta = {
        "producer": "member1",
        "coverage_start": eval_start,
        "coverage_end": eval_end,
        "split": "test",
        "selected_model": model_manifest.get("selected_model", ""),
        "study_id": study_id,
        "source_manifest_sha256": model_manifest.get("coverage", {}).get(
            "source_manifest_sha256", ""
        ),
        "input_path": cfg.get("input_parquet", ""),
        "note": "Chronological holdout test metrics for demand candidate models",
    }

    return df_out, meta


def safe_publish_contract(
    df: pd.DataFrame,
    meta: dict[str, Any],
    root: Path,
    name: str,
) -> Path:
    """Safely validate in-memory, stage in a temp directory, and publish.

    Ensures that validation failures or partial write errors can never
    corrupt, overwrite, or delete an existing valid export.
    Note: File copies are sequential (part-00000.parquet and .meta.json) and do
    not claim OS-level atomic directory replacement. Rollback logic restores
    the backup directory upon caught write failures.
    """
    problems = contracts.validate(df, name)
    if problems:
        raise ValueError(f"validation failed for {name}: {'; '.join(problems)}")

    target_dir = contracts.contract_path(root, name)
    target_meta = contracts.manifest_path(root, name)
    staging_base = root / "artifacts" / "member1" / "forecasts" / ".staging"
    staged_dir = contracts.contract_path(staging_base, name)
    staged_meta = contracts.manifest_path(staging_base, name)

    if staged_dir.exists():
        shutil.rmtree(staged_dir)
    staged_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Write to staging using the standard contracts writer
        contracts.write_table(df, staging_base, name, meta)

        # Validate that the staged parquet and meta read back cleanly
        staged_parquet = staged_dir / "part-00000.parquet"
        if not staged_parquet.is_file() or not staged_meta.is_file():
            raise FileNotFoundError(f"staging write failed to produce files for {name}")

        check_df = pd.read_parquet(staged_parquet)
        check_probs = contracts.validate(check_df, name)
        if check_probs:
            raise ValueError(f"staged readback validation failed: {check_probs}")

        # Staged write is verified. Now safely publish to destination.
        target_dir.mkdir(parents=True, exist_ok=True)
        backup_dir = root / "artifacts" / "member1" / "forecasts" / f".backup_{name}"
        if backup_dir.exists():
            shutil.rmtree(backup_dir)

        # Move existing files to backup if present
        existing_parquet = list(target_dir.glob("*.parquet"))
        if existing_parquet or target_meta.is_file():
            backup_dir.mkdir(parents=True, exist_ok=True)
            for p in existing_parquet:
                shutil.copy2(p, backup_dir / p.name)
            if target_meta.is_file():
                shutil.copy2(target_meta, backup_dir / target_meta.name)

        try:
            # Overwrite destination from staging
            for old in target_dir.glob("*.parquet"):
                old.unlink()
            shutil.copy2(staged_parquet, target_dir / "part-00000.parquet")
            shutil.copy2(staged_meta, target_meta)
        except Exception:
            # Restore backup if copy failed
            if backup_dir.exists() and list(backup_dir.glob("*")):
                for old in target_dir.glob("*.parquet"):
                    old.unlink()
                for b in backup_dir.glob("*.parquet"):
                    shutil.copy2(b, target_dir / b.name)
                b_meta = backup_dir / target_meta.name
                if b_meta.is_file():
                    shutil.copy2(b_meta, target_meta)
            else:
                # First-time publication failed midway: clear partial files
                for partial in target_dir.glob("*.parquet"):
                    partial.unlink()
                if target_meta.is_file():
                    target_meta.unlink()
            raise
        finally:
            if backup_dir.exists():
                shutil.rmtree(backup_dir, ignore_errors=True)

    finally:
        if staging_base.exists():
            shutil.rmtree(staging_base, ignore_errors=True)

    return target_dir


def export_demand_contracts(
    config_path: str | Path,
    root: Path = ROOT,
    study_id: str = "winter_history_91d",
    dry_run: bool = False,
) -> dict[str, Any]:
    """Execute demand contracts export for the specified configuration."""
    cfg = load_demand_config(config_path)
    model_manifest = validate_study_provenance(cfg, root)
    model_dir = root / cfg["output_dir"]

    # 1. Build and validate demand_forecast
    df_forecast, meta_forecast = build_demand_forecast_table(
        model_dir, model_manifest, cfg, study_id
    )

    # 2. Build and validate demand_metrics
    df_metrics, meta_metrics = build_demand_metrics_table(
        model_dir, model_manifest, cfg, study_id
    )

    if dry_run:
        return {
            "status": "dry_run_ok",
            "demand_forecast": {"rows": len(df_forecast), "meta": meta_forecast},
            "demand_metrics": {"rows": len(df_metrics), "meta": meta_metrics},
        }

    # 3. Safe publication
    path_forecast = safe_publish_contract(df_forecast, meta_forecast, root, "demand_forecast")
    path_metrics = safe_publish_contract(df_metrics, meta_metrics, root, "demand_metrics")

    return {
        "status": "published",
        "study_id": study_id,
        "demand_forecast": {
            "path": str(path_forecast),
            "rows": len(df_forecast),
            "coverage": [meta_forecast["coverage_start"], meta_forecast["coverage_end"]],
            "model_version": meta_forecast["model_version"],
        },
        "demand_metrics": {
            "path": str(path_metrics),
            "rows": len(df_metrics),
            "coverage": [meta_metrics["coverage_start"], meta_metrics["coverage_end"]],
            "models": df_metrics["model"].tolist(),
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--config",
        default="configs/demand_categories.yaml",
        help="Path to demand configuration YAML (default: configs/demand_categories.yaml)",
    )
    parser.add_argument(
        "--study-id",
        default="winter_history_91d",
        help="Study identifier for tracking (default: winter_history_91d)",
    )
    parser.add_argument(
        "--root",
        default=str(ROOT),
        help="Repository root directory (default: repo root)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate inputs and tables without writing to published contract paths",
    )
    args = parser.parse_args(argv)

    root = Path(args.root).resolve()
    result = export_demand_contracts(
        config_path=args.config,
        root=root,
        study_id=args.study_id,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
