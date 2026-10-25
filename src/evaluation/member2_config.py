"""Member 2 (mining, resolution, evaluation) settings and IO helpers. ASCII only.

The optional ``member2:`` block in a YAML config overrides these defaults.
Configs are read with a plain YAML loader because ``configs/history.yaml``
is a download config without the sample-only keys that
``src.utils.config.load_config`` requires.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

PRODUCER = "member2"
MODEL_VERSION_MINING = "member2-fpgrowth-v1"
MODEL_VERSION_CLOSURE = "member2-closure-v1"

MEMBER2_DEFAULTS: dict[str, Any] = {
    # Input cleaned Parquet (relative to repo root unless absolute).
    "input": None,
    # Root under which contract paths (artifacts/member2/...) are written.
    "artifacts_root": ".",
    "outdir": "artifacts/member2",
    # Mining: primary basket granularity and thresholds.
    "basket_granularity": "week",  # week -> (incident_zip, created_week)
    "drop_partial_weeks": False,
    "min_support": 0.6,
    "min_confidence": 0.8,
    "support_grid": [0.5, 0.6, 0.7, 0.8],
    "confidence_grid": [0.5, 0.7, 0.9],
    # Secondary (sensitivity) granularity with its own grid.
    "secondary_granularity": "day",
    "secondary_support_grid": [0.1, 0.2, 0.3, 0.4],
    "secondary_min_support": 0.2,
    "secondary_min_confidence": 0.8,
    "max_itemsets_guard": 200000,
    # Closure model.
    "closure_horizon_days": 7,
    "train_cutoff": None,  # YYYY-MM-DD; test = created_day >= cutoff
    "validation_days": 2,  # tail of train used for model selection
    "label_embargo_days": 0,  # train rows must satisfy created_day < cutoff - embargo
    "test_end": None,  # optional inclusive last created_day of the test window
    "top_categories": 40,
    "smoothing_alpha": 20.0,
    "min_cohort_n": 30,
    "seed": 42,
    "max_collect_rows": 2000000,
}


def read_yaml(path: str | Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    if not isinstance(cfg, dict):
        raise TypeError(f"config {path} must be a mapping")
    return cfg


def member2_config(cfg: dict[str, Any]) -> dict[str, Any]:
    """Merge the ``member2`` block over defaults and validate."""
    out = dict(MEMBER2_DEFAULTS)
    block = cfg.get("member2") or {}
    if not isinstance(block, dict):
        raise TypeError("config 'member2' block must be a mapping")
    unknown = sorted(set(block) - set(MEMBER2_DEFAULTS))
    if unknown:
        raise ValueError(f"unknown member2 config keys: {unknown}")
    out.update(block)
    if out["input"] is None:
        out["input"] = cfg.get("etl_output", "data/processed/nyc311_clean")
    for key in ("min_support", "min_confidence", "secondary_min_support",
                "secondary_min_confidence", "smoothing_alpha"):
        out[key] = float(out[key])
    for key in ("support_grid", "confidence_grid", "secondary_support_grid"):
        out[key] = sorted(float(v) for v in out[key])
        if not out[key] or any(not 0 < v <= 1 for v in out[key]):
            raise ValueError(f"member2.{key} values must be in (0, 1]")
    for key in ("closure_horizon_days", "validation_days", "label_embargo_days",
                "top_categories", "min_cohort_n", "seed", "max_collect_rows",
                "max_itemsets_guard"):
        out[key] = int(out[key])
        if out[key] < 0:
            raise ValueError(f"member2.{key} must be >= 0")
    for key in ("min_support", "min_confidence", "secondary_min_support",
                "secondary_min_confidence"):
        if not 0 < out[key] <= 1:
            raise ValueError(f"member2.{key} must be in (0, 1]")
    if out["basket_granularity"] not in ("week", "day"):
        raise ValueError("member2.basket_granularity must be 'week' or 'day'")
    if out["secondary_granularity"] not in ("week", "day", None):
        raise ValueError("member2.secondary_granularity must be week, day or null")
    if out["closure_horizon_days"] < 1:
        raise ValueError("member2.closure_horizon_days must be >= 1")
    return out


def resolve(root: Path, value: str | Path) -> Path:
    p = Path(value)
    return p if p.is_absolute() else root / p


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()  # noqa: UP017


def write_json(path: Path, obj: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, default=str, sort_keys=False)
    return path


def write_extra_table(df: pd.DataFrame, path: Path, meta: dict[str, Any]) -> Path:
    """Write a non-contract Member 2 table (<dir>/part-00000.parquet + meta)."""
    path.mkdir(parents=True, exist_ok=True)
    for old in path.glob("*.parquet"):
        old.unlink()
    df.to_parquet(path / "part-00000.parquet", index=False)
    full = {
        "producer": PRODUCER,
        "generated_at_utc": utc_now(),
        "is_fixture": False,
        "rows": len(df),
        "columns": [str(c) for c in df.columns],
        **meta,
    }
    write_json(path.with_name(path.name + ".meta.json"), full)
    return path


def frame_hash(df: pd.DataFrame) -> str:
    """Order-sensitive content hash of a frame (lists rendered as text)."""
    rendered = df.copy()
    for col in rendered.columns:
        if rendered[col].map(lambda v: hasattr(v, "__len__") and not isinstance(v, str)).any():
            rendered[col] = rendered[col].map(lambda v: "|".join(map(str, list(v))))
    payload = rendered.to_csv(index=False, float_format="%.12g").encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def history_ready(main_root: Path) -> dict[str, Any]:
    """Readiness of Member 1's historical expansion (read-only check)."""
    pq = main_root / "data" / "processed" / "member1_history"
    quality = main_root / "artifacts" / "member1" / "phase4" / "etl_quality.json"
    return {
        "parquet": str(pq),
        "quality_json": str(quality),
        "ready": quality.is_file() and (pq / "_SUCCESS").is_file(),
    }
