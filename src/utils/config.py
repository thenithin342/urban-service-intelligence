"""Config loading helpers. ASCII only."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

REQUIRED_KEYS = [
    "dataset_id",
    "base_url",
    "fields",
    "order",
    "target_rows",
    "page_size",
    "timeout_seconds",
    "max_retries",
    "out_raw",
    "out_manifest",
]


def load_config(path: str | Path) -> dict[str, Any]:
    path = Path(path)
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if not isinstance(cfg, dict):
        raise ValueError(f" Config {path} must be a mapping")
    missing = [k for k in REQUIRED_KEYS if k not in cfg]
    if missing:
        raise ValueError(f" Config {path} missing keys: {missing}")
    return cfg


def resolve_path(base_dir: str | Path, maybe_relative: str | Path) -> Path:
    p = Path(maybe_relative)
    if p.is_absolute():
        return p
    return Path(base_dir) / p
