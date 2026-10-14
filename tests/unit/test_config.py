"""Config tests. ASCII only."""

from __future__ import annotations

from pathlib import Path

from src.utils.config import load_config

ROOT = Path(__file__).resolve().parents[2]


def test_sample_config_loads():
    cfg = load_config(ROOT / "configs" / "sample.yaml")
    assert cfg["dataset_id"] == "erm2-nwe9"
    assert "unique_key" in cfg["fields"]
    assert cfg["target_rows"] >= 1


def test_default_config_loads():
    cfg = load_config(ROOT / "configs" / "default.yaml")
    assert cfg["page_size"] >= 1
