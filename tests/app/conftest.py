"""Shared fixtures for Member 3 dashboard tests. ASCII only."""

from __future__ import annotations

from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
REAL_M1 = REPO / "artifacts" / "member1" / "analytics" / "tables"


def real_member1_present() -> bool:
    return REAL_M1.is_dir() and any(REAL_M1.rglob("*.parquet"))


@pytest.fixture(autouse=True)
def _isolated_env(monkeypatch):
    """Every test starts from the default roots and fresh caches."""
    for var in ("USI_ROOT", "USI_ARTIFACTS_ROOT", "USI_FIXTURES_DIR", "USI_DISABLE_FIXTURES"):
        monkeypatch.delenv(var, raising=False)
    from dashboard import adapters

    adapters.clear_caches()
    yield
    adapters.clear_caches()


@pytest.fixture()
def empty_root(tmp_path, monkeypatch) -> Path:
    """Point the adapters at an empty repository/artifacts root."""
    (tmp_path / "artifacts").mkdir()
    monkeypatch.setenv("USI_ROOT", str(tmp_path))
    monkeypatch.setenv("USI_ARTIFACTS_ROOT", str(tmp_path / "artifacts"))
    return tmp_path
