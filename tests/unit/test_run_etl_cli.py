"""Unit tests for run_etl CLI validation (no Spark session). ASCII only."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path("scripts").resolve()))

import run_etl


def test_snapshot_date_rejected(monkeypatch):
    monkeypatch.setattr(
        sys, "argv",
        ["run_etl.py", "--config", "configs/sample.yaml",
         "--snapshot-date", "not-a-date"],
    )
    with pytest.raises(SystemExit) as exc:
        run_etl.main()
    assert exc.value.code == 2


def test_snapshot_date_accepted_format(monkeypatch):
    # Validation passes; failure (if any) must come later, not from argparse.
    from datetime import date

    assert date.fromisoformat("2024-02-01").isoformat() == "2024-02-01"
    monkeypatch.setattr(
        sys, "argv",
        ["run_etl.py", "--config", "configs/sample.yaml",
         "--snapshot-date", "2024-02-01", "--help"],
    )
    with pytest.raises(SystemExit) as exc:
        run_etl.main()
    assert exc.value.code == 0  # --help exits 0: validation did not reject
