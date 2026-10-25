"""Tests for output freshness/validity checks and scripts/check_outputs.py. ASCII only."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pandas as pd
import pytest

from dashboard import adapters as ad
from src.utils import contracts
from tests.app.conftest import REPO

FIXTURES = REPO / "tests" / "fixtures" / "contracts"


def _load_cli():
    spec = importlib.util.spec_from_file_location(
        "check_outputs", REPO / "scripts" / "check_outputs.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _rows(name: str) -> pd.DataFrame:
    return pd.DataFrame(json.loads((FIXTURES / f"{name}.json").read_text())["rows"])


def _publish(root: Path, name: str, df: pd.DataFrame | None = None, **meta) -> None:
    df = _rows(name) if df is None else df
    if "model_version" in df.columns:
        df = df.assign(model_version="real-1")
    contracts.write_table(df, root, name, {"producer": "test", **meta})


def _cleaned_input(root: Path, coverage_end: str) -> None:
    clean = root / "data" / "processed" / "nyc311_clean"
    clean.mkdir(parents=True)
    (clean / "_SUCCESS").write_text("")
    m1 = root / "artifacts" / "member1" / "analytics"
    m1.mkdir(parents=True, exist_ok=True)
    (m1 / "phase3_metrics.json").write_text(
        json.dumps({"coverage": {"period_start": "2024-01-01", "period_end": coverage_end}})
    )


def test_fixture_only_and_missing_statuses(empty_root):
    reports = {r["table"]: r for r in ad.check_all()}
    assert reports["pattern_rules"]["status"] == ad.FIXTURE
    assert "fixture-only" in reports["pattern_rules"]["problems"][0]
    assert reports["priority_scores"]["status"] == ad.MISSING


def test_fresh_real_table_is_ok(empty_root):
    _cleaned_input(empty_root, "2024-01-12")
    _publish(empty_root, "demand_metrics", coverage_end="2024-01-12")
    rep = ad.check_table("demand_metrics")
    assert rep["status"] == "ok", rep
    assert rep["problems"] == []


def test_missing_model_version_detected(empty_root):
    df = _rows("demand_forecast").assign(model_version="  ")
    contracts.write_table(df, empty_root, "demand_forecast", {"producer": "test"})
    rep = ad.check_table("demand_forecast")
    assert rep["status"] == ad.INVALID
    assert "missing model_version" in rep["problems"]


def test_contract_violation_detected(empty_root):
    path = ad.contract_dir("closure_model_comparison")
    path.mkdir(parents=True)
    _rows("closure_model_comparison").drop(columns=["brier"]).to_parquet(
        path / "part-00000.parquet"
    )
    ad.contract_meta_path("closure_model_comparison").write_text(
        json.dumps({"contract_version": "1.0.0", "producer": "t", "is_fixture": False,
                    "generated_at_utc": "2030-01-01T00:00:00+00:00"})
    )
    rep = ad.check_table("closure_model_comparison")
    assert rep["status"] == ad.INVALID
    assert any("missing columns ['brier']" in p for p in rep["problems"])


def test_stale_when_coverage_older_than_cleaned_data(empty_root):
    _cleaned_input(empty_root, "2024-02-29")
    _publish(empty_root, "resolution_summary")  # fixture rows cover 2024-01-12
    rep = ad.check_table("resolution_summary")
    assert rep["status"] == "stale"
    assert any("coverage_end 2024-01-12 older" in p for p in rep["problems"])


def test_short_coverage_gap_is_warning_not_stale(empty_root):
    # Complete-week producers can end up to 6 days before their input.
    _cleaned_input(empty_root, "2024-01-15")
    _publish(empty_root, "resolution_summary")  # fixture rows cover 2024-01-12
    rep = ad.check_table("resolution_summary")
    assert rep["status"] == "ok"
    assert any("3 days" in w for w in rep["warnings"])


def test_stale_when_generated_before_input_written(empty_root):
    _publish(empty_root, "demand_metrics")
    meta_path = ad.contract_meta_path("demand_metrics")
    meta = json.loads(meta_path.read_text())
    meta["generated_at_utc"] = "2000-01-01T00:00:00+00:00"
    meta_path.write_text(json.dumps(meta))
    _cleaned_input(empty_root, "2024-01-12")
    rep = ad.check_table("demand_metrics")
    assert rep["status"] == "stale"
    assert any("older than input data" in p for p in rep["problems"])


def test_published_fixture_is_not_ok(empty_root):
    _publish(empty_root, "demand_metrics", is_fixture=True)
    rep = ad.check_table("demand_metrics")
    assert rep["status"] == ad.FIXTURE


def test_history_end_parsing():
    assert ad._history_end({"coverage_end": "2024-02-29"}) == "2024-02-29"
    assert ad._history_end({"daily_counts": {"2023-12-01": 5, "2024-02-29": 7}}) == "2024-02-29"
    assert ad._history_end({"config": {"end": "2024-03-01"}}) == "2024-02-29"
    assert ad._history_end({}) is None


@pytest.mark.parametrize(
    ("allow", "expected"), [(False, 1), (True, 0)]
)
def test_cli_exit_codes_on_fixture_only_root(empty_root, capsys, allow, expected):
    cli = _load_cli()
    args = ["--allow-missing"] if allow else []
    assert cli.main(args) == expected
    out = capsys.readouterr().out
    assert "pattern_rules" in out and f"exit {expected}" in out


def test_cli_fails_on_invalid_even_with_allow_missing(empty_root, capsys):
    df = _rows("demand_forecast").assign(model_version="")
    contracts.write_table(df, empty_root, "demand_forecast", {"producer": "test"})
    cli = _load_cli()
    assert cli.main(["--allow-missing", "--json"]) == 1
    payload = json.loads(capsys.readouterr().out)
    statuses = {t["table"]: t["status"] for t in payload["tables"]}
    assert statuses["demand_forecast"] == ad.INVALID
