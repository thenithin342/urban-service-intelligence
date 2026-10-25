"""Shared data contract tests (no Spark). ASCII only."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from src.utils import contracts

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "contracts"


def _fixture(name: str) -> dict:
    return json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize("name", sorted(p.stem for p in FIXTURES.glob("*.json")))
def test_fixtures_match_contracts_and_are_labelled(name):
    assert name in contracts.CONTRACTS
    data = _fixture(name)
    assert data["meta"]["is_fixture"] is True
    assert contracts.validate(pd.DataFrame(data["rows"]), name) == []


def test_every_external_contract_has_a_fixture():
    # Member 3 tables are produced locally; all others need a fixture so the
    # dashboard can be developed before upstream outputs exist.
    names = {n for n, c in contracts.CONTRACTS.items() if c.owner != "member3"}
    assert names <= {p.stem for p in FIXTURES.glob("*.json")}


def test_validate_reports_missing_columns_and_nulls():
    df = pd.DataFrame(_fixture("pattern_rules")["rows"]).drop(columns=["lift"])
    df.loc[0, "support"] = None
    problems = contracts.validate(df, "pattern_rules")
    assert any("missing columns ['lift']" in p for p in problems)
    assert any("support: nulls" in p for p in problems)


def test_validate_reports_wrong_type():
    df = pd.DataFrame(_fixture("demand_metrics")["rows"])
    df["mae"] = "twelve"
    assert any("mae: expected float" in p for p in contracts.validate(df, "demand_metrics"))


def test_nullable_columns_accept_nulls():
    df = pd.DataFrame(_fixture("resolution_summary")["rows"])
    df["closure_rate_7d"] = None
    assert contracts.validate(df, "resolution_summary") == []


def test_write_table_roundtrip_and_meta(tmp_path: Path):
    df = pd.DataFrame(_fixture("pattern_rules")["rows"])
    out = contracts.write_table(df, tmp_path, "pattern_rules", {"producer": "test"})
    back = pd.read_parquet(out)
    assert len(back) == len(df)
    assert contracts.validate(back, "pattern_rules") == []
    meta = json.loads(contracts.manifest_path(tmp_path, "pattern_rules").read_text())
    assert meta["is_fixture"] is False and meta["rows"] == len(df)


def test_write_table_rejects_invalid(tmp_path: Path):
    df = pd.DataFrame(_fixture("pattern_rules")["rows"]).drop(columns=["support"])
    with pytest.raises(ValueError):
        contracts.write_table(df, tmp_path, "pattern_rules", {"producer": "test"})
    assert not contracts.contract_path(tmp_path, "pattern_rules").exists()
