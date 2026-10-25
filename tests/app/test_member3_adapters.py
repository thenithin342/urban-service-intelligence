"""Member 3 adapter and aggregate schema-contract tests (no Spark). ASCII only."""

from __future__ import annotations

import json

import pandas as pd
import pytest

from dashboard import adapters as ad
from src.utils import contracts
from tests.app.conftest import REPO, real_member1_present

FIXTURES = REPO / "tests" / "fixtures" / "contracts"
EXTERNAL = sorted(n for n, c in contracts.CONTRACTS.items() if c.owner != "member3")


# ------------------------------------------------------------ contract tables


@pytest.mark.parametrize("name", EXTERNAL)
def test_fixture_fallback_is_labelled_and_valid(empty_root, name):
    res = ad.load_contract(name)
    assert res.source == ad.FIXTURE
    assert res.is_fixture and res.meta["is_fixture"] is True
    assert res.problems == []
    assert contracts.validate(res.df, name) == []
    assert len(res.df) > 0


@pytest.mark.parametrize("name", sorted(contracts.CONTRACTS))
def test_missing_when_fixtures_disabled(empty_root, monkeypatch, name):
    monkeypatch.setenv("USI_DISABLE_FIXTURES", "1")
    res = ad.load_contract(name)
    assert res.source == ad.MISSING
    assert list(res.df.columns) == list(contracts.CONTRACTS[name].columns)
    assert not res.usable


def test_real_contract_table_preferred_over_fixture(empty_root):
    rows = json.loads((FIXTURES / "pattern_rules.json").read_text())["rows"]
    df = pd.DataFrame(rows).assign(model_version="real-test")
    contracts.write_table(df, empty_root, "pattern_rules", {"producer": "test"})
    res = ad.load_contract("pattern_rules")
    assert res.source == ad.REAL
    assert set(res.df["model_version"]) == {"real-test"}
    assert res.meta["producer"] == "test"


def test_contract_violation_is_refused(empty_root):
    path = ad.contract_dir("demand_metrics")
    path.mkdir(parents=True)
    pd.DataFrame({"model": ["x"], "mae": [1.0]}).to_parquet(path / "part-00000.parquet")
    ad.contract_meta_path("demand_metrics").write_text(
        json.dumps({"contract_version": "1.0.0", "producer": "t",
                    "generated_at_utc": "2026-01-01T00:00:00+00:00", "is_fixture": False})
    )
    res = ad.load_contract("demand_metrics")
    assert res.source == ad.INVALID
    assert any("missing columns" in p for p in res.problems)
    assert not res.usable


def test_real_table_without_meta_is_invalid(empty_root):
    rows = json.loads((FIXTURES / "demand_metrics.json").read_text())["rows"]
    path = ad.contract_dir("demand_metrics")
    path.mkdir(parents=True)
    pd.DataFrame(rows).to_parquet(path / "part-00000.parquet")
    res = ad.load_contract("demand_metrics")
    assert res.source == ad.INVALID
    assert any("meta" in p for p in res.problems)


def test_artifacts_root_override(tmp_path, monkeypatch):
    monkeypatch.setenv("USI_ARTIFACTS_ROOT", str(tmp_path / "elsewhere"))
    assert ad.contract_dir("pattern_rules") == (
        tmp_path / "elsewhere" / "member2" / "mining" / "tables" / "pattern_rules"
    )
    assert ad.member1_dir() == tmp_path / "elsewhere" / "member1" / "analytics"


# ------------------------------------------------------------ member 1 tables


def test_member1_missing_gives_empty_frame(empty_root):
    for name, cols in ad.MEMBER1_COLUMNS.items():
        res = ad.load_member1(name)
        assert res.source == ad.MISSING
        assert list(res.df.columns) == cols


def test_member1_missing_column_is_invalid(empty_root):
    path = ad.member1_dir() / "tables" / "complaints_by_borough"
    path.mkdir(parents=True)
    pd.DataFrame({"borough": ["BRONX"]}).to_parquet(path / "part-0.parquet")
    res = ad.load_member1("complaints_by_borough")
    assert res.source == ad.INVALID
    assert "complaint_count" in res.problems[0]


@pytest.mark.skipif(
    not real_member1_present(),
    reason="real Member 1 tables absent: run scripts/run_member1_analytics.py first",
)
@pytest.mark.parametrize("name", sorted(ad.MEMBER1_COLUMNS))
def test_real_member1_tables_match_artifact_contract(name):
    res = ad.load_member1(name)
    assert res.source == ad.REAL, res.problems
    assert len(res.df) > 0
    assert res.meta["coverage_start"] and res.meta["coverage_end"]
    if name in ("spatial_hotspots", "spatial_grid_cells", "cluster_summary"):
        assert bool(ad.in_bbox(res.df).all()), "spatial table has out-of-bbox cells"
    if "complaint_count" in res.df.columns:
        assert (res.df["complaint_count"] >= 0).all()


@pytest.mark.skipif(not real_member1_present(), reason="real Member 1 tables absent")
def test_real_member1_aggregates_reconcile_with_manifest():
    total = int(ad.member1_manifest()["row_count"])
    for name in ("complaints_by_borough", "complaints_by_category", "complaints_daily"):
        assert int(ad.load_member1(name).df["complaint_count"].sum()) == total


@pytest.mark.skipif(
    not any((REPO / "artifacts").glob("member*/**/*.meta.json")),
    reason="no real contract tables published yet (only fixtures available)",
)
def test_real_contract_tables_validate():
    checked = 0
    for name in contracts.CONTRACTS:
        if ad.contract_dir(name).is_dir():
            res = ad.load_contract(name, allow_fixture=False)
            assert res.problems == [], res.problems
            checked += 1
    assert checked > 0


# ------------------------------------------------------------ helpers


def test_valid_map_points_drops_out_of_bbox_and_nulls():
    df = pd.DataFrame(
        {
            "center_lat": [40.7, 41.5, None, 40.5, 40.6],
            "center_lon": [-73.9, -73.9, -73.9, -75.0, -73.7],
        }
    )
    out = ad.valid_map_points(df)
    assert list(out.index) == [0, 4]
    assert ad.in_bbox(out).all()


def test_coverage_note_labels_short_windows():
    assert "not seasonal conclusions" in ad.coverage_note("2024-01-01", "2024-01-12")
    assert "12 days" in ad.coverage_note("2024-01-01", "2024-01-12")
    assert "not a full annual cycle" in ad.coverage_note("2023-12-01", "2024-02-29")
    assert ad.coverage_note(None, None) == "coverage dates not recorded"


def test_coverage_from_table_columns():
    res = ad.TableResult(
        "priority_backtest",
        pd.DataFrame({"window_start": ["2024-01-01", "2024-01-08"],
                      "window_end": ["2024-01-07", "2024-01-14"]}),
        ad.REAL,
    )
    assert ad.coverage(res) == ("2024-01-01", "2024-01-14")


def test_list_text():
    assert ad.list_text(["a", "b"]) == "a + b"
    assert ad.list_text(None) == ""
    assert ad.list_text("x") == "x"


def test_views_do_not_read_files_directly():
    views = sorted((REPO / "dashboard").rglob("*.py"))
    assert views
    for path in views:
        if path.name == "adapters.py":
            continue
        text = path.read_text(encoding="utf-8")
        for forbidden in ("read_parquet", "read_csv", "nyc311_clean", "data/raw", "spark"):
            assert forbidden not in text, f"{path.name} references {forbidden}"
