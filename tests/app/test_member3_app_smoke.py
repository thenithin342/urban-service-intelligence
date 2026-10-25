"""Streamlit AppTest smoke tests for every dashboard page. ASCII only.

Each page is executed headless through the real st.navigation entry point
(dashboard/app.py) with (a) the repository artifacts, (b) an empty artifacts
root with the fixture fallback, and (c) an empty root without fixtures, which
must render empty states rather than raise.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from tests.app.conftest import REPO, real_member1_present

APP = str(REPO / "dashboard" / "app.py")
VIEWS = sorted(
    p.name for p in (REPO / "dashboard" / "views").glob("*.py") if p.name != "__init__.py"
)


def _texts(at: AppTest) -> str:
    parts = []
    for kind in ("info", "warning", "error", "caption", "markdown", "title", "subheader"):
        parts += [str(e.value) for e in getattr(at, kind)]
    return "\n".join(parts)


def run_page(view: str) -> AppTest:
    at = AppTest.from_file(APP, default_timeout=120)
    at.run()
    assert not at.exception, [e.value for e in at.exception]
    if view != "overview.py":
        at.switch_page(f"views/{view}")
        at.run()
    assert not at.exception, [e.message for e in at.exception]
    assert len(at.title) == 1
    return at


def test_views_discovered():
    expected = {"overview.py", "trends.py", "hotspots.py", "resolution.py"}
    assert expected <= set(VIEWS)


@pytest.mark.skipif(not real_member1_present(), reason="real Member 1 tables absent")
@pytest.mark.parametrize("view", VIEWS)
def test_page_runs_with_real_artifacts(view):
    at = run_page(view)
    text = _texts(at)
    assert "coverage" in text.lower()


@pytest.mark.parametrize("view", VIEWS)
def test_page_runs_with_empty_root_and_fixtures(empty_root, view):
    at = run_page(view)
    text = _texts(at)
    assert "coverage" in text.lower()
    assert "REAL:" not in text  # nothing real exists in an empty root


@pytest.mark.parametrize("view", VIEWS)
def test_page_shows_empty_states_without_any_data(empty_root, monkeypatch, view):
    monkeypatch.setenv("USI_DISABLE_FIXTURES", "1")
    at = run_page(view)
    text = _texts(at)
    assert "No data" in text or "not been produced" in text or "MISSING" in text
    assert "FIXTURE:" not in text


def test_fixture_panels_are_labelled(empty_root):
    at = run_page("resolution.py")
    warnings = " ".join(str(w.value) for w in at.warning)
    assert "FIXTURE" in warnings and "NOT results" in warnings


@pytest.mark.skipif(not real_member1_present(), reason="real Member 1 tables absent")
def test_overview_borough_filter_interaction():
    at = run_page("overview.py")
    ms = at.sidebar.multiselect(key="ov_borough")
    ms.select("BRONX").run()
    assert not at.exception
    assert any("BRONX" in str(c.value) for c in at.caption)


@pytest.mark.skipif(not real_member1_present(), reason="real Member 1 tables absent")
def test_app_reads_only_small_artifact_tables(monkeypatch):
    seen: list[str] = []
    original = pd.read_parquet

    def spy(path, *args, **kwargs):
        seen.append(str(path))
        return original(path, *args, **kwargs)

    monkeypatch.setattr(pd, "read_parquet", spy)
    for view in VIEWS:
        run_page(view)
    assert seen, "pages read no tables"
    art = str(Path(REPO / "artifacts"))
    for path in seen:
        assert path.startswith(art), f"app read a non-artifact file: {path}"
        assert "nyc311_clean" not in path and "member1_history" not in path


def test_forecast_page_shows_cutoff_horizon_and_versions_from_table(empty_root):
    at = run_page("forecasts.py")
    metrics = {m.label: str(m.value) for m in at.metric}
    # Values come from the labelled demand_forecast / closure fixtures.
    assert metrics["Training cutoff"] == "2024-01-12"
    assert metrics["Horizon (days)"] == "7"
    assert metrics["Model version"] == "fixture-0"
    assert metrics["Train cutoff"] == "2024-01-08"
    warnings = " ".join(str(w.value) for w in at.warning)
    assert "FIXTURE: demand_forecast" in warnings
