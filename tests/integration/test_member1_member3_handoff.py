"""Cross-workstream integration tests: Member 1 demand forecasting to Member 3 priorities.

Validates that Member 3 priority scoring consumes Member 1's real 147-row
demand_forecast contract, verifies model provenance and window alignment,
confirms demand_model_units > 0, guards against double counting complaint_type='ALL'
borough totals, and validates dashboard adapter contract states.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from dashboard import adapters as ad
from src.prioritization import scoring
from src.utils import contracts

REPO = Path(__file__).resolve().parents[2]
REAL_DEMAND_FC = REPO / "artifacts" / "member1" / "forecasts" / "tables" / "demand_forecast"
REAL_DEMAND_MET = REPO / "artifacts" / "member1" / "forecasts" / "tables" / "demand_metrics"
REAL_PRIORITY_SCORES = REPO / "artifacts" / "member3" / "priorities" / "tables" / "priority_scores"


def _real_demand_and_priorities_present() -> bool:
    return (
        REAL_DEMAND_FC.is_dir()
        and (REAL_DEMAND_FC / "part-00000.parquet").is_file()
        and REAL_PRIORITY_SCORES.is_dir()
        and (REAL_PRIORITY_SCORES / "part-00000.parquet").is_file()
    )


@pytest.mark.skipif(
    not _real_demand_and_priorities_present(),
    reason="real demand_forecast and priority_scores contracts absent in clean CI checkout",
)
def test_real_demand_forecast_consumed_by_priority_engine():
    """Verify Member 3 priority engine consumes real Member 1 demand forecast."""
    ps = ad.load_contract("priority_scores", allow_fixture=False)
    fc = ad.load_contract("demand_forecast", allow_fixture=False)

    assert ps.source == ad.REAL
    assert ps.usable is True
    assert ps.problems == []
    assert len(ps.df) > 0

    # 1. Assert demand_model_units > 0 and demand_source points to Member 1 model
    meta = ps.meta
    assert meta["demand_model_units"] > 0, "priority engine used observed fallback, not model demand"
    assert meta["demand_model_units"] == 15, "expected 15 modeled borough-category pairs"

    expected_model_version = "demand-v2-seasonal_naive-2024-02-29"
    assert "member1 demand_forecast" in meta["demand_source"]
    assert expected_model_version in meta["demand_source"]

    # 2. Verify window alignment
    ws = str(ps.df["window_start"].iloc[0])
    we = str(ps.df["window_end"].iloc[0])
    assert ws == "2024-02-23"
    assert we == "2024-02-29"

    # Forecast horizon starts the day after scoring window cutoff (2024-03-01)
    fc_df = fc.df
    fc_dates = pd.to_datetime(fc_df["forecast_date"]).dt.date
    assert fc_dates.min() == date(2024, 3, 1)
    assert fc_dates.max() == date(2024, 3, 7)

    # 3. Verify exact numerical alignment for the 15 category series
    fc_categories = fc_df[fc_df["complaint_type"] != "ALL"]
    fc_mean = (
        fc_categories.groupby(["borough", "complaint_type"])["predicted_count"]
        .mean()
        .to_dict()
    )

    ps_df = ps.df
    for (borough, complaint_type), expected_mean in fc_mean.items():
        matched = ps_df[(ps_df["location"] == borough) & (ps_df["complaint_type"] == complaint_type)]
        assert not matched.empty, f"missing priority unit for ({borough}, {complaint_type})"
        actual_demand_value = matched["demand_value"].iloc[0]
        assert abs(actual_demand_value - expected_mean) < 1e-6, (
            f"mismatched demand value for ({borough}, {complaint_type}): "
            f"priority={actual_demand_value}, forecast={expected_mean}"
        )


@pytest.mark.skipif(
    not _real_demand_and_priorities_present(),
    reason="real demand_forecast and priority_scores contracts absent in clean CI checkout",
)
def test_priority_scores_guards_against_double_counting_all():
    """Verify complaint_type='ALL' is not injected as an overlapping unit alongside categories."""
    ps = ad.load_contract("priority_scores", allow_fixture=False)
    ps_df = ps.df

    # In priority_scores, units must be individual complaint types, never aggregate 'ALL'
    all_units = ps_df[ps_df["complaint_type"] == "ALL"]
    assert len(all_units) == 0, (
        f"Found {len(all_units)} units with complaint_type='ALL'; "
        "borough totals must not be scored as complaint categories to avoid double counting."
    )

    # Total units scored
    assert len(ps_df) == 612
    # Verify that ranked units have positive priority score and valid bounded components
    assert (ps_df["priority_score"] >= 0.0).all()
    assert (ps_df["priority_score"] <= 1.0).all()


@pytest.mark.skipif(
    not _real_demand_and_priorities_present(),
    reason="real demand_forecast and priority_scores contracts absent in clean CI checkout",
)
def test_dashboard_demand_and_priority_contracts_load_as_real():
    """Test that dashboard adapters load real contracts without fixture fallback."""
    fc = ad.load_contract("demand_forecast", allow_fixture=False)
    fm = ad.load_contract("demand_metrics", allow_fixture=False)
    ps = ad.load_contract("priority_scores", allow_fixture=False)
    pb = ad.load_contract("priority_backtest", allow_fixture=False)

    for res, name, expected_len in [
        (fc, "demand_forecast", 147),
        (fm, "demand_metrics", 6),
        (ps, "priority_scores", 612),
        (pb, "priority_backtest", 108),
    ]:
        assert res.source == ad.REAL, f"{name} source expected real, got {res.source}"
        assert res.usable is True, f"{name} is not usable"
        assert res.problems == [], f"{name} reported problems: {res.problems}"
        assert len(res.df) == expected_len, f"{name} expected {expected_len} rows, got {len(res.df)}"


def test_synthetic_demand_to_priority_handoff_and_override(tmp_path):
    """Test demand forecast override and double-counting protection using synthetic fixtures."""
    from scripts.run_member3_priorities import _model_inputs_at_root

    # 1. Create a synthetic demand_forecast table with both ALL and categories
    root = tmp_path
    fc_rows = []
    # 2 boroughs x 7 horizons for ALL
    # 2 boroughs x 2 categories x 7 horizons for categories
    start_date = date(2024, 3, 1)
    for h in range(7):
        target = start_date + timedelta(days=h)
        fc_rows.append({
            "forecast_date": str(target),
            "borough": "BROOKLYN",
            "complaint_type": "ALL",
            "predicted_count": 1000.0,
            "training_cutoff": "2024-02-29",
            "model_version": "test-v1",
        })
        fc_rows.append({
            "forecast_date": str(target),
            "borough": "BROOKLYN",
            "complaint_type": "HEAT/HOT WATER",
            "predicted_count": 300.0,
            "training_cutoff": "2024-02-29",
            "model_version": "test-v1",
        })
        fc_rows.append({
            "forecast_date": str(target),
            "borough": "BROOKLYN",
            "complaint_type": "Illegal Parking",
            "predicted_count": 400.0,
            "training_cutoff": "2024-02-29",
            "model_version": "test-v1",
        })

    fc_df = pd.DataFrame(fc_rows)
    contracts.write_table(
        fc_df,
        root,
        "demand_forecast",
        {"producer": "test", "model_version": "test-v1", "is_fixture": False},
    )

    import os
    prev = os.environ.get("USI_ROOT")
    os.environ["USI_ROOT"] = str(root)
    try:
        demand, _backlog, demand_src, _backlog_src = _model_inputs_at_root(
            "borough", date(2024, 2, 23), date(2024, 2, 29), 7
        )
        assert demand is not None
        assert "test-v1" in demand_src

        # Demand has 2 rows: (BROOKLYN, HEAT/HOT WATER) and (BROOKLYN, Illegal Parking).
        # complaint_type == 'ALL' is explicitly excluded from category-level demand overrides.
        assert len(demand) == 2
        assert "ALL" not in demand["complaint_type"].values

        # Now simulate units that do not contain ALL
        units = pd.DataFrame([
            {"location": "BROOKLYN", "complaint_type": "HEAT/HOT WATER", "window_count": 350, "demand_value": 50.0, "backlog_value": 10.0},
            {"location": "BROOKLYN", "complaint_type": "Illegal Parking", "window_count": 420, "demand_value": 60.0, "backlog_value": 15.0},
            {"location": "BROOKLYN", "complaint_type": "Noise", "window_count": 140, "demand_value": 20.0, "backlog_value": 5.0},
        ])

        overridden, stats = scoring.apply_overrides(units, demand=demand)
        # Stats must report 2 units overridden (HEAT and Parking), Noise kept observed (20.0)
        assert stats["demand_model_units"] == 2
        heat_row = overridden[overridden["complaint_type"] == "HEAT/HOT WATER"].iloc[0]
        parking_row = overridden[overridden["complaint_type"] == "Illegal Parking"].iloc[0]
        noise_row = overridden[overridden["complaint_type"] == "Noise"].iloc[0]

        # demand_value is overridden by model
        assert heat_row["demand_value"] == 300.0
        assert parking_row["demand_value"] == 400.0
        assert noise_row["demand_value"] == 20.0

        # historical window_count remains untouched
        assert heat_row["window_count"] == 350
        assert parking_row["window_count"] == 420
        assert noise_row["window_count"] == 140

        # ALL is not in overridden units
        assert "ALL" not in overridden["complaint_type"].values
    finally:
        if prev is None:
            os.environ.pop("USI_ROOT", None)
        else:
            os.environ["USI_ROOT"] = prev


def test_override_preserves_historical_window_count_when_demand_differs(tmp_path):
    """Regression test: proving that window_count reflects historical observed records

    and remains unchanged even when demand_value is overridden by a different forecast.
    """
    from scripts.run_member3_priorities import _model_inputs_at_root

    root = tmp_path
    fc_rows = []
    start_date = date(2024, 3, 1)
    # Predicted demand is 500.0/day for 7 days (total 3500.0), which strongly differs
    # from historical observation (e.g., 50.0/day, window_count = 350).
    for h in range(7):
        target = start_date + timedelta(days=h)
        fc_rows.append({
            "forecast_date": str(target),
            "borough": "MANHATTAN",
            "complaint_type": "HEAT/HOT WATER",
            "predicted_count": 500.0,
            "training_cutoff": "2024-02-29",
            "model_version": "test-differing-v1",
        })

    fc_df = pd.DataFrame(fc_rows)
    contracts.write_table(
        fc_df,
        root,
        "demand_forecast",
        {"producer": "test", "model_version": "test-differing-v1", "is_fixture": False},
    )

    import os
    prev = os.environ.get("USI_ROOT")
    os.environ["USI_ROOT"] = str(root)
    try:
        demand, _backlog, _demand_src, _backlog_src = _model_inputs_at_root(
            "borough", date(2024, 2, 23), date(2024, 2, 29), 7
        )
        assert demand is not None
        assert demand["value"].iloc[0] == 500.0

        # Simulate observed units produced by observed_components() where window_count = 350
        units = pd.DataFrame([
            {
                "location": "MANHATTAN",
                "complaint_type": "HEAT/HOT WATER",
                "window_count": 350,  # historical observed count
                "demand_value": 50.0,  # historical daily mean (350 / 7)
                "backlog_value": 25.0,
            }
        ])

        overridden, stats = scoring.apply_overrides(units, demand=demand)
        assert stats["demand_model_units"] == 1

        row = overridden.iloc[0]
        # demand_value is successfully overridden to forecast value (500.0)
        assert row["demand_value"] == 500.0
        # window_count MUST strictly preserve the historical observed count (350), NOT forecast sum (3500)
        assert row["window_count"] == 350
        assert row["window_count"] != 500.0 * 7
    finally:
        if prev is None:
            os.environ.pop("USI_ROOT", None)
        else:
            os.environ["USI_ROOT"] = prev


def test_override_prevents_double_counting_even_if_units_contain_all(tmp_path):
    """Regression test: even if input units unexpectedly contain an 'ALL' row,

    category-level overrides never override or double-count borough totals.
    """
    from scripts.run_member3_priorities import _model_inputs_at_root

    root = tmp_path
    fc_rows = []
    start_date = date(2024, 3, 1)
    for h in range(7):
        target = start_date + timedelta(days=h)
        fc_rows.append({
            "forecast_date": str(target),
            "borough": "QUEENS",
            "complaint_type": "ALL",
            "predicted_count": 2000.0,
            "training_cutoff": "2024-02-29",
            "model_version": "test-v2",
        })
        fc_rows.append({
            "forecast_date": str(target),
            "borough": "QUEENS",
            "complaint_type": "Noise - Residential",
            "predicted_count": 150.0,
            "training_cutoff": "2024-02-29",
            "model_version": "test-v2",
        })

    fc_df = pd.DataFrame(fc_rows)
    contracts.write_table(
        fc_df,
        root,
        "demand_forecast",
        {"producer": "test", "model_version": "test-v2", "is_fixture": False},
    )

    import os
    prev = os.environ.get("USI_ROOT")
    os.environ["USI_ROOT"] = str(root)
    try:
        demand, _backlog, _demand_src, _backlog_src = _model_inputs_at_root(
            "borough", date(2024, 2, 23), date(2024, 2, 29), 7
        )
        assert demand is not None
        # Demand must NOT contain 'ALL'
        assert "ALL" not in demand["complaint_type"].values
        assert len(demand) == 1
        assert demand["complaint_type"].iloc[0] == "Noise - Residential"

        # Simulate corrupt/unexpected input units that include an 'ALL' row
        units = pd.DataFrame([
            {"location": "QUEENS", "complaint_type": "ALL", "demand_value": 800.0, "backlog_value": 100.0},
            {"location": "QUEENS", "complaint_type": "Noise - Residential", "demand_value": 40.0, "backlog_value": 10.0},
        ])

        overridden, stats = scoring.apply_overrides(units, demand=demand)

        # Only Noise is overridden; ALL remains untouched (keeps 800.0)
        assert stats["demand_model_units"] == 1
        noise_val = overridden[overridden["complaint_type"] == "Noise - Residential"]["demand_value"].iloc[0]
        all_val = overridden[overridden["complaint_type"] == "ALL"]["demand_value"].iloc[0]

        assert noise_val == 150.0
        assert all_val == 800.0  # NOT replaced with 2000.0 from demand_forecast
    finally:
        if prev is None:
            os.environ.pop("USI_ROOT", None)
        else:
            os.environ["USI_ROOT"] = prev
