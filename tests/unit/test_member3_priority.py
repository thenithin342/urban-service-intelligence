"""Unit tests for Member 3 priority scoring and backtest (pure pandas). ASCII only."""

from __future__ import annotations

from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from src.prioritization import aggregate, backtest, scoring
from src.prioritization.scoring import ScoringConfig

D0 = date(2024, 1, 1)


def _records(rows: list[tuple]) -> pd.DataFrame:
    """rows: (created datetime, closed datetime|None, borough, complaint_type)."""
    out = []
    for i, (created, closed, borough, ctype) in enumerate(rows):
        hours = None
        if closed is not None and closed >= created:
            hours = (closed - created).total_seconds() / 3600.0
        out.append(
            {
                "unique_key": str(i),
                "created_at": created,
                "closed_at": closed,
                "created_day": created.date(),
                "complaint_type": ctype,
                "borough": borough,
                "incident_zip": "10001",
                "latitude": 40.7,
                "longitude": -73.9,
                "resolution_hours": hours,
            }
        )
    return pd.DataFrame(out)


def _t(day: int, hour: int = 12) -> datetime:
    return datetime(2024, 1, 1, hour) + timedelta(days=day)


# ------------------------------------------------------------ normalization


def test_minmax_basic_and_bounds():
    out = scoring.minmax(pd.Series([2.0, 4.0, 6.0]))
    assert out.tolist() == [0.0, 0.5, 1.0]


def test_minmax_constant_and_null_columns_are_zero():
    assert scoring.minmax(pd.Series([3.0, 3.0, 3.0])).tolist() == [0.0, 0.0, 0.0]
    assert scoring.minmax(pd.Series([None, None], dtype=float)).tolist() == [0.0, 0.0]
    assert scoring.minmax(pd.Series([1.0, None, 3.0])).tolist() == [0.0, 0.0, 1.0]


# ------------------------------------------------------------ weights


def test_validate_weights_accepts_default():
    assert scoring.validate_weights(scoring.DEFAULT_WEIGHTS) == {
        "demand": 0.5, "backlog": 0.3, "delay": 0.2,
    }


@pytest.mark.parametrize(
    "weights",
    [
        {"demand": 0.5, "backlog": 0.3, "delay": 0.3},  # sums to 1.1
        {"demand": 1.2, "backlog": -0.2, "delay": 0.0},  # negative
        {"demand": 0.5, "backlog": 0.5},  # missing key
        {"demand": 0.4, "backlog": 0.3, "delay": 0.2, "extra": 0.1},  # extra key
        {"demand": "a", "backlog": 0.5, "delay": 0.5},  # not a number
        {"demand": float("nan"), "backlog": 0.5, "delay": 0.5},
    ],
)
def test_validate_weights_rejects_invalid(weights):
    with pytest.raises(ValueError):
        scoring.validate_weights(weights)


# ------------------------------------------------------------ scoring / ranking


def _units(rows):
    return pd.DataFrame(
        rows, columns=["location", "complaint_type", "demand_value", "backlog_value", "delay_value"]
    )


def test_score_is_weighted_sum_of_normalized_components():
    u = _units([("A", "x", 10.0, 0.0, 5.0), ("B", "x", 0.0, 4.0, 15.0), ("C", "x", 5.0, 2.0, 10.0)])
    out = scoring.score_units(u, {"demand": 0.5, "backlog": 0.3, "delay": 0.2}).set_index("location")
    assert out.loc["A", "priority_score"] == pytest.approx(0.5)
    assert out.loc["B", "priority_score"] == pytest.approx(0.5)
    assert out.loc["C", "priority_score"] == pytest.approx(0.25 + 0.15 + 0.1)
    for comp in scoring.COMPONENTS:
        assert out[f"{comp}_component"].between(0, 1).all()


def test_ranking_ties_are_deterministic_and_unique():
    u = _units([("B", "y", 1.0, 1.0, 1.0), ("A", "z", 1.0, 1.0, 1.0), ("A", "y", 1.0, 1.0, 1.0)])
    out = scoring.score_units(u, scoring.DEFAULT_WEIGHTS)
    assert out["rank"].tolist() == [1, 2, 3]
    assert list(zip(out["location"], out["complaint_type"], strict=True)) == [
        ("A", "y"), ("A", "z"), ("B", "y"),
    ]
    again = scoring.score_units(u.iloc[::-1], scoring.DEFAULT_WEIGHTS)
    pd.testing.assert_frame_equal(out, again)


def test_null_delay_contributes_zero():
    u = _units([("A", "x", 1.0, 1.0, None), ("B", "x", 2.0, 1.0, 9.0)])
    out = scoring.score_units(u, scoring.DEFAULT_WEIGHTS).set_index("location")
    assert out.loc["A", "delay_component"] == 0.0


def test_binned_median_interpolates_within_bin():
    # 4 records in bin [12, 18): median at the middle of the bin.
    b = scoring.hours_bin(pd.Series([12.0, 13.0, 16.0, 17.9]))
    assert set(b) == {scoring.HOUR_EDGES.index(12.0)}
    assert scoring.binned_median(b, pd.Series([1, 1, 1, 1])) == pytest.approx(15.0)
    # Open-ended last bin returns its lower edge.
    top = scoring.hours_bin(pd.Series([10000.0]))
    assert scoring.binned_median(top, pd.Series([3])) == 8760.0


def test_hours_bin_edges():
    b = scoring.hours_bin(pd.Series([0.0, 0.49, 0.5, 24.0, 8760.0]))
    assert b.tolist() == [0, 0, 1, scoring.HOUR_EDGES.index(24.0), len(scoring.HOUR_EDGES) - 2]


# ------------------------------------------------------------ observed components


def test_backlog_counts_records_open_at_cutoff():
    recs = _records(
        [
            (_t(0), _t(1), "BRONX", "Noise"),  # closed before cut-off (day 3)
            (_t(1), _t(5), "BRONX", "Noise"),  # closed after cut-off: open at T
            (_t(2), None, "BRONX", "Noise"),  # never closed: open at T
            (_t(2), _t(2, 13), "BRONX", "Heat"),
        ]
    )
    agg = aggregate.aggregate_pandas(recs)
    cfg = ScoringConfig(window_days=3, min_delay_records=1)
    units = scoring.observed_components(agg["created"], agg["delay"], D0 + timedelta(days=2), cfg)
    u = units.set_index("complaint_type")
    assert u.loc["Noise", "window_count"] == 3
    assert u.loc["Noise", "backlog_value"] == 2
    assert u.loc["Noise", "demand_value"] == pytest.approx(1.0)
    assert u.loc["Heat", "backlog_value"] == 0
    # Delay uses only records closed before the cut-off: Noise has the 24h one.
    assert u.loc["Noise", "delay_records"] == 1
    assert u.loc["Heat", "delay_value"] == pytest.approx(1.0, abs=0.5)


def test_min_delay_records_nulls_sparse_delay():
    recs = _records([(_t(0), _t(0, 14), "QUEENS", "Noise")])
    agg = aggregate.aggregate_pandas(recs)
    units = scoring.observed_components(
        agg["created"], agg["delay"], D0, ScoringConfig(window_days=1, min_delay_records=5)
    )
    assert units["delay_value"].isna().all()


def test_negative_duration_counts_as_closed_and_has_no_delay():
    recs = _records([(_t(1), _t(0), "BRONX", "Noise")])
    agg = aggregate.aggregate_pandas(recs)
    assert agg["created"]["close_lag_days"].tolist() == [0]
    assert agg["delay"].empty


def test_records_without_location_are_excluded():
    recs = _records([(_t(0), None, None, "Noise"), (_t(0), None, "BRONX", "Noise")])
    agg = aggregate.aggregate_pandas(recs)
    assert agg["records"] == 2 and agg["records_without_location"] == 1


def test_trim_partial_tail():
    totals = pd.Series([100, 110, 90, 105, 5], index=[D0 + timedelta(days=i) for i in range(5)])
    kept, dropped = aggregate.trim_partial_tail(totals, 0.5)
    assert dropped == [D0 + timedelta(days=4)]
    assert len(kept) == 4


def test_overrides_replace_only_matching_units():
    u = _units([("BRONX", "x", 1.0, 1.0, 1.0), ("QUEENS", "x", 2.0, 2.0, 2.0)])
    demand = pd.DataFrame({"location": ["BRONX"], "complaint_type": ["x"], "value": [9.0]})
    out, stats = scoring.apply_overrides(u, demand=demand)
    assert out["demand_value"].tolist() == [9.0, 2.0]
    assert stats == {"demand_model_units": 1, "backlog_model_units": 0}


# ------------------------------------------------------------ backtest


def test_precision_at_k_math():
    pred = ["a", "b", "c", "d"]
    truth = ["b", "a", "d", "c"]
    assert backtest.precision_at_k(pred, truth, 1) == 0.0
    assert backtest.precision_at_k(pred, truth, 2) == 1.0
    assert backtest.precision_at_k(pred, truth, 3) == pytest.approx(2 / 3)
    assert backtest.precision_at_k(pred, truth, 4) == 1.0
    with pytest.raises(ValueError):
        backtest.precision_at_k(pred, truth, 5)
    with pytest.raises(ValueError):
        backtest.precision_at_k(pred, truth, 0)


def test_spearman_values():
    assert backtest.spearman(pd.Series([1, 2, 3]), pd.Series([10, 20, 30])) == pytest.approx(1.0)
    assert backtest.spearman(pd.Series([1, 2, 3]), pd.Series([3, 2, 1])) == pytest.approx(-1.0)
    assert backtest.spearman(pd.Series([1, 1, 1]), pd.Series([1, 2, 3])) is None


def test_backtest_windows_stay_inside_data():
    wins = list(backtest.backtest_windows(D0, D0 + timedelta(days=20), 7, 7))
    assert [w[1] for w in wins] == [D0 + timedelta(days=6), D0 + timedelta(days=13)]
    for _ws, we, ts, te in wins:
        assert ts == we + timedelta(days=1)
        assert te <= D0 + timedelta(days=20)


def _synthetic(spike: bool) -> pd.DataFrame:
    rng = np.random.default_rng(0)
    rows = []
    units = [("BRONX", "Heat"), ("BRONX", "Noise"), ("QUEENS", "Heat"), ("QUEENS", "Noise"),
             ("BROOKLYN", "Heat"), ("BROOKLYN", "Noise")]
    for day in range(14):
        for j, (b, c) in enumerate(units):
            for _ in range(int(rng.integers(1, 6)) + j):
                closed = _t(day, 10) + timedelta(hours=float(rng.integers(1, 200)))
                rows.append((_t(day, 9), closed if rng.random() < 0.8 else None, b, c))
    if spike:
        # Future-only events: a huge spike created in the target window and
        # many very slow closures, none of which may move window-t ranks.
        rows += [(_t(9, 8), None, "BRONX", "Heat")] * 1000
        rows += [(_t(8, 8), _t(13, 8), "QUEENS", "Noise")] * 300
    return _records(rows)


def test_no_future_data_leakage_in_backtest_ranks():
    cfg = ScoringConfig(window_days=7, min_delay_records=1)
    base = aggregate.aggregate_pandas(_synthetic(False))
    spiked = aggregate.aggregate_pandas(_synthetic(True))
    we = D0 + timedelta(days=6)
    r1, _ = scoring.score_window(base["created"], base["delay"], we, cfg)
    r2, _ = scoring.score_window(spiked["created"], spiked["delay"], we, cfg)
    pd.testing.assert_frame_equal(r1, r2)

    assert r1["rank"].tolist() == r2["rank"].tolist()

    # The spike only changes the evaluation target (next-window truth).
    t2 = backtest.target_counts(spiked["created"], we + timedelta(days=1), we + timedelta(days=7))
    top = t2.sort_values("target_count", ascending=False).iloc[0]
    assert (top["location"], top["complaint_type"]) == ("BRONX", "Heat")
    bt = backtest.run_backtest(spiked["created"], spiked["delay"], D0, D0 + timedelta(days=13),
                               cfg, [2], 7, seed=1)
    assert len(bt) == 3 and set(bt["window_end"]) == {we}


def test_backtest_uses_next_window_volume_as_truth():
    rows = []
    # Window days 0-1: A busiest. Target days 2-3: B busiest.
    for day, counts in [(0, {"A": 5, "B": 1}), (1, {"A": 5, "B": 1}),
                        (2, {"A": 1, "B": 9}), (3, {"A": 1, "B": 9})]:
        for loc, n in counts.items():
            rows += [(_t(day), None, loc, "x")] * n
    agg = aggregate.aggregate_pandas(_records(rows))
    cfg = ScoringConfig(window_days=2, weights={"demand": 1.0, "backlog": 0.0, "delay": 0.0})
    bt = backtest.run_backtest(agg["created"], agg["delay"], D0, D0 + timedelta(days=3),
                               cfg, [1], 2, seed=0)
    got = bt.set_index("method")
    assert got.loc["volume_only", "precision_at_k"] == 0.0
    assert got.loc["priority_score", "precision_at_k"] == 0.0
    assert got.loc["volume_only", "spearman"] == pytest.approx(-1.0)
    assert set(bt["target_start"]) == {D0 + timedelta(days=2)}


def test_summarize_means():
    bt = pd.DataFrame(
        {"method": ["m", "m"], "k": [1, 1], "precision_at_k": [0.0, 1.0],
         "spearman": [0.5, None], "n_units": [4, 6]}
    )
    s = backtest.summarize(bt)
    assert s.loc[0, "mean_precision_at_k"] == 0.5
    assert s.loc[0, "mean_spearman"] == 0.5
    assert s.loc[0, "windows"] == 2
