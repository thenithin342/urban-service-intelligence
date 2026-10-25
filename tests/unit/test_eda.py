"""Unit tests for Phase 1 EDA logic. ASCII only."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.analytics.eda import (
    compute_profile,
    counts_by_borough,
    normalize_frame,
    save_eda_outputs,
    top_complaint_categories,
    volume_over_time,
)

FIXTURE = Path(__file__).parents[1] / "fixtures" / "sample_311.csv"


def load_fixture() -> pd.DataFrame:
    return pd.read_csv(FIXTURE, dtype=str, keep_default_na=True)


def test_top_categories_counts():
    df = load_fixture()
    top = top_complaint_categories(df, n=10)
    # Noise - Residential appears 3x in raw (incl. duplicate row 1)
    row = top[top["complaint_type"] == "Noise - Residential"]
    assert not row.empty
    assert int(row.iloc[0]["n"]) == 3


def test_borough_normalization():
    df = load_fixture()
    by = counts_by_borough(df)
    # 'Brooklyn' lowercase variant merges into BROOKLYN (2 + 1 dup = 3 rows)
    brook = by[by["borough"] == "BROOKLYN"]
    assert not brook.empty
    assert int(brook.iloc[0]["n"]) == 3
    # missing borough becomes UNKNOWN
    unk = by[by["borough"] == "UNKNOWN"]
    assert not unk.empty


def test_volume_over_time_sorted_and_counts():
    df = load_fixture()
    vol = volume_over_time(df, freq="D")
    assert list(vol["period"]) == sorted(vol["period"].tolist())
    # 7 rows have valid created_date (rows with id 1,2,3,4,1dup,7,8)
    assert int(vol["n"].sum()) == 7


def test_weekly_volume_handles_sunday_monday_boundary():
    frame = pd.DataFrame({"created_date": ["2024-01-07T23:59:00", "2024-01-08T00:00:00"]})
    weekly = volume_over_time(frame, freq="W")
    assert weekly.to_dict("records") == [{"period": "2024-01-01/2024-01-07", "n": 1},
                                          {"period": "2024-01-08/2024-01-14", "n": 1}]


def test_profile_reports_duplicates_nulls_invalid():
    df = load_fixture()
    prof = compute_profile(df)
    assert prof["row_count"] == 8
    assert prof["duplicate_count"] == 1
    assert prof["created_nat"] == 1  # row id 5 has no created_date
    assert prof["closed_before_created"] == 1  # row id 3
    assert prof["invalid_or_missing_coords"] >= 2  # row 5 + row 7 outside box


def test_save_eda_outputs_deterministic(tmp_path):
    df = load_fixture()
    a = save_eda_outputs(df, tmp_path / "a")
    b = save_eda_outputs(df, tmp_path / "b")
    for k in ("top", "borough", "volume", "profile"):
        ta = Path(a[k]).read_text(encoding="utf-8")
        tb = Path(b[k]).read_text(encoding="utf-8")
        assert ta == tb


def test_normalize_frame_missing_columns():
    df = pd.DataFrame({"unique_key": ["1"]})
    w = normalize_frame(df)
    assert "borough_std" in w.columns
    assert w.iloc[0]["borough_std"] == "UNKNOWN"
