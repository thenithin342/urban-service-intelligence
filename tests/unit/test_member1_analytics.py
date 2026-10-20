"""Member 1 Phase 3 unit tests (no Spark). ASCII only."""

from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.analytics import figures, geospatial
from src.analytics.member1_config import member1_config


def test_member1_config_defaults_and_override():
    cfg = member1_config({})
    assert cfg["grid_cell_deg"] == 0.01
    assert cfg["kmeans_seed"] == 42
    over = member1_config(
        {"member1": {"grid_cell_deg": 0.02, "kmeans_k_values": [3, 4]}}
    )
    assert over["grid_cell_deg"] == 0.02
    assert over["kmeans_k_values"] == [3, 4]


@pytest.mark.parametrize(
    "block",
    [
        {"grid_cell_deg": 0},
        {"grid_cell_deg": -1},
        {"dbscan_eps_meters": 0},
        {"dbscan_min_pts": 0},
        {"kmeans_k_values": [1]},
        {"kmeans_k_values": []},
    ],
)
def test_member1_config_rejects_invalid(block):
    with pytest.raises(ValueError):
        member1_config({"member1": block})


def test_member1_config_rejects_non_mapping():
    with pytest.raises(TypeError):
        member1_config({"member1": [1, 2]})  # type: ignore[arg-type]


def test_projection_roundtrip():
    lat0, lon0 = 40.7, -73.95
    for lat, lon in [(40.75, -73.98), (40.6, -74.0), (40.85, -73.85)]:
        x = (lon - lon0) * geospatial.DEG_LON_KM_AT_EQ * 1000.0 * math.cos(
            math.radians(lat0)
        )
        y = (lat - lat0) * geospatial.DEG_LAT_KM * 1000.0
        back_lat, back_lon = geospatial.cluster_centers_to_latlon(x, y, lat0, lon0)
        assert back_lat == pytest.approx(lat, abs=1e-6)
        assert back_lon == pytest.approx(lon, abs=1e-6)


def test_figures_empty_inputs_return_none(tmp_path: Path):
    empty = pd.DataFrame()
    for fn in (
        figures.bar_borough,
        figures.bar_categories,
        figures.timeseries_daily,
        figures.heatmap_hour_weekday,
        figures.kde_daily_volume,
    ):
        path, takeaway = fn(empty, tmp_path / f"{fn.__name__}.png")
        assert path is None
        assert "skipped" in takeaway


def test_figures_bar_and_hist_create_files(tmp_path: Path):
    bor = pd.DataFrame(
        {"borough": ["BROOKLYN", "QUEENS"], "complaint_count": [500, 300]}
    )
    p1, t1 = figures.bar_borough(bor, tmp_path / "bor.png")
    assert p1 is not None and Path(p1).exists() and "BROOKLYN" in t1

    res = pd.DataFrame(
        {"resolution_hours": np.linspace(0, 40, 200), "borough": ["BRONX"] * 200}
    )
    p2, t2 = figures.histogram_resolution(res, tmp_path / "res.png")
    assert p2 is not None and Path(p2).exists() and "Median" in t2


def test_figures_heatmap_and_radar(tmp_path: Path):
    days = ["Monday", "Tuesday", "Wednesday"]
    rows = [
        {"weekday": d, "hour": h, "complaint_count": (i + 1) * 10}
        for i, d in enumerate(days)
        for h in range(24)
    ]
    p, t = figures.heatmap_hour_weekday(pd.DataFrame(rows), tmp_path / "heat.png")
    assert p is not None and Path(p).exists() and "Peak cell" in t

    bc = pd.DataFrame(
        {
            "borough": ["BRONX", "BRONX", "QUEENS", "QUEENS"],
            "complaint_type": ["Noise", "Water", "Noise", "Water"],
            "complaint_count": [10, 5, 20, 15],
        }
    )
    p2, t2 = figures.radar_borough_profile(bc, tmp_path / "radar.png")
    assert p2 is not None and Path(p2).exists() and "normalised" in t2
