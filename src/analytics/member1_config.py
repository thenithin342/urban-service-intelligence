"""Member 1 Phase 3 settings: defaults + YAML resolution. ASCII only.

The optional ``member1:`` block in a config overrides these defaults.
Bounds here (k values, DBSCAN sample cap, collect guard) are driver
safety limits, not source-record sampling.
"""

from __future__ import annotations

from typing import Any

MEMBER1_DEFAULTS: dict[str, Any] = {
    "analytics_outdir": "artifacts/member1/analytics",
    "top_categories": 20,
    "grid_cell_deg": 0.01,
    "projection_lat0": 40.7,
    "projection_lon0": -73.95,
    "hotspot_min_count": 50,
    "kmeans_k_values": [2, 3, 4, 5, 6, 8],
    "kmeans_seed": 42,
    "kmeans_variant": "log_count_scaled",
    "dbscan_eps_meters": 250.0,
    "dbscan_eps_grid": [100, 250, 500, 1000],
    "dbscan_min_pts": 25,
    "dbscan_sample_cap": 20000,
    "dbscan_seed": 42,
    "max_collect_rows": 20000,
    "producer_version": "member1-phase3-v1",
}

# Keys whose scalar values must be validated (positive/consistent).
_POSITIVE_INTS = [
    "top_categories",
    "hotspot_min_count",
    "kmeans_seed",
    "dbscan_min_pts",
    "dbscan_sample_cap",
    "dbscan_seed",
    "max_collect_rows",
]


def member1_config(cfg: dict[str, Any]) -> dict[str, Any]:
    """Merge the ``member1`` block over defaults and validate."""
    out = dict(MEMBER1_DEFAULTS)
    block = cfg.get("member1") or {}
    if not isinstance(block, dict):
        raise TypeError("config 'member1' block must be a mapping")
    out.update(block)
    out["analytics_outdir"] = str(out["analytics_outdir"])
    out["grid_cell_deg"] = float(out["grid_cell_deg"])
    out["dbscan_eps_meters"] = float(out["dbscan_eps_meters"])
    out["projection_lat0"] = float(out["projection_lat0"])
    out["projection_lon0"] = float(out["projection_lon0"])
    out["kmeans_k_values"] = [int(k) for k in out["kmeans_k_values"]]
    out["dbscan_eps_grid"] = [float(e) for e in out["dbscan_eps_grid"]]
    out["kmeans_variant"] = str(out["kmeans_variant"])
    for key in _POSITIVE_INTS:
        out[key] = int(out[key])
    if out["grid_cell_deg"] <= 0:
        raise ValueError("member1.grid_cell_deg must be > 0")
    if out["dbscan_eps_meters"] <= 0:
        raise ValueError("member1.dbscan_eps_meters must be > 0")
    if out["dbscan_min_pts"] < 1:
        raise ValueError("member1.dbscan_min_pts must be >= 1")
    if out["dbscan_sample_cap"] < 1:
        raise ValueError("member1.dbscan_sample_cap must be >= 1")
    if out["max_collect_rows"] < 1:
        raise ValueError("member1.max_collect_rows must be >= 1")
    if not out["kmeans_k_values"] or any(k < 2 for k in out["kmeans_k_values"]):
        raise ValueError("member1.kmeans_k_values must contain k >= 2")
    if not out["dbscan_eps_grid"] or any(e <= 0 for e in out["dbscan_eps_grid"]):
        raise ValueError("member1.dbscan_eps_grid must contain positive eps")
    valid_variants = {"log_count_scaled", "log_count_unscaled", "count_scaled"}
    if out["kmeans_variant"] not in valid_variants:
        raise ValueError(
            f"member1.kmeans_variant must be one of {sorted(valid_variants)}"
        )
    return out
