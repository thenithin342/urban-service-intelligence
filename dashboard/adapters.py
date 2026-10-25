"""Dashboard data adapters (Member 3). ASCII only.

Single place where the Streamlit app reads analytics outputs. Rules:

- Only small aggregate tables and JSON manifests are read
  (pandas.read_parquet on artifact directories). The raw CSV and the cleaned
  record-level Parquet are never loaded by the app.
- Shared contract tables (docs/data_contracts.md) are validated with
  src.utils.contracts.validate after reading. A table that violates its
  contract is returned with source "invalid" and pages refuse to render it.
- Member 1 Phase 3 tables are checked for the required columns listed in
  docs/member1/artifact_contract.md.
- When a contract table has not been produced yet, the labelled synthetic
  fixture in tests/fixtures/contracts/ is returned with source "fixture".
  Fixture values are invented and must always be labelled in the UI.

Environment overrides (used by tests):
  USI_ROOT               repository root holding data/ (default: this repo)
  USI_ARTIFACTS_ROOT     artifacts root (default: <USI_ROOT>/artifacts)
  USI_FIXTURES_DIR       contract fixture directory
  USI_DISABLE_FIXTURES   "1" disables the fixture fallback
"""

from __future__ import annotations

import json
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.utils import contracts

try:  # Streamlit is optional for CLI use (scripts/check_outputs.py).
    import streamlit as st

    if not st.runtime.exists():  # CLI use: cached readers work, silence the notice
        logging.getLogger("streamlit.runtime.caching.cache_data_api").setLevel(logging.ERROR)
    _cache = st.cache_data(show_spinner=False, max_entries=256)
except ImportError:  # pragma: no cover - streamlit is pinned in requirements
    def _cache(func):  # type: ignore[no-redef]
        return func


REAL = "real"
FIXTURE = "fixture"
MISSING = "missing"
INVALID = "invalid"

NYC_BBOX = {"lat_min": 40.49, "lat_max": 40.92, "lon_min": -74.26, "lon_max": -73.70}

# Required columns of Member 1 Phase 3 tables (docs/member1/artifact_contract.md).
MEMBER1_COLUMNS: dict[str, list[str]] = {
    "complaints_by_borough": ["borough", "complaint_count"],
    "complaints_by_category": ["complaint_type", "complaint_count"],
    "complaints_by_agency": ["agency", "complaint_count"],
    "complaints_by_zip": ["incident_zip", "complaint_count"],
    "borough_category": ["borough", "complaint_type", "complaint_count"],
    "complaints_daily": ["date", "complaint_count"],
    "complaints_daily_borough": ["date", "borough", "complaint_count"],
    "complaints_weekly": ["week_start", "complaint_count"],
    "hour_weekday": [
        "weekday",
        "hour",
        "complaint_count",
        "days_observed",
        "avg_complaints_per_hour",
    ],
    "resolution_by_borough": [
        "borough",
        "resolved_rows",
        "mean_hours",
        "median_hours",
        "p90_hours",
        "p99_hours",
        "max_hours",
    ],
    "flag_overlap": ["flag_a", "flag_b", "rows"],
    "spatial_hotspots": [
        "spatial_id",
        "center_lat",
        "center_lon",
        "complaint_count",
        "cell_area_km2",
        "density_per_km2",
    ],
    "zip_hotspots": ["zip", "complaint_count"],
    "spatial_grid_cells": [
        "spatial_id",
        "center_lat",
        "center_lon",
        "complaint_count",
        "dominant_borough",
        "dominant_complaint",
        "x_m",
        "y_m",
        "cluster",
    ],
    "kmeans_ablation": ["variant", "k", "silhouette", "wssse", "use_log_count", "scaled"],
    "cluster_summary": [
        "cluster",
        "cells",
        "complaints",
        "center_lat",
        "center_lon",
        "mean_complaints_per_cell",
        "mean_resolution_hours",
        "dominant_borough",
        "dominant_complaint",
    ],
}


@dataclass
class TableResult:
    """A loaded table plus where it came from."""

    name: str
    df: pd.DataFrame
    source: str  # real | fixture | missing | invalid
    meta: dict[str, Any] = field(default_factory=dict)
    problems: list[str] = field(default_factory=list)
    path: str = ""

    @property
    def usable(self) -> bool:
        return self.source in (REAL, FIXTURE) and not self.problems

    @property
    def is_fixture(self) -> bool:
        return self.source == FIXTURE


# ---------------------------------------------------------------- roots


def repo_root() -> Path:
    env = os.environ.get("USI_ROOT")
    return Path(env) if env else CODE_ROOT


def artifacts_root() -> Path:
    env = os.environ.get("USI_ARTIFACTS_ROOT")
    return Path(env) if env else repo_root() / "artifacts"


def fixtures_dir() -> Path | None:
    if os.environ.get("USI_DISABLE_FIXTURES") == "1":
        return None
    env = os.environ.get("USI_FIXTURES_DIR")
    return Path(env) if env else CODE_ROOT / "tests" / "fixtures" / "contracts"


def member1_dir() -> Path:
    return artifacts_root() / "member1" / "analytics"


def contract_dir(name: str) -> Path:
    rel = contracts.CONTRACTS[name].path
    prefix = contracts.ART + "/"
    rel = rel.removeprefix(prefix)
    return artifacts_root() / rel


def contract_meta_path(name: str) -> Path:
    p = contract_dir(name)
    return p.with_name(p.name + ".meta.json")


# ---------------------------------------------------------------- cached io


def _mtime(path: Path) -> float:
    try:
        if path.is_dir():
            stamps = [p.stat().st_mtime for p in path.rglob("*") if p.is_file()]
            return max(stamps) if stamps else path.stat().st_mtime
        return path.stat().st_mtime
    except OSError:
        return 0.0


@_cache
def _read_parquet_cached(path: str, mtime: float) -> pd.DataFrame:
    del mtime  # cache key only: re-read when the files change
    return pd.read_parquet(path)


@_cache
def _read_json_cached(path: str, mtime: float) -> Any:
    del mtime
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def read_json(path: Path) -> Any | None:
    if not path.is_file():
        return None
    try:
        return _read_json_cached(str(path), _mtime(path))
    except (OSError, ValueError):
        return None


def _has_parquet(path: Path) -> bool:
    return path.is_dir() and any(path.rglob("*.parquet"))


def clear_caches() -> None:
    for fn in (_read_parquet_cached, _read_json_cached):
        clear = getattr(fn, "clear", None)
        if clear:
            clear()


# ---------------------------------------------------------------- member 1


def member1_manifest() -> dict[str, Any]:
    return read_json(member1_dir() / "phase3_manifest.json") or {}


def member1_metrics() -> dict[str, Any]:
    return read_json(member1_dir() / "phase3_metrics.json") or {}


def etl_quality() -> dict[str, Any]:
    return read_json(repo_root() / "data" / "processed" / "etl_quality.json") or {}


def load_member1(name: str) -> TableResult:
    """Read one Member 1 Phase 3 aggregate table (real or missing)."""
    cols = MEMBER1_COLUMNS[name]
    path = member1_dir() / "tables" / name
    manifest = member1_manifest()
    meta = {
        "producer": manifest.get("producer_version", ""),
        "coverage_start": (manifest.get("period") or {}).get("period_start"),
        "coverage_end": (manifest.get("period") or {}).get("period_end"),
        "row_count": manifest.get("row_count"),
        "is_fixture": False,
    }
    if not _has_parquet(path):
        return TableResult(name, pd.DataFrame(columns=cols), MISSING, meta, [], str(path))
    try:
        df = _read_parquet_cached(str(path), _mtime(path))
    except (OSError, ValueError) as exc:  # corrupt or unreadable output
        return TableResult(
            name, pd.DataFrame(columns=cols), INVALID, meta, [f"{name}: {exc}"], str(path)
        )
    missing = [c for c in cols if c not in df.columns]
    problems = [f"{name}: missing columns {missing}"] if missing else []
    return TableResult(name, df, INVALID if problems else REAL, meta, problems, str(path))


# ---------------------------------------------------------------- contracts


def _fixture(name: str) -> TableResult | None:
    fdir = fixtures_dir()
    if fdir is None:
        return None
    fpath = fdir / f"{name}.json"
    data = read_json(fpath)
    if not data:
        return None
    meta = dict(data.get("meta") or {})
    meta["is_fixture"] = True  # fixtures are always labelled, whatever the file says
    df = pd.DataFrame(data.get("rows") or [])
    problems = contracts.validate(df, name)
    return TableResult(name, df, INVALID if problems else FIXTURE, meta, problems, str(fpath))


def empty_contract_frame(name: str) -> pd.DataFrame:
    return pd.DataFrame(columns=list(contracts.CONTRACTS[name].columns))


def load_contract(name: str, allow_fixture: bool = True) -> TableResult:
    """Read a shared contract table, validate it, fall back to its fixture."""
    path = contract_dir(name)
    if _has_parquet(path):
        meta = read_json(contract_meta_path(name)) or {}
        problems: list[str] = []
        if not meta:
            problems.append(f"{name}: missing sibling meta JSON {contract_meta_path(name).name}")
        try:
            df = _read_parquet_cached(str(path), _mtime(path))
        except (OSError, ValueError) as exc:
            return TableResult(
                name, empty_contract_frame(name), INVALID, meta, [f"{name}: {exc}"], str(path)
            )
        problems += contracts.validate(df, name)
        if problems:
            source = INVALID
        elif meta.get("is_fixture") is True:
            source = FIXTURE
        else:
            source = REAL
        return TableResult(name, df, source, meta, problems, str(path))
    if allow_fixture:
        fx = _fixture(name)
        if fx is not None:
            return fx
    return TableResult(name, empty_contract_frame(name), MISSING, {}, [], str(path))


# ---------------------------------------------------------------- helpers


def coverage(result: TableResult) -> tuple[str | None, str | None]:
    """(coverage_start, coverage_end) from meta or the table itself."""
    meta = result.meta or {}
    start, end = meta.get("coverage_start"), meta.get("coverage_end")
    df = result.df
    pairs = [
        ("coverage_start", "coverage_end"),
        ("window_start", "window_end"),
        ("prediction_window_start", "prediction_window_end"),
        ("eval_start", "eval_end"),
    ]
    if (start is None or end is None) and not df.empty:
        for a, b in pairs:
            if a in df.columns and b in df.columns:
                start = start or str(pd.to_datetime(df[a]).min().date())
                end = end or str(pd.to_datetime(df[b]).max().date())
                break
    return (str(start) if start else None, str(end) if end else None)


def coverage_note(start: str | None, end: str | None) -> str:
    """Short caveat describing how much history a coverage range represents."""
    if not start or not end:
        return "coverage dates not recorded"
    try:
        days = (date.fromisoformat(end[:10]) - date.fromisoformat(start[:10])).days + 1
    except ValueError:
        return f"{start} to {end}"
    if days < 60:
        caveat = "short sample window - not seasonal conclusions"
    elif days < 365:
        caveat = "multi-month window - not a full annual cycle"
    else:
        caveat = "multi-year window"
    return f"{start} to {end} ({days} days; {caveat})"


def in_bbox(df: pd.DataFrame, lat: str = "center_lat", lon: str = "center_lon") -> pd.Series:
    b = NYC_BBOX
    return (
        df[lat].between(b["lat_min"], b["lat_max"])
        & df[lon].between(b["lon_min"], b["lon_max"])
    )


def valid_map_points(df: pd.DataFrame, lat: str = "center_lat", lon: str = "center_lon"):
    """Rows with non-null coordinates inside the NYC bounding box."""
    if df.empty or lat not in df.columns or lon not in df.columns:
        return df.iloc[0:0]
    keep = df[lat].notna() & df[lon].notna()
    out = df[keep]
    out = out[in_bbox(out, lat, lon)]
    assert bool(in_bbox(out, lat, lon).all()), "out-of-bbox point reached the map"
    return out


def list_text(value: Any) -> str:
    """Render a Parquet/JSON list cell as 'a + b'."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    try:
        return " + ".join(str(v) for v in list(value))
    except TypeError:
        return str(value)


# ---------------------------------------------------------------- freshness


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        ts = pd.Timestamp(value)
    except (ValueError, TypeError):
        return None
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.to_pydatetime()


def cleaned_inputs() -> list[dict[str, Any]]:
    """Known cleaned datasets: path, ready flag, coverage end, write time."""
    root = repo_root()
    out: list[dict[str, Any]] = []
    metrics = member1_metrics()
    sample_end = (metrics.get("coverage") or {}).get("period_end")
    hist_q = read_json(artifacts_root() / "member1" / "phase4" / "etl_quality.json") or {}
    hist_end = _history_end(hist_q)
    for name, rel, end in (
        ("sample", "data/processed/nyc311_clean", sample_end),
        ("history", "data/processed/member1_history", hist_end),
    ):
        path = root / rel
        success = path / "_SUCCESS"
        out.append(
            {
                "name": name,
                "path": str(path),
                "ready": success.is_file(),
                "coverage_end": end,
                "written_at": (
                    datetime.fromtimestamp(success.stat().st_mtime, tz=timezone.utc)  # noqa: UP017
                    if success.is_file()
                    else None
                ),
            }
        )
    return out


def _history_end(q: dict[str, Any]) -> str | None:
    """Last covered (inclusive) day in the Member 1 history quality report."""
    for key in ("coverage_end", "period_end", "end_inclusive", "last_day"):
        if q.get(key):
            return str(q[key])[:10]
    daily = q.get("daily_counts") or q.get("daily") or {}
    if isinstance(daily, dict) and daily:
        return max(str(k)[:10] for k in daily)
    end = q.get("end") or (q.get("config") or {}).get("end")
    if end:  # half-open exclusive end
        return str((pd.Timestamp(end) - pd.Timedelta(days=1)).date())
    return None


def check_table(name: str, inputs: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """Freshness/validity check for one contract table.

    status: ok | missing | fixture | invalid | stale. problems lists every
    finding; warnings are informational.
    """
    inputs = cleaned_inputs() if inputs is None else inputs
    res = load_contract(name, allow_fixture=True)
    real_dir_exists = _has_parquet(contract_dir(name))
    c = contracts.CONTRACTS[name]
    report: dict[str, Any] = {
        "table": name,
        "owner": c.owner,
        "path": str(contract_dir(name)),
        "rows": len(res.df),
        "status": "ok",
        "problems": [],
        "warnings": [],
    }
    if not real_dir_exists:
        report["status"] = FIXTURE if res.source == FIXTURE else MISSING
        report["problems"].append(
            "fixture-only: real output not produced yet"
            if res.source == FIXTURE
            else "missing: no output and no fixture"
        )
        return report
    if res.problems:
        report["status"] = INVALID
        report["problems"].extend(res.problems)
    if res.source == FIXTURE:
        report["status"] = FIXTURE
        report["problems"].append("fixture-only: published table is marked is_fixture")
        return report
    for key in contracts.META_KEYS:
        if key not in res.meta:
            report["problems"].append(f"meta missing key {key}")
            report["status"] = INVALID
    if "model_version" in c.columns:
        col = res.df.get("model_version")
        if col is None or col.isna().any() or (col.astype(str).str.strip() == "").any():
            report["problems"].append("missing model_version")
            report["status"] = INVALID
    if report["status"] != "ok":
        return report
    generated = _parse_ts(res.meta.get("generated_at_utc"))
    _, cov_end = coverage(res)
    ready = [i for i in inputs if i["ready"]]
    meta_input = str(res.meta.get("input_path") or "")
    matched = [i for i in ready if meta_input and Path(i["path"]).name in meta_input]
    compare = matched or ready
    newest_write = max((i["written_at"] for i in compare if i["written_at"]), default=None)
    if generated and newest_write and generated < newest_write:
        report["status"] = "stale"
        report["problems"].append(
            f"stale: generated_at {generated.isoformat()} older than input data written "
            f"{newest_write.isoformat()}"
        )
    latest_cov = max((str(i["coverage_end"]) for i in ready if i["coverage_end"]), default=None)
    if cov_end and latest_cov and cov_end[:10] < latest_cov[:10]:
        report["status"] = "stale"
        report["problems"].append(
            f"stale: coverage_end {cov_end[:10]} older than latest cleaned data coverage "
            f"{latest_cov[:10]}"
        )
    if not generated:
        report["warnings"].append("generated_at_utc not parseable")
    return report


def check_all() -> list[dict[str, Any]]:
    inputs = cleaned_inputs()
    return [check_table(n, inputs) for n in contracts.CONTRACTS]
