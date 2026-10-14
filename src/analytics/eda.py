"""Pandas EDA for Phase 1: aggregates + profiling. ASCII only.

Re-running on the same saved local sample yields identical outputs
(deterministic groupby/sort, no randomness).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

REQUIRED_COLUMNS = [
    "unique_key",
    "created_date",
    "closed_date",
    "complaint_type",
    "descriptor",
    "agency",
    "status",
    "borough",
    "incident_zip",
    "latitude",
    "longitude",
]

BOROUGH_CANON = {
    "BRONX": "BRONX",
    "BROOKLYN": "BROOKLYN",
    "MANHATTAN": "MANHATTAN",
    "QUEENS": "QUEENS",
    "STATEN ISLAND": "STATEN ISLAND",
}

# Approximate NYC bounding box for flagging invalid coords (not for dropping).
LAT_MIN, LAT_MAX = 40.49, 40.92
LON_MIN, LON_MAX = -74.26, -73.70


def load_csv(path: str | Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=True)


def normalize_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in REQUIRED_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA
    out["created_ts"] = pd.to_datetime(out["created_date"], errors="coerce", utc=True)
    out["closed_ts"] = pd.to_datetime(out["closed_date"], errors="coerce", utc=True)
    b = out["borough"].astype("string").str.strip().str.upper()
    out["borough_std"] = b.where(b.isin(set(BOROUGH_CANON)), other="UNKNOWN")
    out["borough_std"] = out["borough_std"].fillna("UNKNOWN")
    ct = out["complaint_type"].astype("string").str.strip()
    out["complaint_std"] = ct.fillna("UNKNOWN").replace("", "UNKNOWN")
    out["latitude_num"] = pd.to_numeric(out["latitude"], errors="coerce")
    out["longitude_num"] = pd.to_numeric(out["longitude"], errors="coerce")
    return out


def top_complaint_categories(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    w = normalize_frame(df)
    counts = (
        w.groupby("complaint_std", dropna=False)
        .size()
        .reset_index(name="n")
        .sort_values(["n", "complaint_std"], ascending=[False, True])
        .head(n)
        .reset_index(drop=True)
    )
    counts.rename(columns={"complaint_std": "complaint_type"}, inplace=True)
    return counts


def counts_by_borough(df: pd.DataFrame) -> pd.DataFrame:
    w = normalize_frame(df)
    counts = (
        w.groupby("borough_std", dropna=False)
        .size()
        .reset_index(name="n")
        .sort_values(["n", "borough_std"], ascending=[False, True])
        .reset_index(drop=True)
    )
    counts.rename(columns={"borough_std": "borough"}, inplace=True)
    return counts


def volume_over_time(df: pd.DataFrame, freq: str = "D") -> pd.DataFrame:
    w = normalize_frame(df).dropna(subset=["created_ts"]).copy()
    if w.empty:
        return pd.DataFrame(columns=["period", "n"])
    w["period"] = w["created_ts"].dt.floor(freq).dt.strftime("%Y-%m-%d")
    if freq.upper().startswith("W"):
        w["period"] = w["created_ts"].dt.floor("D").dt.to_period("W").astype(str)
    out = (
        w.groupby("period").size().reset_index(name="n").sort_values("period")
    ).reset_index(drop=True)
    return out


def compute_profile(df: pd.DataFrame) -> dict[str, Any]:
    w = normalize_frame(df)
    n = len(w)
    null_rates = {c: float(w[c].isna().sum() / n) if n else 0.0 for c in REQUIRED_COLUMNS}
    dup = int(w["unique_key"].duplicated(keep="first").sum()) if n else 0
    created = w["created_ts"]
    closed = w["closed_ts"]
    bad_ts = int(((closed.notna()) & (created.notna()) & (closed < created)).sum())
    lat = w["latitude_num"]
    lon = w["longitude_num"]
    invalid_coords = int(
        (
            lat.isna()
            | lon.isna()
            | (lat < LAT_MIN)
            | (lat > LAT_MAX)
            | (lon < LON_MIN)
            | (lon > LON_MAX)
        ).sum()
    )
    return {
        "row_count": n,
        "unique_keys": int(w["unique_key"].nunique(dropna=False)),
        "duplicate_count": dup,
        "duplicate_rate": float(dup / n) if n else 0.0,
        "null_rates": null_rates,
        "created_min": str(created.min()) if n and created.notna().any() else None,
        "created_max": str(created.max()) if n and created.notna().any() else None,
        "created_nat": int(created.isna().sum()),
        "closed_nat": int(closed.isna().sum()),
        "closed_before_created": bad_ts,
        "invalid_or_missing_coords": invalid_coords,
        "borough_values": w["borough_std"].value_counts(dropna=False).to_dict(),
    }


def save_eda_outputs(df: pd.DataFrame, outdir: str | Path) -> dict[str, str]:
    out = Path(outdir)
    out.mkdir(parents=True, exist_ok=True)
    top = top_complaint_categories(df, n=15)
    by_b = counts_by_borough(df)
    vol = volume_over_time(df, freq="D")
    prof = compute_profile(df)
    p_top = out / "top_categories.csv"
    p_b = out / "by_borough.csv"
    p_v = out / "volume_daily.csv"
    p_p = out / "profile.json"
    top.to_csv(p_top, index=False)
    by_b.to_csv(p_b, index=False)
    vol.to_csv(p_v, index=False)
    with open(p_p, "w", encoding="utf-8") as f:
        json.dump(prof, f, indent=2)
    return {"top": str(p_top), "borough": str(p_b), "volume": str(p_v), "profile": str(p_p)}
