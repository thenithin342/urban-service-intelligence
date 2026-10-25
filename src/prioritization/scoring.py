"""Transparent advisory priority score (Member 3). Pure pandas. ASCII only.

The score is an ADVISORY OPERATIONAL PRIORITY for review, not a proven
staffing optimum: no budget, crew capacity or cost data is used.

Unit: (location, complaint_type), location = borough or ZIP.
Scoring window: calendar days [window_start, window_end] (inclusive).
Cut-off T = window_end + 1 day (00:00). Every input below is computable from
records created before T (docs/data_contracts.md section 3):

  demand_value   mean daily complaints created in the window (observed), or
                 mean Member 1 predicted_count over [T, T + horizon) when a
                 real demand_forecast is supplied.
  backlog_value  complaints created in the window still open at T
                 (closed_at null or closed_at >= T), or Member 2
                 expected_open_after_7d when real cohort predictions are
                 supplied.
  delay_value    median resolution hours of records closed before T
                 (so created and closed before T), from a binned histogram;
                 null when fewer than min_delay_records resolved records.

Each value is min-max normalized to [0, 1] across units in the window
(constant or all-null column -> 0; null -> 0). priority_score is the weighted
sum with weights that are non-negative and sum to 1. Rank 1 = highest score;
ties broken by location then complaint_type (ascending), so ranks are
deterministic and unique.

Input aggregates (built by scripts/run_member3_priorities.py):
  created: day, location, complaint_type, close_lag_days (nullable: open at
           snapshot), n
  delay:   closed_day, location, complaint_type, hours_bin, n
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, timedelta

import numpy as np
import pandas as pd

SCORING_VERSION = "member3-priority-v1"
ADVISORY_LABEL = "advisory operational priority, not a proven staffing optimum"
COMPONENTS = ("demand", "backlog", "delay")
DEFAULT_WEIGHTS = {"demand": 0.5, "backlog": 0.3, "delay": 0.2}

# Resolution-hour histogram edges (hours). Last bin is open-ended.
HOUR_EDGES: tuple[float, ...] = (
    0.0, 0.5, 1.0, 2.0, 3.0, 4.0, 6.0, 8.0, 12.0, 18.0, 24.0, 36.0, 48.0, 72.0,
    96.0, 120.0, 168.0, 240.0, 336.0, 504.0, 720.0, 1080.0, 1440.0, 2160.0,
    4320.0, 8760.0, math.inf,
)
KEYS = ["location", "complaint_type"]


@dataclass(frozen=True)
class ScoringConfig:
    window_days: int = 7
    weights: Mapping[str, float] = field(default_factory=lambda: dict(DEFAULT_WEIGHTS))
    min_delay_records: int = 5
    delay_lookback_days: int | None = None
    scoring_version: str = SCORING_VERSION


def validate_weights(weights: Mapping[str, float]) -> dict[str, float]:
    """Return weights as floats; raise ValueError unless valid.

    Keys must be exactly demand/backlog/delay, values finite and >= 0, and
    the sum must be 1 (tolerance 1e-6).
    """
    keys = set(weights)
    if keys != set(COMPONENTS):
        raise ValueError(f"weights keys must be {sorted(COMPONENTS)}, got {sorted(keys)}")
    out: dict[str, float] = {}
    for k in COMPONENTS:
        try:
            v = float(weights[k])
        except (TypeError, ValueError) as exc:
            raise ValueError(f"weight {k} is not a number: {weights[k]!r}") from exc
        if not math.isfinite(v) or v < 0:
            raise ValueError(f"weight {k} must be finite and >= 0, got {v}")
        out[k] = v
    total = sum(out.values())
    if abs(total - 1.0) > 1e-6:
        raise ValueError(f"weights must sum to 1, got {total:.6f}")
    return out


def minmax(values: pd.Series) -> pd.Series:
    """Min-max scale to [0, 1]. Nulls -> 0; constant/all-null -> all 0."""
    s = pd.to_numeric(values, errors="coerce").astype(float)
    valid = s.dropna()
    if valid.empty:
        return pd.Series(0.0, index=s.index)
    lo, hi = float(valid.min()), float(valid.max())
    if hi - lo <= 0:
        return pd.Series(0.0, index=s.index)
    return ((s - lo) / (hi - lo)).fillna(0.0).clip(0.0, 1.0)


def hours_bin(hours: pd.Series) -> pd.Series:
    """Histogram bin index for resolution hours (same rule as the Spark SQL)."""
    edges = np.asarray(HOUR_EDGES[1:-1])
    return pd.Series(np.searchsorted(edges, hours.to_numpy(), side="right"), index=hours.index)


def binned_median(bins: pd.Series, counts: pd.Series) -> float:
    """Median from histogram counts by linear interpolation inside the bin."""
    df = pd.DataFrame({"b": bins.astype(int), "n": counts.astype(float)})
    df = df.groupby("b", as_index=False)["n"].sum().sort_values("b")
    total = df["n"].sum()
    if total <= 0:
        return float("nan")
    half = total / 2.0
    cum = 0.0
    for b, n in zip(df["b"], df["n"], strict=True):
        if cum + n >= half:
            lo, hi = HOUR_EDGES[b], HOUR_EDGES[b + 1]
            if math.isinf(hi):
                return float(lo)
            return float(lo + (hi - lo) * (half - cum) / n)
        cum += n
    return float(HOUR_EDGES[int(df["b"].iloc[-1])])


def grouped_binned_median(hist: pd.DataFrame) -> pd.DataFrame:
    """Vectorized binned_median per unit: KEYS, delay_value, delay_records."""
    cols = [*KEYS, "delay_value", "delay_records"]
    if hist.empty:
        return pd.DataFrame(columns=cols)
    h = (
        hist.groupby([*KEYS, "hours_bin"], as_index=False)["n"].sum()
        .sort_values([*KEYS, "hours_bin"], kind="mergesort")
    )
    h["n"] = h["n"].astype(float)
    h["cum"] = h.groupby(KEYS)["n"].cumsum()
    h["total"] = h.groupby(KEYS)["n"].transform("sum")
    hit = h[h["cum"] >= h["total"] / 2.0].groupby(KEYS, as_index=False).first()
    edges = np.asarray(HOUR_EDGES)
    b = hit["hours_bin"].astype(int).to_numpy()
    lo, hi = edges[b], edges[b + 1]
    prev = (hit["cum"] - hit["n"]).to_numpy()
    with np.errstate(invalid="ignore"):
        frac = (hit["total"].to_numpy() / 2.0 - prev) / hit["n"].to_numpy()
        val = np.where(np.isinf(hi), lo, lo + (hi - lo) * frac)
    out = hit[KEYS].copy()
    out["delay_value"] = val.astype(float)
    out["delay_records"] = hit["total"].astype(int).to_numpy()
    return out[cols]


def window_bounds(window_end: date, window_days: int) -> tuple[date, date, date]:
    """(window_start, window_end, cutoff) with cutoff = window_end + 1 day."""
    if window_days < 1:
        raise ValueError("window_days must be >= 1")
    start = window_end - timedelta(days=window_days - 1)
    return start, window_end, window_end + timedelta(days=1)


def _as_date(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s).dt.date


def observed_components(
    created: pd.DataFrame,
    delay: pd.DataFrame,
    window_end: date,
    cfg: ScoringConfig,
) -> pd.DataFrame:
    """Raw demand/backlog/delay per unit using only data before the cut-off.

    Units are the (location, complaint_type) pairs with at least one complaint
    created in the window.
    """
    start, end, cutoff = window_bounds(window_end, cfg.window_days)
    c = created.copy()
    c["day"] = _as_date(c["day"])
    c = c[(c["day"] >= start) & (c["day"] <= end)]
    if c.empty:
        return pd.DataFrame(
            columns=[*KEYS, "window_count", "demand_value", "backlog_value", "delay_value",
                     "delay_records"]
        )
    lag = pd.to_numeric(c["close_lag_days"], errors="coerce")
    closed_day = pd.to_datetime(c["day"]) + pd.to_timedelta(lag, unit="D")
    open_at_cutoff = lag.isna() | (closed_day >= pd.Timestamp(cutoff))
    c = c.assign(open_n=np.where(open_at_cutoff, c["n"], 0))
    units = c.groupby(KEYS, as_index=False).agg(window_count=("n", "sum"), backlog_value=("open_n", "sum"))
    units["demand_value"] = units["window_count"] / float(cfg.window_days)

    d = delay.copy()
    if not d.empty:
        d["closed_day"] = _as_date(d["closed_day"])
        keep = d["closed_day"] < cutoff
        if cfg.delay_lookback_days:
            keep &= d["closed_day"] >= cutoff - timedelta(days=int(cfg.delay_lookback_days))
        d = d[keep]
    med = grouped_binned_median(d.merge(units[KEYS], on=KEYS, how="inner"))
    units = units.merge(med, on=KEYS, how="left")
    units["delay_records"] = pd.to_numeric(units["delay_records"]).fillna(0).astype(int)
    units.loc[units["delay_records"] < cfg.min_delay_records, "delay_value"] = np.nan
    units["delay_value"] = units["delay_value"].astype(float)
    units["backlog_value"] = units["backlog_value"].astype(float)
    return units


def apply_overrides(
    units: pd.DataFrame,
    demand: pd.DataFrame | None = None,
    backlog: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Replace observed demand/backlog with model values where available.

    demand / backlog: frames with location, complaint_type, value. Units
    without a model value keep the observed value; counts are returned so
    the meta can state how many units used each source.
    """
    out = units.copy()
    stats = {"demand_model_units": 0, "backlog_model_units": 0}
    for frame, col, key in ((demand, "demand_value", "demand_model_units"),
                            (backlog, "backlog_value", "backlog_model_units")):
        if frame is None or frame.empty:
            continue
        m = out[KEYS].merge(frame[[*KEYS, "value"]], on=KEYS, how="left")["value"]
        has = m.notna().to_numpy()
        out.loc[has, col] = m[has].to_numpy()
        stats[key] = int(has.sum())
    return out, stats


def score_units(units: pd.DataFrame, weights: Mapping[str, float]) -> pd.DataFrame:
    """Normalize components, compute priority_score and a deterministic rank."""
    w = validate_weights(weights)
    out = units.copy()
    for comp in COMPONENTS:
        out[f"{comp}_component"] = minmax(out[f"{comp}_value"]) if len(out) else []
    out["priority_score"] = sum(w[c] * out[f"{c}_component"] for c in COMPONENTS) if len(out) else []
    out = out.sort_values(
        ["priority_score", "location", "complaint_type"],
        ascending=[False, True, True],
        kind="mergesort",
    ).reset_index(drop=True)
    out["rank"] = np.arange(1, len(out) + 1, dtype="int64")
    return out


def score_window(
    created: pd.DataFrame,
    delay: pd.DataFrame,
    window_end: date,
    cfg: ScoringConfig,
    demand_override: pd.DataFrame | None = None,
    backlog_override: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, dict[str, int]]:
    units = observed_components(created, delay, window_end, cfg)
    units, stats = apply_overrides(units, demand_override, backlog_override)
    return score_units(units, cfg.weights), stats
