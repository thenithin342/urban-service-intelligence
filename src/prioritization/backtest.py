"""Rolling historical backtest of the advisory priority ranking. ASCII only.

For each historical window t (window_days long, stepping step_days) the units
are ranked using ONLY data before the cut-off T = window_end + 1 day
(scoring.score_window filters by date). The ranking is then compared with the
actual complaint volume created in the next window [T, T + window_days).

Methods compared on the same unit universe (units with complaints in window t):
  priority_score  the configured weighted score
  volume_only     complaints created in window t (last-window count baseline)
  random          uniform random scores, fixed seed (seed + window index)

Metrics per window and k:
  precision_at_k  |top-k by method  intersect  true top-k by next-window volume| / k
  spearman        Spearman rank correlation of method score vs next-window volume
                  (null when either side is constant)
Ties in the true top-k are broken by location then complaint_type.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, timedelta

import numpy as np
import pandas as pd

from src.prioritization.scoring import KEYS, ScoringConfig, score_window

METHODS = ("priority_score", "volume_only", "random")


def precision_at_k(predicted: Sequence, actual: Sequence, k: int) -> float:
    """Fraction of the first k predicted ids that are in the first k actual ids."""
    if k <= 0:
        raise ValueError("k must be positive")
    pred, act = list(predicted)[:k], set(list(actual)[:k])
    if len(pred) < k or len(act) < k:
        raise ValueError("k exceeds the number of ranked units")
    return len([p for p in pred if p in act]) / float(k)


def spearman(a: pd.Series, b: pd.Series) -> float | None:
    a = pd.Series(np.asarray(a, dtype=float))
    b = pd.Series(np.asarray(b, dtype=float))
    if a.nunique() < 2 or b.nunique() < 2:
        return None
    return float(a.rank().corr(b.rank()))


def _order(df: pd.DataFrame, score_col: str) -> list[tuple[str, str]]:
    ranked = df.sort_values(
        [score_col, "location", "complaint_type"], ascending=[False, True, True], kind="mergesort"
    )
    return list(zip(ranked["location"], ranked["complaint_type"], strict=True))


def backtest_windows(first_day: date, last_day: date, window_days: int, step_days: int):
    """Yield (window_end, target_start, target_end) with the target inside the data."""
    i = 0
    while True:
        ws = first_day + timedelta(days=i * step_days)
        we = ws + timedelta(days=window_days - 1)
        ts = we + timedelta(days=1)
        te = ts + timedelta(days=window_days - 1)
        if te > last_day:
            return
        yield ws, we, ts, te
        i += 1


def target_counts(created: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    c = created.assign(day=pd.to_datetime(created["day"]).dt.date)
    c = c[(c["day"] >= start) & (c["day"] <= end)]
    return c.groupby(KEYS, as_index=False)["n"].sum().rename(columns={"n": "target_count"})


def evaluate_window(
    created: pd.DataFrame,
    delay: pd.DataFrame,
    window_end: date,
    target_start: date,
    target_end: date,
    cfg: ScoringConfig,
    ks: Sequence[int],
    seed: int,
) -> list[dict]:
    # Scoring sees only data before the cut-off (enforced inside score_window
    # and, defensively, by truncating the inputs here).
    cutoff = window_end + timedelta(days=1)
    hist_created = created[pd.to_datetime(created["day"]).dt.date < cutoff]
    hist_delay = delay[pd.to_datetime(delay["closed_day"]).dt.date < cutoff]
    scored, _ = score_window(hist_created, hist_delay, window_end, cfg)
    if scored.empty:
        return []
    tgt = target_counts(created, target_start, target_end)
    df = scored.merge(tgt, on=KEYS, how="left")
    df["target_count"] = df["target_count"].fillna(0).astype(float)
    rng = np.random.default_rng(seed)
    df["random_score"] = rng.random(len(df))
    scores = {
        "priority_score": "priority_score",
        "volume_only": "window_count",
        "random": "random_score",
    }
    truth = _order(df, "target_count")
    window_start = window_end - timedelta(days=cfg.window_days - 1)
    rows = []
    for method in METHODS:
        col = scores[method]
        pred = _order(df, col)
        rho = spearman(df[col], df["target_count"])
        for k in ks:
            if k > len(df):
                continue
            rows.append(
                {
                    "window_start": window_start,
                    "window_end": window_end,
                    "target_start": target_start,
                    "target_end": target_end,
                    "method": method,
                    "k": int(k),
                    "precision_at_k": precision_at_k(pred, truth, k),
                    "spearman": rho,
                    "n_units": len(df),
                }
            )
    return rows


def run_backtest(
    created: pd.DataFrame,
    delay: pd.DataFrame,
    first_day: date,
    last_day: date,
    cfg: ScoringConfig,
    ks: Sequence[int],
    step_days: int,
    seed: int = 42,
) -> pd.DataFrame:
    rows: list[dict] = []
    for i, (_, we, ts, te) in enumerate(
        backtest_windows(first_day, last_day, cfg.window_days, step_days)
    ):
        rows += evaluate_window(created, delay, we, ts, te, cfg, ks, seed + i)
    cols = ["window_start", "window_end", "target_start", "target_end", "method", "k",
            "precision_at_k", "spearman", "n_units"]
    out = pd.DataFrame(rows, columns=cols)
    if not out.empty:
        out["spearman"] = out["spearman"].astype(float)
        out["k"] = out["k"].astype("int64")
        out["n_units"] = out["n_units"].astype("int64")
    return out


def summarize(bt: pd.DataFrame) -> pd.DataFrame:
    """Mean precision@k and Spearman per method x k across windows."""
    if bt.empty:
        return bt
    return (
        bt.groupby(["method", "k"], as_index=False)
        .agg(
            windows=("precision_at_k", "size"),
            mean_precision_at_k=("precision_at_k", "mean"),
            mean_spearman=("spearman", "mean"),
            mean_n_units=("n_units", "mean"),
        )
        .sort_values(["k", "method"])
        .reset_index(drop=True)
    )
