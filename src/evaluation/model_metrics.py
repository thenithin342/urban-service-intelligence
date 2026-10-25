"""Binary-classification metrics for the closure models. ASCII only.

PR-AUC is reported as average precision (sklearn ``average_precision_score``,
step-wise, no interpolation). PR-AUC / ROC-AUC are ``None`` when a split has
a single class. Precision/recall use threshold 0.5 (``p >= 0.5``);
precision is 0.0 when nothing is predicted positive.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    log_loss,
    precision_score,
    recall_score,
    roc_auc_score,
)


def binary_metrics(y: np.ndarray | pd.Series, p: np.ndarray | pd.Series,
                   threshold: float = 0.5) -> dict[str, Any]:
    y = np.asarray(y, dtype=int)
    p = np.clip(np.asarray(p, dtype=float), 0.0, 1.0)
    if len(y) == 0:
        raise ValueError("binary_metrics needs at least one row")
    two_classes = len(np.unique(y)) == 2
    pred = (p >= threshold).astype(int)
    return {
        "n": len(y),
        "positive_rate": float(y.mean()),
        "mean_prediction": float(p.mean()),
        "pr_auc": float(average_precision_score(y, p)) if two_classes else None,
        "roc_auc": float(roc_auc_score(y, p)) if two_classes else None,
        "brier": float(brier_score_loss(y, p, pos_label=1)),
        "log_loss": float(log_loss(y, np.clip(p, 1e-6, 1 - 1e-6), labels=[0, 1])),
        "precision_at_05": float(precision_score(y, pred, zero_division=0)),
        "recall_at_05": float(recall_score(y, pred, zero_division=0)),
    }


def calibration_table(y: np.ndarray | pd.Series, p: np.ndarray | pd.Series,
                      n_bins: int = 10) -> pd.DataFrame:
    """Equal-width probability bins: n, mean prediction, observed rate."""
    y = np.asarray(y, dtype=int)
    p = np.clip(np.asarray(p, dtype=float), 0.0, 1.0)
    edges = np.linspace(0.0, 1.0, n_bins + 1)
    idx = np.clip(np.digitize(p, edges[1:-1], right=False), 0, n_bins - 1)
    rows = []
    for b in range(n_bins):
        mask = idx == b
        n = int(mask.sum())
        rows.append({
            "bin": b,
            "bin_lower": float(edges[b]),
            "bin_upper": float(edges[b + 1]),
            "n": n,
            "mean_prediction": float(p[mask].mean()) if n else None,
            "observed_rate": float(y[mask].mean()) if n else None,
        })
    out = pd.DataFrame(rows)
    out["mean_prediction"] = out["mean_prediction"].astype("float64")
    out["observed_rate"] = out["observed_rate"].astype("float64")
    return out


def expected_calibration_error(cal: pd.DataFrame) -> float:
    filled = cal[cal["n"] > 0]
    total = filled["n"].sum()
    if total == 0:
        return float("nan")
    gap = (filled["mean_prediction"] - filled["observed_rate"]).abs()
    return float((gap * filled["n"]).sum() / total)


def group_metrics(frame: pd.DataFrame, group: str, y: str, p: str,
                  baseline: str | None = None, min_n: int = 30) -> pd.DataFrame:
    """Per-group error breakdown; flags groups smaller than ``min_n``."""
    rows = []
    for key, g in frame.groupby(group, dropna=False, sort=True):
        m = binary_metrics(g[y], g[p])
        row = {
            group: None if (isinstance(key, float) and np.isnan(key)) else key,
            "n": m["n"],
            "positive_rate": m["positive_rate"],
            "mean_prediction": m["mean_prediction"],
            "calibration_gap": m["mean_prediction"] - m["positive_rate"],
            "brier": m["brier"],
            "pr_auc": m["pr_auc"],
            "roc_auc": m["roc_auc"],
            "small_sample": m["n"] < min_n,
        }
        if baseline is not None:
            b = float(np.mean((np.asarray(g[baseline], float) - np.asarray(g[y], float)) ** 2))
            row["baseline_brier"] = b
            row["brier_skill"] = 1.0 - m["brier"] / b if b > 0 else None
        rows.append(row)
    out = pd.DataFrame(rows)
    for col in ("pr_auc", "roc_auc", "brier_skill"):
        if col in out.columns:
            out[col] = out[col].astype("float64")
    return out.sort_values(["n", group], ascending=[False, True], ignore_index=True,
                           na_position="last")
