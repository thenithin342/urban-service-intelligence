"""Member 2 report figures (matplotlib PNG) drawn only from measured tables. ASCII only.

Palette: fixed-order categorical slots of the reference data-viz palette
(light mode); one y-axis per chart; recessive grid; legends whenever a chart
has two or more series.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6"]
SURFACE = "#fcfcfb"
TEXT = "#0b0b0b"
TEXT_2 = "#52514e"
GRID = "#e4e3df"


def _axes(title: str, subtitle: str, size: tuple[float, float] = (8, 4.8)):
    fig, ax = plt.subplots(figsize=size, dpi=130)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=TEXT_2, labelsize=8)
    ax.grid(True, color=GRID, linewidth=0.6)
    ax.set_axisbelow(True)
    fig.suptitle(title, x=0.01, ha="left", color=TEXT, fontsize=11, fontweight="bold")
    ax.set_title(subtitle, loc="left", color=TEXT_2, fontsize=8)
    return fig, ax


def _save(fig, path: Path) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)
    return str(path)


def threshold_sensitivity(grid: pd.DataFrame, path: Path, coverage: str) -> str:
    """Rules vs min_support per (granularity, min_confidence); log scale."""
    fig, ax = _axes("FP-Growth threshold sensitivity",
                    f"Association rules per threshold pair (log scale). {coverage}")
    i = 0
    for gran in sorted(grid["granularity"].unique(), reverse=True):
        for conf in sorted(grid["min_confidence"].unique()):
            g = grid[(grid["granularity"] == gran) & (grid["min_confidence"] == conf)]
            g = g.sort_values("min_support")
            if g.empty:
                continue
            style = "-" if gran == "week" else "--"
            ax.plot(g["min_support"], g["n_rules"].clip(lower=0.8), style, marker="o",
                    markersize=5, linewidth=2, color=SERIES[i % len(SERIES)],
                    label=f"ZIP x {gran}, conf >= {conf:g}")
            i += 1
    ax.set_yscale("log")
    ax.set_xlabel("min_support", color=TEXT_2, fontsize=9)
    ax.set_ylabel("rules", color=TEXT_2, fontsize=9)
    ax.legend(fontsize=7, frameon=False, ncol=2)
    return _save(fig, path)


def model_comparison(comp: pd.DataFrame, path: Path, window: str) -> str:
    """Test-split Brier per model (lower is better), single series."""
    test = comp[comp["split"] == "test"].sort_values("brier", ascending=False)
    fig, ax = _axes("Closure-within-7-days models: test Brier score",
                    f"Lower is better. Test window {window}; PR-AUC in labels.")
    y = np.arange(len(test))
    ax.barh(y, test["brier"], color=SERIES[0], height=0.6)
    for yi, (b, pr) in enumerate(zip(test["brier"], test["pr_auc"])):
        label = f"{b:.4f}" + ("" if pd.isna(pr) else f"  (PR-AUC {pr:.3f})")
        ax.text(b, yi, "  " + label, va="center", color=TEXT, fontsize=8)
    ax.set_yticks(y, test["model"])
    ax.set_xlim(0, test["brier"].max() * 1.45)
    ax.set_xlabel("Brier score", color=TEXT_2, fontsize=9)
    ax.grid(axis="y", visible=False)
    return _save(fig, path)


def calibration(cal: pd.DataFrame, path: Path, window: str) -> str:
    fig, ax = _axes("Calibration on the test window",
                    f"Mean predicted vs observed 7-day closure rate per 0.1 bin. {window}",
                    size=(6.4, 5.6))
    ax.plot([0, 1], [0, 1], color=TEXT_2, linewidth=1, linestyle=":", label="perfect")
    for i, (name, g) in enumerate(cal.groupby("model", sort=False)):
        g = g[g["n"] > 0]
        ax.plot(g["mean_prediction"], g["observed_rate"], marker="o", markersize=4,
                linewidth=2, color=SERIES[i % len(SERIES)], label=name)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_xlabel("mean predicted probability", color=TEXT_2, fontsize=9)
    ax.set_ylabel("observed closure rate", color=TEXT_2, fontsize=9)
    ax.legend(fontsize=7, frameon=False, loc="upper left")
    return _save(fig, path)


def pr_curves(pred: pd.DataFrame, models: list[str], path: Path, window: str) -> str:
    from sklearn.metrics import precision_recall_curve

    fig, ax = _axes("Precision-recall curves (test window)", window, size=(6.4, 5.2))
    y = pred["closed_within_7d"].to_numpy()
    for i, name in enumerate(models):
        prec, rec, _ = precision_recall_curve(y, pred[f"p_{name}"].to_numpy())
        ax.plot(rec, prec, linewidth=2, color=SERIES[i % len(SERIES)], label=name)
    ax.axhline(y.mean(), color=TEXT_2, linewidth=1, linestyle=":", label="positive rate")
    ax.set_xlabel("recall", color=TEXT_2, fontsize=9)
    ax.set_ylabel("precision", color=TEXT_2, fontsize=9)
    ax.set_ylim(max(0.0, y.mean() - 0.1), 1.005)
    ax.legend(fontsize=7, frameon=False, loc="lower left")
    return _save(fig, path)


def group_brier(stab: pd.DataFrame, key: str, path: Path, title: str, window: str,
                top: int = 15) -> str:
    """Selected model vs prior Brier per group (two series, side by side)."""
    s = stab.head(top).iloc[::-1]
    fig, ax = _axes(title, f"Test window {window}. Groups with n < 30 are marked *.",
                    size=(8, max(3.2, 0.32 * len(s) + 1.2)))
    y = np.arange(len(s))
    ax.barh(y + 0.2, s["baseline_brier"], height=0.38, color=SERIES[1], label="prior_rate")
    ax.barh(y - 0.2, s["brier"], height=0.38, color=SERIES[0], label="selected model")
    labels = [f"{'*' if sm else ''}{k} (n={n})" for k, n, sm in
              zip(s[key].fillna("UNKNOWN"), s["n"], s["small_sample"])]
    ax.set_yticks(y, labels, fontsize=7)
    ax.set_xlabel("Brier score", color=TEXT_2, fontsize=9)
    ax.grid(axis="y", visible=False)
    ax.legend(fontsize=7, frameon=False, loc="lower right")
    return _save(fig, path)
