"""Member 2 evaluation helpers: figure rendering and content hashing (no Spark)."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from src.evaluation import member2_figures as figs
from src.evaluation import model_metrics as mm
from src.evaluation.member2_config import frame_hash, history_ready, write_extra_table


def test_frame_hash_is_order_sensitive_and_handles_lists():
    a = pd.DataFrame({"items": [["A", "B"], ["C"]], "v": [0.1, 0.2]})
    assert frame_hash(a) == frame_hash(a.copy())
    assert frame_hash(a) != frame_hash(a.iloc[::-1].reset_index(drop=True))
    b = a.copy()
    b.loc[0, "v"] = 0.1000001
    assert frame_hash(a) != frame_hash(b)


def test_write_extra_table_and_history_ready(tmp_path: Path):
    out = write_extra_table(pd.DataFrame({"x": [1, 2]}), tmp_path / "t", {"k": "v"})
    assert len(pd.read_parquet(out)) == 2
    assert (tmp_path / "t.meta.json").is_file()
    assert history_ready(tmp_path)["ready"] is False


def test_figures_render_from_small_measured_like_tables(tmp_path: Path):
    grid = pd.DataFrame({
        "granularity": ["week", "week", "day", "day"], "min_support": [0.5, 0.6, 0.1, 0.2],
        "min_confidence": [0.5] * 4, "n_rules": [100, 10, 50, 0]})
    comp = pd.DataFrame({"model": ["a", "b"], "split": ["test", "test"],
                         "brier": [0.1, 0.2], "pr_auc": [0.9, None]})
    rng = np.random.default_rng(0)
    y = rng.integers(0, 2, 200)
    pred = pd.DataFrame({"closed_within_7d": y, "p_a": rng.random(200),
                         "p_b": np.clip(y * 0.6 + 0.2, 0, 1)})
    cal = pd.concat([mm.calibration_table(y, pred[f"p_{m}"]).assign(model=m)
                     for m in ("a", "b")])
    stab = mm.group_metrics(pred.assign(g=np.where(y > 0, "x", "y")), "g",
                            "closed_within_7d", "p_b", "p_a", 30)
    paths = [
        figs.threshold_sensitivity(grid, tmp_path / "g.png", "cov"),
        figs.model_comparison(comp, tmp_path / "m.png", "w"),
        figs.calibration(cal, tmp_path / "c.png", "w"),
        figs.pr_curves(pred, ["a", "b"], tmp_path / "p.png", "w"),
        figs.group_brier(stab, "g", tmp_path / "s.png", "t", "w"),
    ]
    assert all(Path(p).stat().st_size > 1000 for p in paths)
