"""Compute advisory priority scores and their rolling backtest (Member 3).

Reads cleaned Parquet with Spark, aggregates to small daily
(date x location x complaint_type) tables, then scores in pandas
(src/prioritization). Writes the priority_scores and priority_backtest
contract tables via contracts.write_table.

  python scripts/run_member3_priorities.py --config configs/sample.yaml
      [--input data/processed/member1_history] [--location-type borough|zip]
      [--window-days 7] [--step-days 7] [--root .]

ASCII only.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import yaml

from src.prioritization import aggregate, backtest, scoring
from src.utils import contracts

DEFAULTS = {
    "input": "data/processed/nyc311_clean",
    "location_type": "borough",
    "window_days": 7,
    "step_days": 7,
    "weights": dict(scoring.DEFAULT_WEIGHTS),
    "min_delay_records": 5,
    "delay_lookback_days": None,
    "backtest_k": [10, 25, 50],
    "random_seed": 42,
    "partial_day_ratio": 0.5,
    "lag_cap_days": 60,
    "scoring_version": scoring.SCORING_VERSION,
    "spark_master": "local[2]",
}


def load_member3_config(path: str | Path | None) -> dict:
    cfg = dict(DEFAULTS)
    if path:
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        cfg.update(raw.get("member3") or {})
    cfg["weights"] = scoring.validate_weights(cfg["weights"])
    return cfg


def _model_inputs(root: Path, location_type: str, ws, we, window_days: int):
    """Real Member 1/2 model outputs usable for the scoring window, if any."""
    import os

    previous = os.environ.get("USI_ROOT")
    os.environ["USI_ROOT"] = str(root)
    try:
        return _model_inputs_at_root(location_type, ws, we, window_days)
    finally:
        if previous is None:
            os.environ.pop("USI_ROOT", None)
        else:
            os.environ["USI_ROOT"] = previous


def _model_inputs_at_root(location_type: str, ws, we, window_days: int):
    from dashboard import adapters as ad

    demand = backlog = None
    demand_src = "observed: mean daily complaints created in the window"
    backlog_src = "observed: complaints created in the window still open at the cut-off"
    if location_type != "borough":
        return demand, backlog, demand_src + " (model outputs are borough-level)", backlog_src
    cutoff = we + timedelta(days=1)
    fc = ad.load_contract("demand_forecast", allow_fixture=False)
    if fc.source == ad.REAL:
        d = fc.df.assign(
            forecast_date=pd.to_datetime(fc.df["forecast_date"]).dt.date,
            training_cutoff=pd.to_datetime(fc.df["training_cutoff"]).dt.date,
        )
        d = d[
            (d["forecast_date"] >= cutoff)
            & (d["forecast_date"] < cutoff + timedelta(days=window_days))
            & (d["training_cutoff"] <= we)
            & d["borough"].notna()
        ]
        if not d.empty:
            demand = (
                d.groupby(["borough", "complaint_type"], as_index=False)["predicted_count"].mean()
                .rename(columns={"borough": "location", "predicted_count": "value"})
            )
            versions = ",".join(sorted(d["model_version"].astype(str).unique()))
            demand_src = f"member1 demand_forecast mean predicted_count (model_version {versions})"
    cc = ad.load_contract("closure_cohort_predictions", allow_fixture=False)
    if cc.source == ad.REAL:
        b = cc.df.assign(
            pws=pd.to_datetime(cc.df["prediction_window_start"]).dt.date,
            pwe=pd.to_datetime(cc.df["prediction_window_end"]).dt.date,
        )
        b = b[(b["pwe"] <= we) & (b["pws"] >= ws) & b["borough"].notna()]
        if not b.empty:
            backlog = (
                b.groupby(["borough", "complaint_type"], as_index=False)["expected_open_after_7d"]
                .sum()
                .rename(columns={"borough": "location", "expected_open_after_7d": "value"})
            )
            versions = ",".join(sorted(b["model_version"].astype(str).unique()))
            backlog_src = (
                f"member2 closure_cohort_predictions expected_open_after_7d (model_version {versions})"
            )
    return demand, backlog, demand_src, backlog_src


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Member 3 advisory priority scores + backtest")
    ap.add_argument("--config", default=None)
    ap.add_argument("--input", default=None, help="cleaned Parquet directory")
    ap.add_argument("--location-type", choices=sorted(aggregate.LOCATION_COLUMNS), default=None)
    ap.add_argument("--window-days", type=int, default=None)
    ap.add_argument("--step-days", type=int, default=None)
    ap.add_argument("--root", default=str(ROOT), help="repository root for outputs")
    args = ap.parse_args(argv)

    cfg = load_member3_config(args.config)
    for key, val in (("input", args.input), ("location_type", args.location_type),
                     ("window_days", args.window_days), ("step_days", args.step_days)):
        if val is not None:
            cfg[key] = val
    root = Path(args.root)
    input_path = Path(cfg["input"])
    if not input_path.is_absolute():
        input_path = root / input_path
    if not (input_path / "_SUCCESS").is_file():
        print(f"ERROR: cleaned Parquet not found or incomplete: {input_path}", file=sys.stderr)
        return 2
    wd, step = int(cfg["window_days"]), int(cfg["step_days"])
    if wd < 1 or step < 1 or wd > int(cfg["lag_cap_days"]):
        print("ERROR: need 1 <= window_days <= lag_cap_days and step_days >= 1", file=sys.stderr)
        return 2
    sc = scoring.ScoringConfig(
        window_days=wd,
        weights=cfg["weights"],
        min_delay_records=int(cfg["min_delay_records"]),
        delay_lookback_days=cfg["delay_lookback_days"],
        scoring_version=str(cfg["scoring_version"]),
    )

    t0 = time.perf_counter()
    from src.utils.spark import get_spark

    spark = get_spark("member3-priorities", master=str(cfg["spark_master"]))
    try:
        agg = aggregate.aggregate_spark(
            spark, str(input_path), cfg["location_type"], int(cfg["lag_cap_days"])
        )
    finally:
        spark.stop()
    t_agg = time.perf_counter() - t0
    created, delay, coords = agg["created"], agg["delay"], agg["coords"]

    totals = aggregate.daily_totals(created)
    full, dropped = aggregate.trim_partial_tail(totals, float(cfg["partial_day_ratio"]))
    first_day, last_day = full.index.min(), full.index.max()
    n_days = (last_day - first_day).days + 1
    if n_days < wd:
        print(f"ERROR: only {n_days} full days, need >= window_days={wd}", file=sys.stderr)
        return 2

    # Current scoring window: the last window_days full days.
    ws, we, _ = scoring.window_bounds(last_day, wd)
    demand, backlog, demand_src, backlog_src = _model_inputs(
        root, cfg["location_type"], ws, we, wd
    )
    scored, stats = scoring.score_window(created, delay, we, sc, demand, backlog)
    scored = scored.merge(coords[["location", "center_lat", "center_lon"]], on="location", how="left")
    scored = scored.sort_values("rank").reset_index(drop=True)
    scored.insert(0, "location_type", cfg["location_type"])
    scored["window_start"] = ws
    scored["window_end"] = we
    scored["scoring_version"] = sc.scoring_version

    bt = backtest.run_backtest(
        created, delay, first_day, last_day, sc, list(cfg["backtest_k"]), step,
        int(cfg["random_seed"]),
    )
    summary = backtest.summarize(bt)
    runtime = time.perf_counter() - t0

    coverage = {
        "coverage_start": str(totals.index.min()),
        "coverage_end": str(last_day),
        "partial_days_excluded": [str(d) for d in dropped],
        "full_days": n_days,
    }
    common = {
        "producer": "member3-priorities",
        "scoring_version": sc.scoring_version,
        "label": scoring.ADVISORY_LABEL,
        "input_path": str(input_path),
        "input_records": agg["records"],
        "records_without_location_excluded": agg["records_without_location"],
        "location_type": cfg["location_type"],
        "window_days": wd,
        "weights": cfg["weights"],
        "demand_source": demand_src,
        "backlog_source": backlog_src,
        "delay_source": (
            "median resolution_hours of records closed before the cut-off (binned histogram, "
            f"min {sc.min_delay_records} records, lookback {sc.delay_lookback_days or 'all'} days)"
        ),
        "feature_timestamps": {
            "window": "created_day in [window_start, window_end]",
            "cutoff": "window_end + 1 day 00:00 local; all inputs use records created before it",
            "backlog": "closed_at null or on/after cut-off date",
            "delay": "closed_at date before cut-off",
        },
        **coverage,
    }
    contracts.write_table(
        scored, root, "priority_scores",
        {**common, **stats, "n_units": len(scored), "runtime_seconds": round(runtime, 2)},
    )
    contracts.write_table(
        bt, root, "priority_backtest",
        {
            **common,
            "demand_source": "observed (backtest always uses observed inputs)",
            "backlog_source": "observed (backtest always uses observed inputs)",
            "step_days": step,
            "k_values": list(cfg["backtest_k"]),
            "random_seed": int(cfg["random_seed"]),
            "n_windows": int(bt[["window_end"]].drop_duplicates().shape[0]) if not bt.empty else 0,
            "summary": summary.astype(object).where(summary.notna(), None).to_dict(orient="records"),
            "aggregate_seconds": round(t_agg, 2),
            "runtime_seconds": round(runtime, 2),
        },
    )
    print(json.dumps(
        {
            "input": str(input_path),
            "records": agg["records"],
            "aggregate_rows": {"created": len(created), "delay": len(delay), "coords": len(coords)},
            "coverage": coverage,
            "scoring_window": [str(ws), str(we)],
            "units": len(scored),
            "demand_source": demand_src,
            "backlog_source": backlog_src,
            "backtest_windows": 0 if bt.empty else int(bt["window_end"].nunique()),
            "aggregate_seconds": round(t_agg, 2),
            "runtime_seconds": round(runtime, 2),
        },
        indent=2,
    ))
    if not summary.empty:
        print(summary.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
