"""Member 2 Phase 7 CLI: sensitivity, error breakdowns, reproducibility, figures.

Reads the published Member 2 artifacts under ``<root>/artifacts/member2``
(run run_member2_mining.py and run_member2_resolution.py first) and writes
``artifacts/member2/evaluation/{evaluation.json, tables/}`` and
``artifacts/member2/figures/*.png``. With ``--repro`` it re-runs mining (no
grid) and resolution into a scratch root and compares content hashes of
every deterministic output with the published ones.

Usage:
  python scripts/run_member2_evaluation.py --config configs/history.yaml \
      --input <parquet> [--repro] [--repro-root <dir>]
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import pandas as pd

from src.evaluation import member2_figures as figs
from src.evaluation import mining_eval
from src.evaluation import model_metrics as mm
from src.evaluation.member2_config import (
    frame_hash,
    member2_config,
    read_yaml,
    resolve,
    utc_now,
    write_extra_table,
    write_json,
)
from src.utils import contracts

# Contract tables plus extras compared by the reproducibility check.
REPRO_TABLES = {
    "pattern_rules": None,
    "frequent_itemsets": None,
    "resolution_summary": None,
    "closure_model_comparison": None,
    "closure_cohort_predictions": None,
}
REPRO_EXTRAS = [
    "mining/tables/baskets_week",
    "resolution/predictions/test_predictions",
    "resolution/tables/calibration",
    "resolution/tables/stability_by_borough",
    "resolution/tables/stability_by_category",
]


def _hash_dir(path: Path) -> dict:
    if not path.exists():
        return {"exists": False}
    df = pd.read_parquet(path)
    return {"exists": True, "rows": len(df), "sha256": frame_hash(df)}


def _hashes(root: Path) -> dict:
    out = {name: _hash_dir(contracts.contract_path(root, name)) for name in REPRO_TABLES}
    for rel in REPRO_EXTRAS:
        out[rel] = _hash_dir(root / "artifacts" / "member2" / rel)
    return out


def _reproduce(args, root: Path) -> dict:
    import run_member2_mining
    import run_member2_resolution

    scratch = Path(args.repro_root) if args.repro_root else Path(
        tempfile.mkdtemp(prefix="member2_repro_"))
    common = ["--config", args.config, "--root", str(scratch), "--master", args.master]
    if args.input:
        common += ["--input", args.input]
    t0 = time.time()
    rc_m = run_member2_mining.main([*common, "--no-grid"])
    rc_r = run_member2_resolution.main([*common, "--no-importance"])
    first, second = _hashes(root), _hashes(scratch)
    tables = {}
    for k in first:
        a, b = first[k], second[k]
        tables[k] = {"identical": a.get("exists") and a == b,
                     "rows": a.get("rows"), "rerun_rows": b.get("rows"),
                     "sha256": a.get("sha256")}
    return {"rerun_root": str(scratch), "exit_codes": {"mining": rc_m, "resolution": rc_r},
            "runtime_seconds": round(time.time() - t0, 2), "tables": tables,
            "all_identical": all(v["identical"] for v in tables.values())}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Member 2 Phase 7 evaluation")
    ap.add_argument("--config", required=True)
    ap.add_argument("--input", default=None, help="Cleaned Parquet (for --repro reruns)")
    ap.add_argument("--root", default=None, help="Root holding artifacts/member2")
    ap.add_argument("--repro", action="store_true", help="Re-run and compare hashes")
    ap.add_argument("--repro-root", default=None)
    ap.add_argument("--master", default="local[2]")
    args = ap.parse_args(argv)

    cfg = read_yaml(args.config)
    m2 = member2_config(cfg)
    root = Path(args.root) if args.root else resolve(ROOT, m2["artifacts_root"])
    base = root / "artifacts" / "member2"
    ev_dir, fig_dir = base / "evaluation", base / "figures"
    needed = ["pattern_rules", "mining_threshold_experiments", "closure_model_comparison",
              "closure_cohort_predictions", "resolution_summary"]
    missing = [n for n in needed if not contracts.contract_path(root, n).exists()]
    if missing:
        print(f"[member2] BLOCKER: missing artifacts {missing}; run mining/resolution first",
              flush=True)
        return 3

    tables = {n: pd.read_parquet(contracts.contract_path(root, n)) for n in needed}
    contract_problems = {n: contracts.validate(t, n) for n, t in tables.items()}
    mining_metrics = json.loads((base / "mining" / "mining_metrics.json").read_text())
    res_metrics = json.loads((base / "resolution" / "resolution_metrics.json").read_text())
    audit = json.loads((base / "audit" / "audit.json").read_text())
    pred = pd.read_parquet(base / "resolution" / "predictions" / "test_predictions")
    cal = pd.read_parquet(base / "resolution" / "tables" / "calibration")
    stab_b = pd.read_parquet(base / "resolution" / "tables" / "stability_by_borough")
    stab_c = pd.read_parquet(base / "resolution" / "tables" / "stability_by_category")
    selected = res_metrics["selected_model"]
    t_lo, t_hi = res_metrics["test_window"]
    window = f"{t_lo} to {t_hi}"
    meta = {"source_root": str(root), "test_window": [t_lo, t_hi],
            "selected_model": selected}

    # FP-Growth sensitivity (from the measured threshold grid).
    grid = tables["mining_threshold_experiments"]
    sens = mining_eval.sensitivity_summary(grid)
    write_extra_table(sens, ev_dir / "tables" / "mining_sensitivity", meta)

    # Error breakdowns by agency and borough x category (selected model).
    pred = pred.assign(p_selected=pred[f"p_{selected}"])
    by_agency = mm.group_metrics(pred, "agency", "closed_within_7d", "p_selected",
                                 "p_prior_rate", m2["min_cohort_n"])
    write_extra_table(by_agency, ev_dir / "tables" / "error_by_agency", meta)
    cohort_err = pred.assign(err=pred["closed_within_7d"] - pred["p_selected"])
    cohort_err = cohort_err.groupby(["borough", "complaint_type"], dropna=False).agg(
        n=("err", "size"), mean_residual=("err", "mean"),
        residual_sum=("err", "sum")).reset_index()
    cohort_err["small_sample"] = cohort_err["n"] < m2["min_cohort_n"]
    cohort_err = cohort_err.sort_values(["n", "borough", "complaint_type"],
                                        ascending=[False, True, True], ignore_index=True)
    write_extra_table(cohort_err, ev_dir / "tables" / "error_by_cohort", meta)

    # Integrity checks: duplicates, date leakage, contracts.
    test_days = pd.to_datetime(pred["created_day"])
    train_hi = pd.Timestamp(res_metrics["train_window"][1])
    integrity = {
        "contract_problems": contract_problems,
        "input_duplicate_unique_key": audit.get("duplicate_unique_key"),
        "test_predictions_duplicate_unique_key": int(pred["unique_key"].duplicated().sum()),
        "test_days_within_window": bool(test_days.min() >= pd.Timestamp(t_lo)
                                        and test_days.max() <= pd.Timestamp(t_hi)),
        "train_max_day_before_test_min_day": bool(train_hi < test_days.min()),
        "embargo_gap_days": res_metrics["leakage"]["embargo_gap_days"],
        "forbidden_features_used": res_metrics["leakage"]["forbidden_in_features"],
        "train_test_key_overlap": res_metrics["leakage"]["train_test_key_overlap"],
        "cohort_n_matches_test_rows": int(
            tables["closure_cohort_predictions"]["n_complaints"].sum()) == len(pred),
        "rules_regression_max_abs_diff":
            mining_metrics["primary"]["regression_check"]["max_abs_diff"],
        "spark_vs_pandas_baskets_equal":
            mining_metrics["primary"]["spark_vs_pandas_baskets_equal"],
    }

    print("[member2] figures", flush=True)
    cov = f"Coverage {mining_metrics['primary']['stats']['coverage_start']} to " \
          f"{mining_metrics['primary']['stats']['coverage_end']}."
    figures = {
        "threshold_sensitivity": figs.threshold_sensitivity(
            grid, fig_dir / "threshold_sensitivity.png", cov),
        "model_comparison": figs.model_comparison(
            tables["closure_model_comparison"], fig_dir / "model_comparison_brier.png",
            window),
        "calibration": figs.calibration(cal, fig_dir / "calibration_test.png", window),
        "pr_curves": figs.pr_curves(
            pred, [m for m in res_metrics["test_metrics"]], fig_dir / "pr_curves_test.png",
            window),
        "stability_borough": figs.group_brier(
            stab_b, "borough", fig_dir / "stability_by_borough.png",
            f"Brier by borough: {selected} vs prior", window),
        "stability_category": figs.group_brier(
            stab_c, "complaint_type", fig_dir / "stability_by_category.png",
            f"Brier by complaint type (top 15 by volume): {selected} vs prior", window),
    }

    repro = _reproduce(args, root) if args.repro else None
    small_c = stab_c[stab_c["small_sample"]]
    out = {
        "producer": "member2", "generated_at_utc": utc_now(), "source_root": str(root),
        "selected_model": selected, "test_window": [t_lo, t_hi],
        "integrity": integrity,
        "stability": {
            "boroughs": len(stab_b),
            "categories": len(stab_c),
            "small_sample_categories": len(small_c),
            "small_sample_category_rows": int(small_c["n"].sum()),
            "worst_category_calibration_gap": stab_c.loc[
                stab_c["calibration_gap"].abs().idxmax()].to_dict() if len(stab_c) else None,
        },
        "mining_sensitivity": sens.to_dict(orient="records"),
        "figures": figures,
        "reproducibility": repro,
    }
    write_json(ev_dir / "evaluation.json", out)
    print(json.dumps({"integrity": integrity, "reproducibility": None if repro is None else {
        "all_identical": repro["all_identical"], "exit_codes": repro["exit_codes"]}},
        indent=2, default=str), flush=True)
    ok = (not any(contract_problems.values())
          and integrity["test_predictions_duplicate_unique_key"] == 0
          and integrity["train_max_day_before_test_min_day"]
          and integrity["cohort_n_matches_test_rows"]
          and (repro is None or repro["all_identical"]))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
