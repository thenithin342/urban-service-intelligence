"""Member 2 Phase 4 CLI: location-time baskets, FP-Growth rules, threshold grid.

Writes the contract tables ``pattern_rules``, ``frequent_itemsets`` and
``mining_threshold_experiments`` (docs/data_contracts.md 2.1) under
``<root>/artifacts/member2/mining/tables`` plus non-contract extras
(baskets, secondary-granularity rules) and ``mining_metrics.json``.

Usage:
  python scripts/run_member2_mining.py --config configs/sample.yaml
Optional:
  --input <parquet>   --root <dir> (contract root, default repo root)
  --master local[2]   --no-grid
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pandas as pd

from src.evaluation import mining_eval
from src.evaluation.member2_config import (
    MODEL_VERSION_MINING,
    PRODUCER,
    frame_hash,
    member2_config,
    read_yaml,
    resolve,
    utc_now,
    write_extra_table,
    write_json,
)
from src.mining import baskets as bk
from src.mining import fpgrowth as fp
from src.utils import contracts
from src.utils.spark import ensure_worker_env, get_spark

BASKET_COLUMNS = ["unique_key", "complaint_type", "incident_zip", "created_day",
                  "created_week"]


def _d(text: str) -> date:
    return date.fromisoformat(text)


def _run_granularity(df, m2: dict, granularity: str, supports: list[float],
                     confidences: list[float], run_grid: bool):
    drop = bool(m2["drop_partial_weeks"]) and granularity == "week"
    baskets, stats = bk.build_baskets_spark(df, granularity, drop)
    baskets = baskets.cache()
    n = stats["n_baskets"]
    grid, skipped = (fp.threshold_grid(baskets, n, supports, confidences,
                                       m2["max_itemsets_guard"])
                     if run_grid else (pd.DataFrame(), []))
    if not grid.empty:
        grid.insert(0, "granularity", granularity)
        grid["basket_definition"] = stats["basket_definition"]
    return baskets, stats, grid, skipped


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Member 2 Phase 4 FP-Growth mining")
    ap.add_argument("--config", required=True)
    ap.add_argument("--input", default=None, help="Override cleaned Parquet path")
    ap.add_argument("--root", default=None,
                    help="Root for contract paths (default: repo root)")
    ap.add_argument("--no-grid", action="store_true", help="Skip threshold grid")
    ap.add_argument("--master", default="local[2]")
    args = ap.parse_args(argv)

    ensure_worker_env()
    cfg = read_yaml(args.config)
    m2 = member2_config(cfg)
    input_path = Path(args.input) if args.input else resolve(ROOT, m2["input"])
    root = Path(args.root) if args.root else resolve(ROOT, m2["artifacts_root"])
    mining_dir = root / "artifacts" / "member2" / "mining"
    if not input_path.exists():
        print(f"[member2] BLOCKER: cleaned Parquet not found at {input_path}", flush=True)
        return 3

    spark = get_spark("member2-mining", master=args.master)
    t0 = time.time()
    try:
        df = spark.read.parquet(str(input_path)).select(*BASKET_COLUMNS).cache()
        if df.count() == 0:
            print("[member2] BLOCKER: input is empty", flush=True)
            return 3

        print("[member2] primary baskets + threshold grid", flush=True)
        prim = m2["basket_granularity"]
        p_baskets, p_stats, p_grid, p_skip = _run_granularity(
            df, m2, prim, m2["support_grid"], m2["confidence_grid"], not args.no_grid)
        sec = m2["secondary_granularity"]
        s_baskets = s_stats = None
        s_grid, s_skip = pd.DataFrame(), []
        if sec:
            print(f"[member2] secondary baskets ({sec}) + threshold grid", flush=True)
            s_baskets, s_stats, s_grid, s_skip = _run_granularity(
                df, m2, sec, m2["secondary_support_grid"], m2["confidence_grid"],
                not args.no_grid)

        print("[member2] primary rules", flush=True)
        n = p_stats["n_baskets"]
        primary = fp.mine(p_baskets, n, m2["min_support"], m2["min_confidence"],
                          m2["max_itemsets_guard"])
        cov_start, cov_end = _d(p_stats["coverage_start"]), _d(p_stats["coverage_end"])
        rules = primary["rules"].copy()
        rules["min_support"] = m2["min_support"]
        rules["min_confidence"] = m2["min_confidence"]
        rules["basket_definition"] = p_stats["basket_definition"]
        rules["coverage_start"] = cov_start
        rules["coverage_end"] = cov_end
        rules["model_version"] = MODEL_VERSION_MINING
        # Contract columns plus the additive is_redundant flag.
        rules = rules[[*contracts.CONTRACTS["pattern_rules"].columns, "is_redundant"]]
        itemsets = primary["itemsets"].copy()
        itemsets["model_version"] = MODEL_VERSION_MINING
        itemsets = itemsets[list(contracts.CONTRACTS["frequent_itemsets"].columns)]

        base_meta = {
            "producer": PRODUCER,
            "model_version": MODEL_VERSION_MINING,
            "input_path": str(input_path),
            "coverage_start": p_stats["coverage_start"],
            "coverage_end": p_stats["coverage_end"],
            "basket_definition": p_stats["basket_definition"],
            "n_baskets": n,
            "note": "Associations between co-reported complaint categories; not causal.",
        }
        contracts.write_table(rules, root, "pattern_rules", {
            **base_meta, "min_support": m2["min_support"],
            "min_confidence": m2["min_confidence"]})
        contracts.write_table(itemsets, root, "frequent_itemsets", {
            **base_meta, "min_support": m2["min_support"]})
        grid = pd.concat([g for g in (p_grid, s_grid) if not g.empty], ignore_index=True)
        if not grid.empty:
            contracts.write_table(grid, root, "mining_threshold_experiments", {
                **base_meta, "basket_definition": None,
                "basket_definitions": {prim: p_stats["basket_definition"],
                                       **({sec: s_stats["basket_definition"]} if sec else {})},
                "skipped": p_skip + s_skip,
                "runtime_note": "fit + itemset/rule materialization, local Spark, "
                                f"master={args.master}",
            })

        print("[member2] regression checks", flush=True)
        p_pd = p_baskets.toPandas()
        p_pd["period"] = pd.to_datetime(p_pd["period"]).dt.date
        p_pd["items"] = p_pd["items"].map(list)
        check = mining_eval.verify_rules(p_pd["items"], primary["rules"])
        ref = bk.build_baskets_pandas(
            pd.read_parquet(input_path, columns=BASKET_COLUMNS), prim,
            bool(m2["drop_partial_weeks"]) and prim == "week")
        spark_vs_pandas = bool(
            len(ref) == len(p_pd)
            and (ref["incident_zip"].tolist() == p_pd["incident_zip"].tolist())
            and (ref["period"].tolist() == p_pd["period"].tolist())
            and all(list(a) == list(b) for a, b in zip(ref["items"], p_pd["items"])))
        write_extra_table(p_pd, mining_dir / "tables" / f"baskets_{prim}", base_meta)

        secondary_out = None
        if sec:
            s_rules = fp.mine(s_baskets, s_stats["n_baskets"], m2["secondary_min_support"],
                              m2["secondary_min_confidence"], m2["max_itemsets_guard"])
            sr = s_rules["rules"]
            write_extra_table(sr, mining_dir / "tables" / f"pattern_rules_{sec}", {
                **base_meta, "basket_definition": s_stats["basket_definition"],
                "n_baskets": s_stats["n_baskets"],
                "min_support": m2["secondary_min_support"],
                "min_confidence": m2["secondary_min_confidence"]})
            s_pd = s_baskets.toPandas()
            s_check = mining_eval.verify_rules(s_pd["items"].map(list), sr, max_rules=500)
            secondary_out = {
                "stats": s_stats,
                "n_rules": len(sr),
                "n_itemsets": s_rules["n_itemsets"],
                "regression_check": s_check,
                "n_rules_non_redundant": int((~sr["is_redundant"]).sum()),
                "top_rules_by_lift": _top(sr),
                "top_non_redundant_rules_by_lift": _top(sr[~sr["is_redundant"]]),
            }

        metrics = {
            "producer": PRODUCER,
            "model_version": MODEL_VERSION_MINING,
            "generated_at_utc": utc_now(),
            "config_path": args.config,
            "input_path": str(input_path),
            "spark_version": spark.version,
            "primary": {
                "stats": p_stats,
                "min_support": m2["min_support"],
                "min_confidence": m2["min_confidence"],
                "n_itemsets": primary["n_itemsets"],
                "n_rules": len(rules),
                "n_rules_lift_gt_1_2": int((rules["lift"] > 1.2).sum()),
                "n_rules_non_redundant": int((~rules["is_redundant"]).sum()),
                "regression_check": check,
                "spark_vs_pandas_baskets_equal": spark_vs_pandas,
                "top_rules_by_lift": _top(primary["rules"]),
                "top_non_redundant_rules_by_lift": _top(
                    primary["rules"][~primary["rules"]["is_redundant"]]),
                "rules_sha256": frame_hash(rules.drop(columns=["model_version"])),
                "itemsets_sha256": frame_hash(itemsets),
            },
            "secondary": secondary_out,
            "grid_rows": len(grid),
            "skipped_grid_points": p_skip + s_skip,
            "runtime_seconds": round(time.time() - t0, 2),
        }
        write_json(mining_dir / "mining_metrics.json", metrics)
        print(pd.Series({
            "n_baskets": n, "n_itemsets": primary["n_itemsets"], "n_rules": len(rules),
            "spark_vs_pandas_baskets_equal": spark_vs_pandas,
            "max_abs_diff": check["max_abs_diff"],
        }).to_string(), flush=True)
        if not grid.empty:
            print(grid.drop(columns=["basket_definition"]).to_string(), flush=True)
        ok = spark_vs_pandas and max(check["max_abs_diff"][k]
                                     for k in ("support", "confidence", "lift")) < 1e-9
        return 0 if ok else 1
    finally:
        spark.stop()


def _top(rules: pd.DataFrame, k: int = 10) -> list[dict]:
    cols = ["antecedent", "consequent", "support", "confidence", "lift", "rule_baskets"]
    out = rules.head(k)[cols].to_dict(orient="records")
    for r in out:
        r["antecedent"] = list(r["antecedent"])
        r["consequent"] = list(r["consequent"])
    return out


if __name__ == "__main__":
    raise SystemExit(main())
