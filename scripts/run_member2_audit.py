"""Member 2 Phase 3 CLI: flag reconciliation, closure-state and backlog audit.

Reads the cleaned Parquet and writes gitignored artifacts under
``artifacts/member2/audit`` (tables/ + audit.json). Also runs a Spark vs
pandas consistency check on a deterministic, bounded hash sample of the
input and cross-checks pairwise flag overlap against Member 1's
``flag_overlap`` table when it is available.

Usage:
  python scripts/run_member2_audit.py --config configs/sample.yaml
Optional:
  --input <parquet>   --outdir <dir>   --member1-tables <dir>   --master local[2]
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pandas as pd
from pyspark.sql import functions as F

from src.evaluation import quality_audit as qa
from src.evaluation.member2_config import (
    member2_config,
    read_yaml,
    resolve,
    utc_now,
    write_extra_table,
    write_json,
)
from src.modeling import resolution as res
from src.utils.spark import ensure_worker_env, get_spark

CONSISTENCY_BUCKETS = 5  # keep ~1/5 of rows, capped below


def _bounded_sample(df, max_rows: int):
    """Deterministic sample: rows whose unique_key hash falls in bucket 0."""
    sub = df.filter(F.pmod(F.xxhash64("unique_key"), F.lit(CONSISTENCY_BUCKETS)) == 0)
    sub = sub.orderBy("unique_key").limit(max_rows)
    return sub


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Member 2 Phase 3 audit")
    ap.add_argument("--config", required=True)
    ap.add_argument("--input", default=None, help="Override cleaned Parquet path")
    ap.add_argument("--outdir", default=None, help="Override member2 output root")
    ap.add_argument("--member1-tables", default=None,
                    help="Member 1 analytics tables dir (for flag_overlap cross-check)")
    ap.add_argument("--consistency-rows", type=int, default=20000)
    ap.add_argument("--master", default="local[2]")
    args = ap.parse_args(argv)

    ensure_worker_env()
    cfg = read_yaml(args.config)
    m2 = member2_config(cfg)
    input_path = Path(args.input) if args.input else resolve(ROOT, m2["input"])
    outdir = Path(args.outdir) if args.outdir else resolve(ROOT, m2["outdir"])
    audit_dir = outdir / "audit"
    if not input_path.exists():
        print(f"[member2] BLOCKER: cleaned Parquet not found at {input_path}", flush=True)
        return 3

    spark = get_spark("member2-audit", master=args.master)
    t0 = time.time()
    try:
        df = spark.read.parquet(str(input_path))
        n = df.count()
        if n == 0:
            print("[member2] BLOCKER: input is empty", flush=True)
            return 3
        cov = df.agg(F.min("created_day").alias("lo"), F.max("created_day").alias("hi"),
                     F.countDistinct("unique_key").alias("keys")).collect()[0]
        common = {
            "input_path": str(input_path),
            "coverage_start": str(cov["lo"]),
            "coverage_end": str(cov["hi"]),
        }

        print("[member2] flag reconciliation", flush=True)
        flags = qa.flag_reconciliation_spark(df)
        print("[member2] resolution / closure-state audit", flush=True)
        resolution = qa.resolution_audit_spark(df)
        labelled = res.add_closure_label_spark(df, m2["closure_horizon_days"])
        labels = res.label_summary_spark(labelled)

        tables = {
            "flag_per_flag": flags["per_flag"],
            "flag_combinations": flags["combinations"],
            "flag_pairwise": flags["pairwise"],
            "status_closure": resolution["status_closure"],
            "duration_classes": resolution["duration_classes"],
            "backlog_by_borough": resolution["backlog_by_borough"],
            "label_summary": labels,
        }
        for name, frame in tables.items():
            write_extra_table(frame, audit_dir / "tables" / name, common)

        print("[member2] Spark vs pandas consistency (bounded sample)", flush=True)
        sub = _bounded_sample(df, args.consistency_rows)
        sub_pd = sub.toPandas()
        s_flags = qa.flag_reconciliation_spark(sub)
        p_flags = qa.flag_reconciliation_pandas(sub_pd)
        s_res = qa.resolution_audit_spark(sub)
        p_res = qa.resolution_audit_pandas(sub_pd)
        s_lab = res.label_summary_spark(res.add_closure_label_spark(sub))
        p_lab = res.label_summary_pandas(res.add_closure_label_pandas(sub_pd))
        s_sum = res.resolution_summary_spark(res.add_closure_label_spark(sub)).toPandas()
        p_sum = res.resolution_summary_pandas(res.add_closure_label_pandas(sub_pd))
        s_sum["coverage_start"] = s_sum["coverage_start"].astype(str)
        s_sum["coverage_end"] = s_sum["coverage_end"].astype(str)
        p_sum["coverage_start"] = p_sum["coverage_start"].astype(str)
        p_sum["coverage_end"] = p_sum["coverage_end"].astype(str)
        consistency = {
            "sample_rows": len(sub_pd),
            "sample_rule": f"pmod(xxhash64(unique_key), {CONSISTENCY_BUCKETS}) == 0, "
                           f"first {args.consistency_rows} by unique_key",
            **{f"flags.{k}": v for k, v in qa.compare_spark_pandas(
                s_flags, p_flags, ["per_flag", "combinations", "pairwise"]).items()},
            **{f"resolution.{k}": v for k, v in qa.compare_spark_pandas(
                s_res, p_res, ["status_closure", "duration_classes",
                               "backlog_by_borough", "quantiles"]).items()},
            "labels.label_summary": qa.frames_equal(s_lab, p_lab),
            "labels.resolution_summary": qa.frames_equal(s_sum, p_sum, float_tol=1e-6),
        }

        m1_dir = (Path(args.member1_tables) if args.member1_tables
                  else ROOT / "artifacts" / "member1" / "analytics" / "tables")
        overlap_check: dict = {"member1_tables": str(m1_dir)}
        if (m1_dir / "flag_overlap").exists():
            m1 = pd.read_parquet(m1_dir / "flag_overlap")
            m1 = m1[["flag_a", "flag_b", "rows"]]
            m1 = m1.sort_values(["rows", "flag_a", "flag_b"], ascending=[False, True, True],
                                ignore_index=True)
            m1["rows"] = m1["rows"].astype("int64")
            overlap_check["matches_member2_pairwise"] = qa.frames_equal(
                m1, flags["pairwise"].reset_index(drop=True))
        else:
            overlap_check["matches_member2_pairwise"] = None
            overlap_check["note"] = "Member 1 flag_overlap not found; cross-check skipped"

        audit = {
            "producer": "member2",
            "generated_at_utc": utc_now(),
            **common,
            "row_count": int(n),
            "distinct_unique_key": int(cov["keys"]),
            "duplicate_unique_key": int(n - cov["keys"]),
            "flag_summary": flags["summary"],
            "per_flag": dict(zip(flags["per_flag"]["flag"],
                                 map(int, flags["per_flag"]["rows"]))),
            "resolution_quantiles_hours": resolution["quantiles"],
            "duration_classes": dict(zip(resolution["duration_classes"]["duration_class"],
                                         map(int, resolution["duration_classes"]["rows"]))),
            "backlog_unresolved_at_snapshot": int(
                resolution["status_closure"].loc[
                    ~resolution["status_closure"]["has_closed_at"], "rows"].sum()),
            "snapshot_min": resolution["snapshot_min"],
            "snapshot_max": resolution["snapshot_max"],
            "closed_at_max": resolution["closed_at_max"],
            "closed_after_snapshot_rows": resolution["closed_after_snapshot_rows"],
            "label_summary": labels.to_dict(orient="records"),
            "closure_horizon_days": m2["closure_horizon_days"],
            "spark_pandas_consistency": consistency,
            "member1_flag_overlap_check": overlap_check,
            "runtime_seconds": round(time.time() - t0, 2),
        }
        write_json(audit_dir / "audit.json", audit)
        print(pd.Series({k: v for k, v in audit.items()
                         if k in ("row_count", "per_flag", "flag_summary",
                                  "backlog_unresolved_at_snapshot")}).to_string(), flush=True)
        print(f"[member2] consistency: {consistency}", flush=True)
        print(f"[member2] member1 overlap: {overlap_check}", flush=True)
        failed = [k for k, v in consistency.items() if v is False]
        if failed or overlap_check.get("matches_member2_pairwise") is False:
            print(f"[member2] FAIL consistency checks: {failed}", flush=True)
            return 1
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
