"""Run PySpark ETL: raw CSV -> partitioned Parquet + quality report. ASCII only."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.processing.clean import clean, read_raw, write_clean
from src.utils.config import load_config, resolve_path
from src.utils.spark import ensure_worker_env, get_spark


def main() -> int:
    ap = argparse.ArgumentParser(description="Run PySpark ETL")
    ap.add_argument("--config", required=True, help="Path to YAML config")
    ap.add_argument("--snapshot-date", default=None, help="YYYY-MM-DD override")
    ap.add_argument("--master", default="local[2]")
    args = ap.parse_args()
    if args.snapshot_date is not None:
        try:
            date.fromisoformat(args.snapshot_date)
        except ValueError:
            ap.error("--snapshot-date must be YYYY-MM-DD")

    ensure_worker_env()
    cfg = load_config(args.config)
    raw_path = str(resolve_path(ROOT, cfg.get("input_csv", cfg["out_raw"])))
    out_path = str(resolve_path(ROOT, cfg.get("etl_output", "data/processed/nyc311_clean")))
    quality_path = resolve_path(
        ROOT, cfg.get("etl_quality_report", "data/processed/etl_quality.json")
    )
    quality_path.parent.mkdir(parents=True, exist_ok=True)

    spark = get_spark("urban-etl", master=args.master)
    try:
        print(f"[etl] reading {raw_path}", flush=True)
        df_raw = read_raw(spark, raw_path)
        df_clean, metrics = clean(df_raw, snapshot=args.snapshot_date)
        print(f"[etl] writing {out_path}", flush=True)
        write_clean(df_clean, out_path)
        metrics.update(
            {
                "input_path": cfg.get("input_csv", cfg["out_raw"]),
                "output_path": cfg.get("etl_output", "data/processed/nyc311_clean"),
                "spark_version": spark.version,
                "config": {k: cfg.get(k) for k in ("dataset_id", "where", "order", "seed")},
            }
        )
        with open(quality_path, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2)
        print(json.dumps(metrics, indent=2))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
