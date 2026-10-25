"""Run equivalent, reconciled Pandas and Spark benchmarks on real data."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.evaluation.benchmark import run_benchmark
from src.utils.spark import get_spark


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", required=True)
    args = ap.parse_args()
    with open(args.config, encoding="utf-8") as stream:
        cfg = yaml.safe_load(stream)
    spark = get_spark("member1-benchmark")
    try:
        report = run_benchmark(spark, cfg, ROOT)
        print(json.dumps(report["summary"], indent=2))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
