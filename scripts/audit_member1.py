"""Reconcile final Member 1 forecasts, model artifacts, handoff and benchmark evidence."""

import argparse
import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.evaluation.member1_audit import audit_member1
from src.modeling.demand import load_demand_config
from src.utils.spark import get_spark


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--benchmark-config", required=True)
    args = parser.parse_args()
    cfg = load_demand_config(args.config)
    with open(args.benchmark_config, encoding="utf-8") as stream:
        benchmark = yaml.safe_load(stream)
    spark = get_spark("member1-final-audit")
    try:
        print(json.dumps(audit_member1(spark, cfg, benchmark, ROOT), indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
