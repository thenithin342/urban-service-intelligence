"""Compare Member 1 demand baselines and genuine Spark MLlib regressors."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.modeling.demand import load_demand_config, train_and_forecast
from src.utils.spark import get_spark


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", required=True)
    args = ap.parse_args()
    cfg = load_demand_config(args.config)
    spark = get_spark("member1-demand-models")
    try:
        print(json.dumps(train_and_forecast(spark, cfg, ROOT), indent=2))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
