"""Run Phase 2 cleaning on verified Phase 4 history without replacing the sample."""

import argparse
import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.processing.history import process_history
from src.utils.spark import get_spark


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    with open(args.config, encoding="utf-8") as stream:
        config = yaml.safe_load(stream)
    spark = get_spark("member1-history")
    try:
        print(json.dumps(process_history(config, ROOT, spark), indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
