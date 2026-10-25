"""Download complete bounded historical NYC 311 windows, resuming verified days."""

import argparse
import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.ingestion.history import download_history


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    with open(args.config, encoding="utf-8") as stream:
        config = yaml.safe_load(stream)
    result = download_history(config, ROOT)
    print(json.dumps({k: v for k, v in result.items() if k != "days"}, indent=2))


if __name__ == "__main__":
    main()
