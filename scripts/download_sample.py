"""Download a bounded NYC 311 sample per YAML config. ASCII only."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.ingestion.downloader import download_sample
from src.utils.config import load_config


def main() -> int:
    ap = argparse.ArgumentParser(description="Download bounded NYC 311 sample")
    ap.add_argument("--config", required=True, help="Path to YAML config")
    args = ap.parse_args()
    cfg = load_config(args.config)
    manifest = download_sample(cfg, ROOT)
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
