"""Phase 5 stub: feature builder (not yet implemented). ASCII only."""

from __future__ import annotations

import argparse


def main() -> int:
    ap = argparse.ArgumentParser(description="Build features (Phase 5)")
    ap.add_argument("--config", required=True)
    ap.parse_args()
    print("PHASE 5 NOT YET IMPLEMENTED: feature builder comes in Phase 5.")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
