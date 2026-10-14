"""Phase 2 stub: PySpark ETL (not yet implemented). ASCII only."""

from __future__ import annotations

import argparse


def main() -> int:
    ap = argparse.ArgumentParser(description="Run PySpark ETL (Phase 2)")
    ap.add_argument("--config", required=True)
    ap.parse_args()
    print("PHASE 2 NOT YET IMPLEMENTED: ETL pipeline comes in Phase 2.")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
