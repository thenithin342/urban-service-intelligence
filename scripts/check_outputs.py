"""Check every shared contract table for presence, validity and freshness.

Reports, per table in src/utils/contracts.CONTRACTS: missing, fixture-only,
contract violations, missing model_version, and staleness (generated before
its input data was written, or coverage_end older than the latest cleaned
data coverage). Exits 1 when any problem is found.

--allow-missing  treat missing / fixture-only tables as warnings (CI use,
                 where gitignored artifacts are never present).
--json           print the full report as JSON.

ASCII only.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard import adapters as ad

SOFT = {ad.MISSING, ad.FIXTURE}


def evaluate(reports: list[dict], allow_missing: bool) -> int:
    """Return the exit code for a list of check_table reports."""
    for r in reports:
        if r["status"] == "ok":
            continue
        if allow_missing and r["status"] in SOFT:
            continue
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--allow-missing", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    reports = ad.check_all()
    code = evaluate(reports, args.allow_missing)
    if args.json:
        print(json.dumps({"exit_code": code, "tables": reports}, indent=2, default=str))
        return code
    width = max(len(r["table"]) for r in reports)
    for r in reports:
        soft = args.allow_missing and r["status"] in SOFT
        tag = "OK  " if r["status"] == "ok" else ("WARN" if soft else "FAIL")
        line = f"[{tag}] {r['table']:<{width}}  {r['status']:<8} rows={r['rows']}"
        print(line)
        for p in r["problems"]:
            print(f"         - {p}")
        for w in r["warnings"]:
            print(f"         ~ {w}")
    n_bad = sum(r["status"] != "ok" for r in reports)
    print(f"{len(reports) - n_bad}/{len(reports)} tables ok; exit {code}")
    return code


if __name__ == "__main__":
    raise SystemExit(main())
