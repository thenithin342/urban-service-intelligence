"""Output Status page: freshness/validity of every contract table. ASCII only."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import streamlit as st

from dashboard import adapters as ad
from dashboard import common as ui

ui.page_header("Output Status", "Automated checks also available as scripts/check_outputs.py.")

inputs = ad.cleaned_inputs()
reports = ad.check_all()
manifest = ad.member1_manifest()
period = manifest.get("period") or {}
cov = ad.coverage_note(period.get("period_start"), period.get("period_end"))
n_ok = sum(r["status"] == "ok" for r in reports)
msg = (
    f"**Data coverage (Member 1 Phase 3 tables):** {cov}. "
    f"{n_ok} of {len(reports)} contract tables are real and pass all checks; "
    "the rest are FIXTURE-only, missing, invalid or stale as listed below. Counts: "
    + ", ".join(
        f"{status.upper()}={sum(r['status'] == status for r in reports)}"
        for status in sorted({r["status"] for r in reports})
    )
    + "."
)
if n_ok == len(reports):
    st.info(msg)
else:
    st.warning(msg)

st.subheader("Contract tables")
table = pd.DataFrame(
    [
        {
            "table": r["table"],
            "owner": r["owner"],
            "status": r["status"],
            "rows": r["rows"],
            "problems": "; ".join(r["problems"]),
        }
        for r in reports
    ]
)
st.dataframe(table, hide_index=True)

st.subheader("Cleaned inputs")
st.dataframe(
    pd.DataFrame(
        [
            {
                "dataset": i["name"],
                "ready (_SUCCESS)": i["ready"],
                "coverage_end": i["coverage_end"] or "",
                "written_at_utc": i["written_at"].isoformat() if i["written_at"] else "",
            }
            for i in inputs
        ]
    ),
    hide_index=True,
)
st.caption(
    "Stale = output generated before its input data was last written, or covering an "
    "earlier end date than the latest cleaned data."
)
