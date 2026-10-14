"""Minimal Phase 1 dashboard: reads computed EDA artifacts. ASCII only."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
ART = ROOT / "artifacts" / "eda_sample"


@st.cache_data
def load_csv(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    return pd.read_csv(path)


st.title("Urban Service Intelligence (Phase 1)")
st.caption("Decision-support prototype. Advisory outputs only, not dispatch.")
st.info("Source: NYC 311 Service Requests 2020-Present (erm2-nwe9). See data/manifests.")

top = load_csv(ART / "top_categories.csv")
bor = load_csv(ART / "by_borough.csv")
vol = load_csv(ART / "volume_daily.csv")

if top is None or bor is None or vol is None:
    st.warning("No EDA artifacts found. Run: python scripts/download_sample.py --config configs/sample.yaml, then profiling.")
else:
    st.subheader("Top complaint categories")
    st.bar_chart(top.set_index("complaint_type"))
    st.subheader("Complaints by borough")
    st.bar_chart(bor.set_index("borough"))
    st.subheader("Daily volume")
    st.line_chart(vol.set_index("period"))
