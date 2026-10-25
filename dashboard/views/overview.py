"""Overview page: volume by borough, category and agency. ASCII only."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import plotly.express as px
import streamlit as st

from dashboard import adapters as ad
from dashboard import common as ui

ui.page_header("Overview")

borough = ad.load_member1("complaints_by_borough")
category = ad.load_member1("complaints_by_category")
agency = ad.load_member1("complaints_by_agency")
bcat = ad.load_member1("borough_category")
ui.coverage_banner([borough, category, agency, bcat], label="Member 1 Phase 3")

manifest = ad.member1_manifest()
quality = ad.etl_quality()
c1, c2, c3, c4 = st.columns(4)
c1.metric("Records analysed", f"{manifest['row_count']:,}" if manifest.get("row_count") else "n/a")
period = manifest.get("period") or {}
c2.metric("Period start", period.get("period_start", "n/a"))
c3.metric("Period end", period.get("period_end", "n/a"))
c4.metric(
    "Rows with any quality flag",
    f"{quality['flagged_rows']:,}" if "flagged_rows" in quality else "n/a",
)
partial = ((ad.member1_metrics().get("coverage") or {}).get("partial_day_dates")) or []
if partial:
    st.caption(f"Partial (incomplete) days in the window: {', '.join(partial)}.")

st.sidebar.header("Filters")
sel_b = ui.multiselect_filter("Borough", ui.options(bcat.df, "borough"), key="ov_borough")
top_n = st.sidebar.slider("Top N categories", 5, 30, 15, key="ov_topn")

left, right = st.columns(2)
with left:
    st.subheader("Complaints by borough")
    if ui.source_badge(borough):
        df = borough.df.assign(borough=borough.df["borough"].fillna("UNKNOWN"))
        df = ui.apply_filter(df, "borough", sel_b)
        if df.empty:
            st.info("No rows for the selected boroughs.")
        else:
            fig = px.bar(df.sort_values("complaint_count"), x="complaint_count", y="borough")
            st.plotly_chart(fig, width="stretch")
with right:
    st.subheader("Complaints by agency")
    if ui.source_badge(agency):
        df = agency.df.sort_values("complaint_count", ascending=False).head(top_n)
        st.plotly_chart(px.bar(df, x="agency", y="complaint_count"), width="stretch")

st.subheader("Top complaint categories")
if sel_b:
    if ui.source_badge(bcat):
        df = ui.apply_filter(bcat.df, "borough", sel_b)
        df = (
            df.groupby("complaint_type", dropna=False)["complaint_count"].sum().reset_index()
            .sort_values("complaint_count", ascending=False)
            .head(top_n)
        )
        if df.empty:
            st.info("No rows for the selected boroughs.")
        else:
            st.caption(f"Filtered to: {', '.join(sel_b)} (from borough_category).")
            st.plotly_chart(px.bar(df, x="complaint_type", y="complaint_count"), width="stretch")
elif ui.source_badge(category):
    df = category.df.sort_values("complaint_count", ascending=False).head(top_n)
    st.plotly_chart(px.bar(df, x="complaint_type", y="complaint_count"), width="stretch")

with st.expander("Data quality flag overlap (records can carry several flags)"):
    overlap = ad.load_member1("flag_overlap")
    if ui.source_badge(overlap):
        st.dataframe(overlap.df, hide_index=True)
    if quality.get("flag_breakdown"):
        st.json(quality["flag_breakdown"], expanded=False)
