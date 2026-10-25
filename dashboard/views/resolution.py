"""Resolution page: Member 1 duration stats + Member 2 closure summary. ASCII only."""

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

ui.page_header("Resolution")

rb = ad.load_member1("resolution_by_borough")
rs = ad.load_contract("resolution_summary")
ui.coverage_banner([rb, rs], label="Resolution")
st.caption(
    "Resolution hours use resolved records only (closed_at >= created_at); unresolved "
    "records are never imputed as zero. closed_at reflects the acquisition snapshot."
)

st.sidebar.header("Filters")
boroughs = sorted(set(ui.options(rb.df, "borough")) | set(ui.options(rs.df, "borough")))
sel_b = ui.multiselect_filter("Borough", boroughs, key="rs_borough")
sel_c = ui.multiselect_filter(
    "Complaint type (Member 2 table)", ui.options(rs.df, "complaint_type"), key="rs_ctype"
)

st.subheader("Resolution duration by borough (Member 1)")
if ui.source_badge(rb):
    df = ui.apply_filter(rb.df.assign(borough=rb.df["borough"].fillna("UNKNOWN")), "borough", sel_b)
    if df.empty:
        st.info("No rows for the selected boroughs.")
    else:
        long = df.melt(
            id_vars="borough", value_vars=["median_hours", "mean_hours", "p90_hours"],
            var_name="statistic", value_name="hours",
        )
        fig = px.bar(long, x="borough", y="hours", color="statistic", barmode="group")
        st.plotly_chart(fig, width="stretch")
        st.dataframe(df, hide_index=True)

st.subheader("Closure within 7 days by borough x complaint type (Member 2)")
if ui.source_badge(rs):
    df = rs.df.assign(borough=rs.df["borough"].fillna("UNKNOWN"))
    df = ui.apply_filter(ui.apply_filter(df, "borough", sel_b), "complaint_type", sel_c)
    if df.empty:
        st.info("No rows for the selected filters.")
    else:
        st.caption(
            "closure_rate_7d is computed on label-mature (eligible) records only; "
            "newer records are censored, not counted as failures."
        )
        st.dataframe(
            df.sort_values("total_count", ascending=False), hide_index=True
        )
        plot = df.dropna(subset=["closure_rate_7d"])
        if not plot.empty:
            fig = px.scatter(
                plot, x="median_resolution_hours", y="closure_rate_7d",
                size="eligible_count", color="borough", hover_data=["complaint_type"],
            )
            st.plotly_chart(fig, width="stretch")
