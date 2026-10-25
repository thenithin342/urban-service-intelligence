"""Priorities page: advisory priority score breakdown and backtest. ASCII only."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard import adapters as ad
from dashboard import common as ui

COMPONENTS = ("demand", "backlog", "delay")

ui.page_header(
    "Priorities",
    "ADVISORY operational priority for review - not a proven staffing optimum and not "
    "a dispatch instruction. No budget, crew capacity or cost data is used.",
)

ps = ad.load_contract("priority_scores", allow_fixture=False)
pb = ad.load_contract("priority_backtest", allow_fixture=False)
ui.coverage_banner([ps, pb], label="Priority input")
st.caption(
    "Volumes are reported complaints (absolute, not per-capita): areas that report more "
    "rank higher. Formula and feature timestamps: docs/member3/priority_scoring.md."
)
if ps.source == ad.MISSING:
    st.info(
        "No data: priority outputs have not been produced yet. Run "
        "`python scripts/run_member3_priorities.py --config configs/sample.yaml`."
    )

meta = ps.meta or {}
weights = meta.get("weights") or {}

if ui.source_badge(ps):
    df = ps.df.copy()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Scoring window", f"{df['window_start'].min()} to {df['window_end'].max()}")
    c2.metric("Units ranked", f"{len(df):,}")
    c3.metric("Location type", str(df["location_type"].iloc[0]))
    c4.metric("Scoring version", str(df["scoring_version"].iloc[0]))
    st.markdown(
        "**Score** = "
        + " + ".join(f"{weights.get(c, '?')} x {c}_component" for c in COMPONENTS)
        + " (each component min-max normalized to [0, 1] across units)."
    )
    st.caption(
        f"Demand source: {meta.get('demand_source', 'n/a')}. "
        f"Backlog source: {meta.get('backlog_source', 'n/a')}. "
        f"Delay source: {meta.get('delay_source', 'n/a')}."
    )

    st.sidebar.header("Filters")
    sel_l = ui.multiselect_filter("Location", ui.options(df, "location"), key="pr_loc")
    sel_c = ui.multiselect_filter("Complaint type", ui.options(df, "complaint_type"), key="pr_ctype")
    top_n = st.sidebar.slider("Show top N", 5, 100, 20, key="pr_topn")
    view = ui.apply_filter(ui.apply_filter(df, "location", sel_l), "complaint_type", sel_c)
    view = view.sort_values("rank").head(top_n)

    st.subheader("Score breakdown (weighted component contributions)")
    if view.empty:
        st.info("No units match the filters.")
    else:
        view = view.assign(unit=view["location"] + " / " + view["complaint_type"])
        contrib = pd.DataFrame(
            {
                "unit": view["unit"],
                "rank": view["rank"],
                **{c: float(weights.get(c, 0)) * view[f"{c}_component"] for c in COMPONENTS},
            }
        ).melt(id_vars=["unit", "rank"], var_name="component", value_name="contribution")
        fig = px.bar(
            contrib.sort_values("rank", ascending=False), x="contribution", y="unit",
            color="component", orientation="h", height=max(320, 22 * len(view)),
        )
        st.plotly_chart(fig, width="stretch")
        cols = [
            "rank", "location", "complaint_type", "priority_score",
            "demand_value", "backlog_value", "delay_value",
            "demand_component", "backlog_component", "delay_component",
        ]
        st.dataframe(view[cols], hide_index=True)

    st.subheader("Map (location centroid, sized by summed score of shown units)")
    if {"center_lat", "center_lon"} <= set(df.columns) and not view.empty:
        by_loc = (
            view.groupby("location", as_index=False)
            .agg(score=("priority_score", "sum"), units=("unit", "count"),
                 top_rank=("rank", "min"), center_lat=("center_lat", "first"),
                 center_lon=("center_lon", "first"))
        )
        pts = ad.valid_map_points(by_loc)
        if pts.empty:
            st.info("No valid coordinates for the shown locations.")
        else:
            fig = px.scatter_map(
                pts, lat="center_lat", lon="center_lon", size="score", color="top_rank",
                hover_data=["location", "units", "top_rank"], zoom=9, height=460,
                center={"lat": 40.70, "lon": -73.95}, map_style="carto-positron",
            )
            st.plotly_chart(fig, width="stretch")
            st.caption("Centroid = mean of valid in-bbox coordinates of the location's records.")
    else:
        st.info("Map needs location coordinates (center_lat/center_lon) in priority_scores.")

st.subheader("Backtest: does the ranking anticipate next-window volume?")
if ui.source_badge(pb):
    bt = pb.df.copy()
    ks = sorted(bt["k"].unique().tolist())
    k = st.selectbox("k", ks, index=0, key="pr_k")
    sub = bt[bt["k"] == k]
    summary = (
        sub.groupby("method", as_index=False)
        .agg(windows=("precision_at_k", "size"), mean_precision_at_k=("precision_at_k", "mean"),
             mean_spearman=("spearman", "mean"))
        .sort_values("mean_precision_at_k", ascending=False)
    )
    st.dataframe(summary, hide_index=True)
    fig = px.line(
        sub.assign(window_end=pd.to_datetime(sub["window_end"])),
        x="window_end", y="precision_at_k", color="method", markers=True,
    )
    st.plotly_chart(fig, width="stretch")
    st.caption(
        "Each point ranks units with data before the cut-off (window_end + 1 day) and compares "
        "the top-k with the true top-k by complaints created in the following window. "
        "volume_only = last-window count; random = fixed-seed random order. The priority "
        "score deliberately mixes backlog and delay, so it is not expected to beat a pure "
        "volume baseline at predicting volume; the comparison shows how much the ranking "
        "departs from volume, not whether it is the right staffing choice."
    )
