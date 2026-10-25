"""Trends page: daily, weekly and hour x weekday volume. ASCII only."""

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

WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

ui.page_header("Trends")

daily = ad.load_member1("complaints_daily")
daily_b = ad.load_member1("complaints_daily_borough")
weekly = ad.load_member1("complaints_weekly")
hw = ad.load_member1("hour_weekday")
ui.coverage_banner([daily, daily_b, weekly, hw], label="Member 1 Phase 3")

partial = ((ad.member1_metrics().get("coverage") or {}).get("partial_day_dates")) or []
if partial:
    st.caption(
        f"Partial days (not complete 24h of records): {', '.join(partial)}. "
        "Their low counts reflect the acquisition cut-off, not a real drop."
    )

st.sidebar.header("Filters")
sel_b = ui.multiselect_filter("Borough", ui.options(daily_b.df, "borough"), key="tr_borough")
rng = ui.date_range_filter(daily_b.df if daily_b.usable else daily.df, "date", key="tr_dates")

st.subheader("Daily complaints")
if sel_b:
    if ui.source_badge(daily_b):
        df = daily_b.df.assign(borough=daily_b.df["borough"].fillna("UNKNOWN"))
        df = ui.apply_dates(ui.apply_filter(df, "borough", sel_b), "date", rng)
        if df.empty:
            st.info("No rows for the selected filters.")
        else:
            df = df.assign(date=pd.to_datetime(df["date"]))
            fig = px.line(df, x="date", y="complaint_count", color="borough", markers=True)
            st.plotly_chart(fig, width="stretch")
elif ui.source_badge(daily):
    df = ui.apply_dates(daily.df, "date", rng)
    if df.empty:
        st.info("No rows in the selected date range.")
    else:
        df = df.assign(date=pd.to_datetime(df["date"]))
        st.plotly_chart(px.line(df, x="date", y="complaint_count", markers=True), width="stretch")

left, right = st.columns(2)
with left:
    st.subheader("Weekly complaints")
    if ui.source_badge(weekly):
        st.caption("Weeks start Monday; edge weeks may be incomplete in a short window.")
        st.dataframe(weekly.df, hide_index=True)
with right:
    st.subheader("Average complaints per hour by weekday")
    if ui.source_badge(hw):
        pivot = hw.df.pivot_table(
            index="weekday", columns="hour", values="avg_complaints_per_hour", aggfunc="sum"
        ).reindex([d for d in WEEKDAYS if d in set(hw.df["weekday"])])
        fig = px.imshow(pivot, aspect="auto", labels={"color": "avg/hour"})
        st.plotly_chart(fig, width="stretch")
        st.caption("Normalized by the number of observed dates for each weekday.")
