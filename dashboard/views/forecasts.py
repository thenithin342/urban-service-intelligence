"""Forecasts and Closure page: Member 1 demand + Member 2 closure models. ASCII only."""

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

ui.page_header("Forecasts and Closure")

fc = ad.load_contract("demand_forecast")
fm = ad.load_contract("demand_metrics")
cm = ad.load_contract("closure_model_comparison")
cc = ad.load_contract("closure_cohort_predictions")
ui.coverage_banner([fc, fm, cm, cc], label="Model input")
st.caption(
    "All evaluation uses chronological holdouts defined by the producers. Short "
    "training windows cannot support seasonal claims."
)


def _versions(df: pd.DataFrame) -> str:
    if "model_version" not in df.columns or df.empty:
        return "n/a"
    return ", ".join(sorted(df["model_version"].astype(str).unique()))


st.sidebar.header("Filters")
boroughs = sorted(set(ui.options(fc.df, "borough")) | set(ui.options(cc.df, "borough")))
ctypes = sorted(set(ui.options(fc.df, "complaint_type")) | set(ui.options(cc.df, "complaint_type")))
sel_b = ui.multiselect_filter("Borough", boroughs, key="fc_borough")
sel_c = ui.multiselect_filter("Complaint type", ctypes, key="fc_ctype")

tab_demand, tab_closure = st.tabs(["Demand forecast", "Closure (7-day)"])

with tab_demand:
    st.subheader("Demand forecast (Member 1)")
    if ui.source_badge(fc):
        df = fc.df.assign(
            borough=fc.df["borough"].fillna("ALL"),
            forecast_date=pd.to_datetime(fc.df["forecast_date"]),
        )
        cutoffs = pd.to_datetime(df["training_cutoff"])
        horizon = fc.meta.get("horizon_days")
        if horizon is None and not df.empty:
            horizon = int((df["forecast_date"].max() - cutoffs.max()).days)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Training cutoff", str(cutoffs.max().date()) if not df.empty else "n/a")
        c2.metric("Horizon (days)", str(horizon) if horizon is not None else "n/a")
        c3.metric(
            "Forecast dates",
            f"{df['forecast_date'].min().date()} to {df['forecast_date'].max().date()}"
            if not df.empty else "n/a",
        )
        c4.metric("Model version", _versions(df))
        df = ui.apply_filter(ui.apply_filter(df, "borough", sel_b), "complaint_type", sel_c)
        if df.empty:
            st.info("No forecast rows for the selected filters.")
        else:
            df = df.assign(series=df["borough"] + " / " + df["complaint_type"])
            fig = px.line(df, x="forecast_date", y="predicted_count", color="series", markers=True)
            st.plotly_chart(fig, width="stretch")
            st.dataframe(df.drop(columns=["series"]), hide_index=True)

    st.subheader("Forecast evaluation metrics")
    if ui.source_badge(fm):
        st.caption(
            "MAE / RMSE in complaints per day; WAPE = sum|error| / sum(actual), blank when "
            "the actual total is zero."
        )
        st.dataframe(fm.df, hide_index=True)
        long = fm.df.melt(id_vars="model", value_vars=["mae", "rmse"], var_name="metric")
        st.plotly_chart(
            px.bar(long, x="model", y="value", color="metric", barmode="group"), width="stretch"
        )

with tab_closure:
    st.subheader("Closure model comparison (Member 2)")
    if ui.source_badge(cm):
        c1, c2, c3 = st.columns(3)
        c1.metric("Train cutoff", str(cm.meta.get("train_cutoff", "n/a")))
        c2.metric("Test window", str(cm.meta.get("test_window", cm.meta.get("test_start", "n/a"))))
        c3.metric("Model version", _versions(cm.df))
        st.caption(
            "Target: closed within 7 days of creation, on label-mature cohorts only. "
            "Precision/recall at threshold 0.5. Compare against the prior-rate baseline."
        )
        st.dataframe(cm.df.sort_values(["split", "model"]), hide_index=True)
        test = cm.df[cm.df["split"] == "test"].dropna(subset=["pr_auc"])
        if not test.empty:
            st.plotly_chart(px.bar(test, x="model", y="pr_auc", title="Test PR-AUC"), width="stretch")

    st.subheader("Closure cohort predictions (backlog input)")
    if ui.source_badge(cc):
        df = cc.df.assign(borough=cc.df["borough"].fillna("UNKNOWN"))
        df = ui.apply_filter(ui.apply_filter(df, "borough", sel_b), "complaint_type", sel_c)
        if df.empty:
            st.info("No cohort rows for the selected filters.")
        else:
            win = (
                f"{pd.to_datetime(df['prediction_window_start']).min().date()} to "
                f"{pd.to_datetime(df['prediction_window_end']).max().date()}"
            )
            st.caption(
                f"Prediction window {win}. expected_open_after_7d = sum(1 - p_close) per cohort."
            )
            st.dataframe(df.sort_values("expected_open_after_7d", ascending=False), hide_index=True)
