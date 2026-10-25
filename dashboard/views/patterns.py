"""Pattern Explorer: Member 2 association rules and threshold runs. ASCII only.

All numbers shown are read from the pattern_rules / frequent_itemsets /
threshold_experiments tables exactly as produced (or from the labelled
fixture until Member 2 publishes them). Nothing is computed or invented here
beyond filtering and sorting.
"""

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

ui.page_header("Pattern Explorer")

rules = ad.load_contract("pattern_rules")
itemsets = ad.load_contract("frequent_itemsets")
experiments = ad.load_contract("mining_threshold_experiments")
ui.coverage_banner([rules, itemsets, experiments], label="Pattern mining")
st.info(
    "Association is not causation: a rule A -> B means complaint types A and B were "
    "reported in the same basket more often than independence would predict. It does "
    "not mean A causes B, and it reflects reporting behaviour as well as conditions."
)


def _first(df: pd.DataFrame, col: str, meta: dict) -> object:
    if col in df.columns and not df.empty:
        vals = df[col].dropna().unique()
        if len(vals) == 1:
            return vals[0]
        if len(vals) > 1:
            return ", ".join(str(v) for v in vals)
    return meta.get(col, "n/a")


st.subheader("Association rules")
if ui.source_badge(rules):
    df = rules.df.copy()
    c1, c2, c3 = st.columns(3)
    c1.metric("Baskets (n_baskets)", str(_first(df, "n_baskets", rules.meta)))
    c2.metric("Rules in table", f"{len(df):,}")
    c3.metric("Model version", str(_first(df, "model_version", rules.meta)))
    st.markdown(f"**Basket definition:** {_first(df, 'basket_definition', rules.meta)}")
    st.caption(
        f"Mining thresholds used: min_support={_first(df, 'min_support', rules.meta)}, "
        f"min_confidence={_first(df, 'min_confidence', rules.meta)}. "
        f"Input coverage: {ad.coverage_note(*ad.coverage(rules))}."
    )

    st.sidebar.header("Rule filters")
    def _slider(label: str, col: str, key: str) -> float:
        lo, hi = float(df[col].min()), float(df[col].max())
        if lo == hi:
            st.sidebar.caption(f"{label}: all rules have {lo:g}")
            return lo
        return st.sidebar.slider(label, lo, hi, lo, key=key)

    min_sup = _slider("Min support", "support", "pt_sup")
    min_conf = _slider("Min confidence", "confidence", "pt_conf")
    min_lift = _slider("Min lift", "lift", "pt_lift")
    search = st.sidebar.text_input("Item contains", "", key="pt_search").strip().lower()

    df["antecedent_text"] = df["antecedent"].map(ad.list_text)
    df["consequent_text"] = df["consequent"].map(ad.list_text)
    view = df[(df["support"] >= min_sup) & (df["confidence"] >= min_conf) & (df["lift"] >= min_lift)]
    if search:
        hit = view["antecedent_text"].str.lower().str.contains(search, regex=False) | view[
            "consequent_text"
        ].str.lower().str.contains(search, regex=False)
        view = view[hit]
    cols = [
        "antecedent_text", "consequent_text", "support", "confidence", "lift",
        "rule_baskets", "n_baskets",
    ]
    st.caption(f"{len(view)} of {len(df)} rules match the filters (exact table values).")
    if view.empty:
        st.info("No rules match the filters.")
    else:
        st.dataframe(
            view.sort_values(["lift", "confidence"], ascending=False)[cols].rename(
                columns={"antecedent_text": "antecedent", "consequent_text": "consequent"}
            ),
            hide_index=True,
        )
        fig = px.scatter(
            view, x="support", y="confidence", size="lift", color="lift",
            hover_data=["antecedent_text", "consequent_text", "rule_baskets"],
        )
        st.plotly_chart(fig, width="stretch")

left, right = st.columns(2)
with left:
    st.subheader("Threshold experiments")
    if ui.source_badge(experiments):
        st.dataframe(
            experiments.df.sort_values(["min_support", "min_confidence"]), hide_index=True
        )
with right:
    st.subheader("Frequent itemsets")
    if ui.source_badge(itemsets):
        fi = itemsets.df.assign(items=itemsets.df["items"].map(ad.list_text))
        st.dataframe(fi.sort_values("freq", ascending=False), hide_index=True, height=320)
