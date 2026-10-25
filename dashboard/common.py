"""Shared Streamlit UI helpers: banners, source badges, filters. ASCII only."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date

import pandas as pd
import streamlit as st

from dashboard import adapters as ad

FIXTURE_TEXT = (
    "FIXTURE - synthetic contract fixture with invented values, shown only so "
    "the layout can be reviewed. These are NOT results."
)


def page_header(title: str, caption: str | None = None) -> None:
    st.title(title)
    st.caption(
        caption
        or "Decision-support prototype over NYC 311 (erm2-nwe9). Advisory outputs only, not dispatch."
    )


def coverage_banner(results: Iterable[ad.TableResult], label: str = "Data") -> None:
    """One banner per page: coverage dates plus real/fixture/missing status."""
    results = list(results)
    starts, ends = [], []
    for r in results:
        if r.source in (ad.REAL, ad.FIXTURE):
            s, e = ad.coverage(r)
            if s:
                starts.append(s)
            if e:
                ends.append(e)
    status = {r.name: r.source for r in results}
    real = [n for n, s in status.items() if s == ad.REAL]
    fixture = [n for n, s in status.items() if s == ad.FIXTURE]
    missing = [n for n, s in status.items() if s == ad.MISSING]
    invalid = [n for n, s in status.items() if s == ad.INVALID]
    cov = ad.coverage_note(min(starts) if starts else None, max(ends) if ends else None)
    parts = [f"**{label} coverage:** {cov}."]
    if real:
        parts.append(f"REAL: {', '.join(real)}.")
    if fixture:
        parts.append(f"FIXTURE: {', '.join(fixture)}.")
    if missing:
        parts.append(f"MISSING: {', '.join(missing)}.")
    if invalid:
        parts.append(f"INVALID (contract violation, not rendered): {', '.join(invalid)}.")
    parts.append("Counts are reported-complaint frequency, not per-capita rates.")
    text = " ".join(parts)
    if invalid:
        st.error(text)
    elif fixture or missing:
        st.warning(text)
    else:
        st.info(text)


def source_badge(result: ad.TableResult) -> bool:
    """Render a per-panel source label. Returns True when the panel may render."""
    if result.source == ad.FIXTURE:
        st.warning(f"{result.name}: {FIXTURE_TEXT}")
        return True
    if result.source == ad.MISSING:
        st.info(f"No data: `{result.name}` has not been produced yet ({result.path}).")
        return False
    if result.source == ad.INVALID:
        st.error(f"`{result.name}` violates its contract and is not rendered:")
        for p in result.problems[:10]:
            st.code(p, language=None)
        return False
    if result.df.empty:
        st.info(f"`{result.name}` is empty.")
        return False
    version = result.meta.get("model_version") or result.meta.get("scoring_version")
    extra = f", version {version}" if version else ""
    st.caption(f"Source: REAL output `{result.name}`{extra}.")
    return True


def options(df: pd.DataFrame, col: str) -> list[str]:
    if df.empty or col not in df.columns:
        return []
    return sorted(df[col].fillna("UNKNOWN").astype(str).unique().tolist())


def multiselect_filter(label: str, values: list[str], key: str) -> list[str]:
    """Sidebar multiselect; empty selection means 'all'."""
    if not values:
        return []
    return st.sidebar.multiselect(label, values, default=[], key=key, help="Empty = all")


def apply_filter(df: pd.DataFrame, col: str, selected: list[str]) -> pd.DataFrame:
    if not selected or df.empty or col not in df.columns:
        return df
    return df[df[col].fillna("UNKNOWN").astype(str).isin(selected)]


def date_range_filter(df: pd.DataFrame, col: str, key: str) -> tuple[date, date] | None:
    if df.empty or col not in df.columns:
        return None
    dates = pd.to_datetime(df[col])
    lo, hi = dates.min().date(), dates.max().date()
    picked = st.sidebar.date_input(
        "Date range", value=(lo, hi), min_value=lo, max_value=hi, key=key
    )
    if isinstance(picked, (tuple, list)) and len(picked) == 2:
        return picked[0], picked[1]
    return lo, hi


def apply_dates(df: pd.DataFrame, col: str, rng: tuple[date, date] | None) -> pd.DataFrame:
    if rng is None or df.empty:
        return df
    d = pd.to_datetime(df[col]).dt.date
    return df[(d >= rng[0]) & (d <= rng[1])]
