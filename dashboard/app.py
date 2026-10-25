"""Urban Service Intelligence dashboard entry point (Member 3). ASCII only.

Run: python -m streamlit run dashboard/app.py
Pages live in dashboard/views/ and read small aggregate artifacts only,
through dashboard/adapters.py.
"""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

VIEWS = Path(__file__).resolve().parent / "views"

st.set_page_config(page_title="Urban Service Intelligence", layout="wide")


PAGES = {
    "Analytics (Member 1 outputs)": [
        st.Page(str(VIEWS / "overview.py"), title="Overview", default=True),
        st.Page(str(VIEWS / "trends.py"), title="Trends"),
        st.Page(str(VIEWS / "hotspots.py"), title="Hotspots"),
        st.Page(str(VIEWS / "resolution.py"), title="Resolution"),
    ],
    "Models (Members 1-2 outputs)": [
        st.Page(str(VIEWS / "patterns.py"), title="Pattern Explorer"),
        st.Page(str(VIEWS / "forecasts.py"), title="Forecasts and Closure"),
    ],
    "Decision support (Member 3)": [
        st.Page(str(VIEWS / "priorities.py"), title="Priorities"),
        st.Page(str(VIEWS / "status.py"), title="Output Status"),
    ],
}

nav = st.navigation(PAGES)
st.sidebar.caption("Advisory prototype - not a dispatch tool.")
nav.run()
