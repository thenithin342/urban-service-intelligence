"""Hotspots page: valid-coordinate grid cells, ZIPs and clusters. ASCII only."""

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

ui.page_header("Hotspots")

hot = ad.load_member1("spatial_hotspots")
grid = ad.load_member1("spatial_grid_cells")
zips = ad.load_member1("zip_hotspots")
clusters = ad.load_member1("cluster_summary")
ui.coverage_banner([hot, grid, zips, clusters], label="Member 1 Phase 3")
st.caption(
    "Spatial tables include valid coordinates only (NYC bounding box lat 40.49-40.92, "
    "lon -74.26 to -73.70); records with missing or invalid coordinates are excluded "
    "from maps but kept in non-spatial counts."
)

st.sidebar.header("Filters")
metric = st.sidebar.radio(
    "Colour cells by", ["density_per_km2", "complaint_count"], key="hs_metric"
)
min_count = st.sidebar.number_input("Minimum complaints per cell", 0, value=0, key="hs_min")
sel_b = ui.multiselect_filter(
    "Dominant borough (grid cells)", ui.options(grid.df, "dominant_borough"), key="hs_borough"
)

st.subheader("Hotspot grid cells")
if ui.source_badge(hot):
    df = hot.df[hot.df["complaint_count"] >= min_count]
    if sel_b and grid.usable:
        ids = ui.apply_filter(grid.df, "dominant_borough", sel_b)["spatial_id"]
        df = df[df["spatial_id"].isin(set(ids))]
    pts = ad.valid_map_points(df)
    dropped = len(df) - len(pts)
    if pts.empty:
        st.info("No hotspot cells match the filters.")
    else:
        fig = px.scatter_map(
            pts,
            lat="center_lat",
            lon="center_lon",
            color=metric,
            size="complaint_count",
            size_max=14,
            hover_data=["spatial_id", "complaint_count", "density_per_km2"],
            zoom=9.3,
            center={"lat": 40.70, "lon": -73.95},
            height=560,
            map_style="carto-positron",
        )
        st.plotly_chart(fig, width="stretch")
        st.caption(
            f"{len(pts)} cells shown; {dropped} dropped by the bounding-box guard. "
            "Hotspot = grid cell with at least the configured minimum complaint count."
        )
        st.dataframe(
            pts.sort_values(metric, ascending=False).head(25), hide_index=True
        )

left, right = st.columns(2)
with left:
    st.subheader("ZIP hotspots")
    if ui.source_badge(zips):
        st.dataframe(
            zips.df.sort_values("complaint_count", ascending=False), hide_index=True, height=320
        )
with right:
    st.subheader("KMeans cluster summary")
    if ui.source_badge(clusters):
        st.caption(
            "Cluster IDs are not stable across runs; interpret clusters by their attributes."
        )
        st.dataframe(clusters.df, hide_index=True)
