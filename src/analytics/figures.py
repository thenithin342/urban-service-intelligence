"""Report-ready Phase 3 figures (matplotlib PNG + plotly HTML map). ASCII only.

Every figure:
- has a title, labelled axes with units, and a provenance stamp
  (period + producer version);
- returns a short, data-grounded takeaway string that is stored in the
  artifact manifest so the report text cannot drift from the figure.

Functions guard against empty/degenerate input and return ``None`` for
the path when there is nothing to draw, with an explanatory takeaway.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")  # headless, deterministic rendering

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

WEEKDAY_ORDER = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]
_ACCENT = "#1f4e79"
_GREY = "#888888"


def _period_text(period: dict[str, Any] | None) -> str:
    if not period:
        return "period unknown"
    return f"{period.get('period_start', '?')}..{period.get('period_end', '?')}"


def _save(fig: Any, path: Path, period: dict[str, Any] | None, producer: str) -> str:
    fig.text(
        0.005,
        0.005,
        f"NYC 311 reported complaints | {_period_text(period)} | {producer}",
        fontsize=6.5,
        color=_GREY,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=140, bbox_inches="tight")
    plt.close(fig)
    return str(path)


def _empty(path: Path) -> tuple[None, str]:
    return None, f"skipped: no data for {path.name}"


def bar_borough(
    df: pd.DataFrame, path: Path, period: Any = None, producer: str = ""
) -> tuple[str | None, str]:
    if df is None or df.empty:
        return _empty(path)
    d = df.dropna(subset=["complaint_count"]).sort_values(
        "complaint_count", ascending=True
    )
    fig, ax = plt.subplots(figsize=(8, 4.5))
    labels = d["borough"].fillna("UNKNOWN").astype(str)
    ax.barh(labels, d["complaint_count"], color=_ACCENT)
    ax.set_xlabel("Complaint count (records)")
    ax.set_title("Reported 311 complaints by borough")
    total = int(d["complaint_count"].sum())
    top = d.iloc[-1]
    takeaway = (
        f"{top['borough']} has the most reported complaints "
        f"({int(top['complaint_count']):,} of {total:,}); counts are reporting "
        "frequency, not infrastructure quality."
    )
    return _save(fig, path, period, producer), takeaway


def bar_categories(
    df: pd.DataFrame,
    path: Path,
    period: Any = None,
    producer: str = "",
    top: int = 15,
) -> tuple[str | None, str]:
    if df is None or df.empty:
        return _empty(path)
    d = df.sort_values("complaint_count", ascending=False).head(top).iloc[::-1]
    fig, ax = plt.subplots(figsize=(9, 5.5))
    ax.barh(d["complaint_type"].astype(str), d["complaint_count"], color=_ACCENT)
    ax.set_xlabel("Complaint count (records)")
    ax.set_title(f"Top {min(top, len(d))} complaint categories")
    toprow = d.iloc[-1]
    takeaway = (
        f"'{toprow['complaint_type']}' is the most reported category "
        f"({int(toprow['complaint_count']):,} records) in this window."
    )
    return _save(fig, path, period, producer), takeaway


def timeseries_daily(
    df: pd.DataFrame, path: Path, period: Any = None, producer: str = ""
) -> tuple[str | None, str]:
    if df is None or df.empty:
        return _empty(path)
    d = df.sort_values("date")
    x = pd.to_datetime(d["date"])
    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.plot(x, d["complaint_count"], marker="o", ms=3, color=_ACCENT)
    ax.set_xlabel("Date")
    ax.set_ylabel("Complaint count (records/day)")
    ax.set_title("Daily reported 311 complaint volume")
    fig.autofmt_xdate()
    mean = float(d["complaint_count"].mean())
    takeaway = (
        f"Daily volume averages {mean:,.0f} records with a range of "
        f"{int(d['complaint_count'].min()):,}-{int(d['complaint_count'].max()):,}; "
        "the window is too short for seasonal claims."
    )
    return _save(fig, path, period, producer), takeaway


def daily_lines_by_borough(
    df: pd.DataFrame, path: Path, period: Any = None, producer: str = ""
) -> tuple[str | None, str]:
    if df is None or df.empty:
        return _empty(path)
    d = df.dropna(subset=["date"]).copy()
    d["borough"] = d["borough"].fillna("UNKNOWN")
    piv = d.pivot_table(
        index="date", columns="borough", values="complaint_count", aggfunc="sum"
    ).fillna(0)
    piv.index = pd.to_datetime(piv.index)
    fig, ax = plt.subplots(figsize=(9, 4.5))
    for col in piv.columns:
        ax.plot(piv.index, piv[col], label=str(col), linewidth=1.4)
    ax.set_xlabel("Date")
    ax.set_ylabel("Complaint count (records/day)")
    ax.set_title("Daily volume by borough")
    ax.legend(fontsize=8, ncol=2)
    fig.autofmt_xdate()
    shares = piv.sum()
    top = shares.idxmax()
    takeaway = (
        f"{top} contributes the largest share of daily volume "
        f"({shares[top] / shares.sum():.1%} of records in window)."
    )
    return _save(fig, path, period, producer), takeaway


def histogram_resolution(
    df: pd.DataFrame, path: Path, period: Any = None, producer: str = ""
) -> tuple[str | None, str]:
    if df is None or df.empty:
        return _empty(path)
    v = pd.to_numeric(df["resolution_hours"], errors="coerce").dropna()
    if v.empty:
        return _empty(path)
    p99 = float(v.quantile(0.99))
    clipped = v.clip(upper=p99) if p99 > 0 else v
    fig, ax = plt.subplots(figsize=(8, 4.5))
    bins = np.linspace(0, max(p99, 1.0), 50)
    ax.hist(clipped, bins=bins, color=_ACCENT, edgecolor="white")
    ax.set_xlabel("Resolution time (hours, clipped at 99th percentile)")
    ax.set_ylabel("Resolved complaints (count)")
    ax.set_title("Distribution of resolution time (resolved rows only)")
    takeaway = (
        f"Median resolution is {v.median():.1f} h (p90 {v.quantile(0.9):.1f} h, "
        f"p99 {p99:.1f} h); unresolved rows are excluded, not zero-filled."
    )
    return _save(fig, path, period, producer), takeaway


def box_resolution_by_borough(
    df: pd.DataFrame, path: Path, period: Any = None, producer: str = ""
) -> tuple[str | None, str]:
    if df is None or df.empty:
        return _empty(path)
    d = df.dropna(subset=["resolution_hours"]).copy()
    if d.empty:
        return _empty(path)
    groups, labels = [], []
    for b, sub in d.groupby(d["borough"].fillna("UNKNOWN")):
        v = pd.to_numeric(sub["resolution_hours"], errors="coerce").dropna()
        if len(v):
            groups.append(v.to_numpy())
            labels.append(str(b))
    if not groups:
        return _empty(path)
    fig, ax = plt.subplots(figsize=(9, 4.8))
    ax.boxplot(groups, tick_labels=labels, showfliers=False, patch_artist=True)
    ax.set_yscale("log")
    ax.set_ylabel("Resolution time (hours, log scale)")
    ax.set_xlabel("Borough")
    ax.set_title("Resolution time by borough (resolved rows only, outliers hidden)")
    medians = [float(np.median(g)) for g in groups]
    worst = labels[int(np.argmax(medians))]
    takeaway = (
        f"Median resolution is highest in {worst} ({max(medians):.1f} h) among "
        "boroughs; box plots use resolved rows only and hide outliers."
    )
    return _save(fig, path, period, producer), takeaway


def violin_daily_by_borough(
    df: pd.DataFrame, path: Path, period: Any = None, producer: str = ""
) -> tuple[str | None, str]:
    if df is None or df.empty:
        return _empty(path)
    d = df.copy()
    d["borough"] = d["borough"].fillna("UNKNOWN")
    groups, labels = [], []
    for b, sub in d.groupby("borough"):
        v = pd.to_numeric(sub["complaint_count"], errors="coerce").dropna()
        if len(v) >= 2:
            groups.append(v.to_numpy())
            labels.append(str(b))
    if not groups:
        return _empty(path)
    fig, ax = plt.subplots(figsize=(9, 4.8))
    parts = ax.violinplot(groups, showmeans=False, showextrema=True, showmedians=True)
    for pc in parts["bodies"]:
        pc.set_facecolor(_ACCENT)
        pc.set_alpha(0.5)
    ax.set_xticks(range(1, len(labels) + 1))
    ax.set_xticklabels(labels)
    ax.set_ylabel("Daily complaint count (records/day)")
    ax.set_xlabel("Borough")
    ax.set_title("Per-day volume spread by borough")
    takeaway = (
        "Violins show the spread of daily volume within each borough; wide "
        "spreads mean unstable daily demand, not higher counts."
    )
    return _save(fig, path, period, producer), takeaway


def kde_daily_volume(
    df: pd.DataFrame, path: Path, period: Any = None, producer: str = ""
) -> tuple[str | None, str]:
    if df is None or df.empty:
        return _empty(path)
    v = pd.to_numeric(df["complaint_count"], errors="coerce").dropna().to_numpy()
    if len(v) < 3 or np.ptp(v) == 0:
        return _empty(path)
    from scipy.stats import gaussian_kde

    kde = gaussian_kde(v)
    grid = np.linspace(v.min(), v.max(), 200)
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.plot(grid, kde(grid), color=_ACCENT)
    ax.fill_between(grid, kde(grid), color=_ACCENT, alpha=0.2)
    ax.set_xlabel("Daily complaint count (records/day)")
    ax.set_ylabel("Density (KDE)")
    ax.set_title("Estimated density of daily complaint volume")
    takeaway = (
        f"Daily volume is centred near {np.median(v):,.0f} records/day; KDE is a "
        "smoothing of the observed days only."
    )
    return _save(fig, path, period, producer), takeaway


def heatmap_hour_weekday(
    df: pd.DataFrame, path: Path, period: Any = None, producer: str = ""
) -> tuple[str | None, str]:
    if df is None or df.empty:
        return _empty(path)
    piv = df.pivot_table(
        index="weekday", columns="hour", values="complaint_count", aggfunc="sum"
    )
    piv = piv.reindex([d for d in WEEKDAY_ORDER if d in piv.index])
    if piv.empty:
        return _empty(path)
    fig, ax = plt.subplots(figsize=(11, 4.2))
    im = ax.imshow(piv.to_numpy(), aspect="auto", cmap="viridis")
    ax.set_xticks(range(len(piv.columns)))
    ax.set_xticklabels([int(c) for c in piv.columns])
    ax.set_yticks(range(len(piv.index)))
    ax.set_yticklabels(piv.index)
    ax.set_xlabel("Hour of day (0-23, NYC local wall time)")
    ax.set_ylabel("Weekday")
    ax.set_title("Complaint volume heatmap: hour x weekday")
    cbar = fig.colorbar(im, ax=ax)
    cbar.set_label("Complaint count (records)")
    flat = piv.stack()
    takeaway = (
        f"Peak cell is {flat.idxmax()[0]} {int(flat.idxmax()[1])}:00 "
        f"({int(flat.max()):,} records); reporting has clear time-of-day structure."
    )
    return _save(fig, path, period, producer), takeaway


def radar_borough_profile(
    df: pd.DataFrame,
    path: Path,
    period: Any = None,
    producer: str = "",
    top: int = 6,
) -> tuple[str | None, str]:
    if df is None or df.empty:
        return _empty(path)
    d = df.copy()
    d["borough"] = d["borough"].fillna("UNKNOWN")
    totals = d.groupby("complaint_type")["complaint_count"].sum().sort_values(
        ascending=False
    )
    cats = list(totals.head(top).index)
    if not cats:
        return _empty(path)
    sub = d[d["complaint_type"].isin(cats)]
    piv = sub.pivot_table(
        index="borough", columns="complaint_type", values="complaint_count", aggfunc="sum"
    ).fillna(0)
    share = piv.div(piv.sum(axis=1).replace(0, np.nan), axis=0).fillna(0) * 100.0
    if share.empty:
        return _empty(path)
    angles = np.linspace(0, 2 * np.pi, len(cats), endpoint=False).tolist()
    angles += angles[:1]
    fig, ax = plt.subplots(figsize=(7, 6.5), subplot_kw={"polar": True})
    for borough in share.index:
        vals = share.loc[borough, cats].tolist()
        vals += vals[:1]
        ax.plot(angles, vals, linewidth=1.4, label=str(borough))
        ax.fill(angles, vals, alpha=0.06)
    ax.set_xticks(angles[:-1])
    short = [c[:18] + ("..." if len(c) > 18 else "") for c in cats]
    ax.set_xticklabels(short, fontsize=7)
    ax.set_title("Category mix by borough (share of borough volume, %)", pad=18)
    ax.legend(loc="upper right", bbox_to_anchor=(1.25, 1.1), fontsize=7)
    takeaway = (
        "Radar compares only normalised category shares per borough; a longer "
        "spoke means a larger share of that borough's own volume, not more records."
    )
    return _save(fig, path, period, producer), takeaway


def silhouette_curve(
    df: pd.DataFrame, path: Path, period: Any = None, producer: str = ""
) -> tuple[str | None, str]:
    if df is None or df.empty:
        return _empty(path)
    fig, ax = plt.subplots(figsize=(7.5, 4.3))
    ax.plot(df["k"], df["silhouette"], marker="o", color=_ACCENT)
    best = df.loc[df["silhouette"].idxmax()]
    ax.axvline(best["k"], color=_GREY, linestyle="--", linewidth=1)
    ax.set_xlabel("Number of clusters k")
    ax.set_ylabel("Silhouette score (squared Euclidean on scaled features)")
    ax.set_title("KMeans model selection: silhouette vs k")
    takeaway = (
        f"Best silhouette {best['silhouette']:.3f} at k={int(best['k'])}; "
        "silhouette alone does not prove cluster meaning."
    )
    return _save(fig, path, period, producer), takeaway


def cluster_scatter(
    df: pd.DataFrame, path: Path, period: Any = None, producer: str = ""
) -> tuple[str | None, str]:
    if df is None or df.empty:
        return _empty(path)
    d = df.dropna(subset=["center_lat", "center_lon"])
    if d.empty:
        return _empty(path)
    fig, ax = plt.subplots(figsize=(7.5, 6.5))
    sizes = 20 + 180 * (d["complaints"] / d["complaints"].max())
    sc = ax.scatter(
        d["center_lon"],
        d["center_lat"],
        s=sizes,
        c=d["cluster"],
        cmap="tab10",
        alpha=0.75,
        edgecolor="white",
        linewidth=0.4,
    )
    for _, r in d.iterrows():
        ax.annotate(
            f"C{int(r['cluster'])}",
            (r["center_lon"], r["center_lat"]),
            fontsize=8,
            ha="center",
            va="center",
        )
    ax.set_xlabel("Longitude (deg)")
    ax.set_ylabel("Latitude (deg)")
    ax.set_title("KMeans spatial clusters (bubble size = complaints)")
    fig.colorbar(sc, ax=ax, label="Cluster id")
    takeaway = (
        f"{len(d)} cluster centres shown over valid coordinates; clusters are "
        "descriptive reporting-intensity groups, not risk labels."
    )
    return _save(fig, path, period, producer), takeaway


def dbscan_scatter(
    df: pd.DataFrame, path: Path, period: Any = None, producer: str = ""
) -> tuple[str | None, str]:
    if df is None or df.empty:
        return _empty(path)
    d = df.dropna(subset=["latitude", "longitude"])
    if d.empty:
        return _empty(path)
    noise = d[d["cluster"] == -1]
    core = d[d["cluster"] != -1]
    fig, ax = plt.subplots(figsize=(7.5, 6.5))
    ax.scatter(
        noise["longitude"], noise["latitude"], s=4, c=_GREY, alpha=0.4, label="noise"
    )
    sc = ax.scatter(
        core["longitude"],
        core["latitude"],
        s=6,
        c=core["cluster"],
        cmap="tab20",
        alpha=0.8,
    )
    ax.set_xlabel("Longitude (deg)")
    ax.set_ylabel("Latitude (deg)")
    ax.set_title("DBSCAN spatial clusters on bounded sample (haversine)")
    ax.legend(loc="upper left", fontsize=8)
    fig.colorbar(sc, ax=ax, label="Cluster id")
    ncl = int(core["cluster"].nunique())
    share = 0.0 if len(d) == 0 else len(noise) / len(d)
    takeaway = (
        f"DBSCAN found {ncl} dense clusters with {share:.1%} noise on a bounded "
        "sample; it is single-node, not distributed, and eps is in metres."
    )
    return _save(fig, path, period, producer), takeaway


def dbscan_sensitivity(
    df: pd.DataFrame, path: Path, period: Any = None, producer: str = ""
) -> tuple[str | None, str]:
    """DBSCAN parameter effect: cluster count and noise share vs eps."""
    if df is None or df.empty:
        return _empty(path)
    d = df.sort_values("eps_meters")
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.plot(
        d["eps_meters"],
        d["n_clusters_excluding_noise"],
        marker="o",
        color=_ACCENT,
        label="clusters (excl. noise)",
    )
    ax.set_xlabel("DBSCAN eps (metres, haversine)")
    ax.set_ylabel("Cluster count")
    ax.set_xscale("log")
    ax2 = ax.twinx()
    ax2.plot(
        d["eps_meters"],
        d["noise_share"] * 100.0,
        marker="s",
        color="#b5651d",
        label="noise share (%)",
    )
    ax2.set_ylabel("Noise share (%)")
    lines = ax.get_lines() + ax2.get_lines()
    ax.legend(lines, [ln.get_label() for ln in lines], fontsize=8, loc="best")
    ax.set_title("DBSCAN parameter sensitivity (fixed min_pts)")
    small = d.iloc[0]
    takeaway = (
        f"At eps={small['eps_meters']:.0f} m noise is "
        f"{small['noise_share']:.1%} with {int(small['n_clusters_excluding_noise'])} "
        "clusters; larger eps merges points and reduces noise, showing that the "
        "density result is parameter-dependent."
    )
    return _save(fig, path, period, producer), takeaway


def hotspot_map_plotly(
    df: pd.DataFrame, path: Path, period: Any = None, producer: str = ""
) -> tuple[str | None, str]:
    """Interactive plotly geospatial map (HTML, no tile server required)."""
    if df is None or df.empty:
        return _empty(path)
    import plotly.graph_objects as go

    d = df.dropna(subset=["center_lat", "center_lon"])
    if d.empty:
        return _empty(path)
    fig = go.Figure(
        go.Scattergeo(
            lon=d["center_lon"],
            lat=d["center_lat"],
            text=d["spatial_id"],
            marker={
                "size": (4 + 18 * (d["complaint_count"] / d["complaint_count"].max())),
                "color": d["complaint_count"],
                "colorscale": "Viridis",
                "colorbar": {"title": "Complaints"},
                "line": {"width": 0.3, "color": "white"},
            },
            hovertemplate=(
                "cell %{text}<br>lat %{lat:.4f} lon %{lon:.4f}"
                "<br>complaints %{marker.color}<extra></extra>"
            ),
        )
    )
    fig.update_geos(
        scope="north america",
        lataxis_range=[40.45, 40.95],
        lonaxis_range=[-74.3, -73.65],
        showland=True,
        landcolor="#f2f2f2",
        showcountries=False,
        showcoastlines=True,
    )
    fig.update_layout(
        title=(
            "Reported 311 complaint density grid (valid NYC coordinates) | "
            f"{_period_text(period)} | {producer}"
        ),
        margin={"l": 0, "r": 0, "t": 40, "b": 0},
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(str(path), include_plotlyjs="cdn")
    takeaway = (
        f"{len(d)} grid cells with >= threshold records; darker/larger = more "
        "reported complaints (counts, not population-adjusted rates)."
    )
    return str(path), takeaway
