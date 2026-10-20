"""Deterministic geospatial analytics for Member 1 Phase 3. ASCII only.

Coordinate rules (see docs/data_dictionary.md):
- Only rows with non-null latitude/longitude inside the NYC bounding box
  participate in spatial analysis. Invalid/missing coordinates are
  excluded here and only here; non-spatial counts keep every row.
- The phase-2 ETL already nulls invalid coordinates, so ``valid`` is a
  defensive re-check, never an imputation.

Grid: a fixed lat/lon degree grid over the NYC bounding box. Cells are
labelled ``row:col`` from the south-west corner. Cell area varies
slightly with latitude, so a per-cell area (km^2) is reported and
density is counts/area; raw counts are the primary measure.

Local projection: an equirectangular tangent-plane projection around a
NYC reference point, used only for metric KMeans features and cluster
centres. It is a local approximation (documented), not a geodetic CRS.
"""

from __future__ import annotations

import math
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

LAT_MIN, LAT_MAX = 40.49, 40.92
LON_MIN, LON_MAX = -74.26, -73.70

EARTH_RADIUS_M = 6371008.8
DEG_LAT_KM = 110.574
DEG_LON_KM_AT_EQ = 111.320


def valid_coords_filter() -> Any:
    lat, lon = F.col("latitude"), F.col("longitude")
    return (
        lat.isNotNull()
        & lon.isNotNull()
        & (lat >= LAT_MIN)
        & (lat <= LAT_MAX)
        & (lon >= LON_MIN)
        & (lon <= LON_MAX)
    )


def filter_valid_coords(df: DataFrame) -> DataFrame:
    return df.filter(valid_coords_filter())


def geo_quality_summary(df: DataFrame) -> dict[str, Any]:
    total = df.count()
    valid = filter_valid_coords(df).count()
    invalid_by_borough = {
        (r["borough"] if r["borough"] is not None else "UNKNOWN"): int(r["n"])
        for r in (
            df.filter(~valid_coords_filter())
            .groupBy("borough")
            .count()
            .withColumnRenamed("count", "n")
            .orderBy("borough")
            .collect()
        )
    }
    return {
        "total_rows": int(total),
        "valid_coords_rows": int(valid),
        "invalid_coords_rows": int(total - valid),
        "invalid_coords_share": float((total - valid) / total) if total else 0.0,
        "bbox": {
            "lat_min": LAT_MIN,
            "lat_max": LAT_MAX,
            "lon_min": LON_MIN,
            "lon_max": LON_MAX,
        },
        "invalid_coords_by_borough": invalid_by_borough,
    }


def _cell_columns(cell_deg: float) -> dict[str, Any]:
    lat, lon = F.col("latitude"), F.col("longitude")
    row = F.floor((lat - F.lit(LAT_MIN)) / F.lit(cell_deg))
    col = F.floor((lon - F.lit(LON_MIN)) / F.lit(cell_deg))
    return {
        "cell_id": F.concat_ws(
            ":", (row.cast("int")).cast("string"), (col.cast("int")).cast("string")
        ).alias("cell_id"),
        "center_lat": (
            F.lit(LAT_MIN) + (row + F.lit(0.5)) * F.lit(cell_deg)
        ).alias("center_lat"),
        "center_lon": (
            F.lit(LON_MIN) + (col + F.lit(0.5)) * F.lit(cell_deg)
        ).alias("center_lon"),
    }


def cell_area_km2(cell_deg: float, center_lat: Any) -> Any:
    h_km = cell_deg * DEG_LAT_KM
    w_km = cell_deg * DEG_LON_KM_AT_EQ * F.cos(F.radians(center_lat))
    return F.abs(h_km * w_km)


def spatial_hotspots(df: DataFrame, cell_deg: float, min_count: int) -> DataFrame:
    """Complaint density on a fixed grid, valid coordinates only."""
    cols = _cell_columns(cell_deg)
    grid = (
        filter_valid_coords(df)
        .select(cols["cell_id"].alias("spatial_id"), cols["center_lat"], cols["center_lon"])
        .groupBy("spatial_id", "center_lat", "center_lon")
        .count()
        .withColumnRenamed("count", "complaint_count")
        .filter(F.col("complaint_count") >= min_count)
    )
    grid = grid.withColumn(
        "cell_area_km2", F.round(cell_area_km2(cell_deg, F.col("center_lat")), 6)
    )
    grid = grid.withColumn(
        "density_per_km2", F.round(F.col("complaint_count") / F.col("cell_area_km2"), 3)
    )
    return (
        grid.withColumn("center_lat", F.round("center_lat", 5))
        .withColumn("center_lon", F.round("center_lon", 5))
        .orderBy(F.col("complaint_count").desc(), "spatial_id")
    )


def zip_hotspots(df: DataFrame, min_count: int) -> DataFrame:
    """Complaint counts per validated 5-digit ZIP (kept as text)."""
    return (
        df.filter(F.col("incident_zip").isNotNull())
        .groupBy(F.col("incident_zip").alias("zip"))
        .count()
        .withColumnRenamed("count", "complaint_count")
        .filter(F.col("complaint_count") >= min_count)
        .orderBy(F.col("complaint_count").desc(), F.col("zip").asc())
    )


def project_local(lat: Any, lon: Any, lat0: float, lon0: float) -> tuple[Any, Any]:
    """Equirectangular local projection to approximate metres around (lat0, lon0)."""
    x = (lon - F.lit(lon0)) * F.lit(DEG_LON_KM_AT_EQ * 1000.0) * F.cos(
        F.radians(F.lit(lat0))
    )
    y = (lat - F.lit(lat0)) * F.lit(DEG_LAT_KM * 1000.0)
    return x, y


def _dominant(
    df: DataFrame, cell_col: str, value_col: str, out_name: str
) -> DataFrame:
    counts = (
        df.groupBy(cell_col, value_col)
        .count()
        .withColumnRenamed("count", "_n")
        .fillna({value_col: "UNKNOWN"})
    )
    return (
        counts.orderBy(F.col("_n").desc(), F.col(cell_col).asc(), F.col(value_col).asc())
        .groupBy(cell_col)
        .agg(F.first(value_col).alias(out_name))
    )


def cell_features(
    df: DataFrame,
    cell_deg: float,
    lat0: float,
    lon0: float,
    min_count: int = 0,
) -> DataFrame:
    """Aggregated grid-cell feature table used for clustering.

    Columns: spatial_id, center_lat, center_lon, x_m, y_m, complaint_count,
    mean_resolution_hours, dominant_borough, dominant_complaint.
    """
    cols = _cell_columns(cell_deg)
    valid = filter_valid_coords(df)
    base = valid.select(
        cols["cell_id"].alias("spatial_id"),
        cols["center_lat"],
        cols["center_lon"],
        "borough",
        "complaint_type",
        "resolution_hours",
    )
    agg = (
        base.groupBy("spatial_id", "center_lat", "center_lon")
        .agg(
            F.count(F.lit(1)).alias("complaint_count"),
            F.mean("resolution_hours").alias("mean_resolution_hours"),
        )
        .filter(F.col("complaint_count") >= min_count)
    )
    dom_b = _dominant(base, "spatial_id", "borough", "dominant_borough")
    dom_c = _dominant(base, "spatial_id", "complaint_type", "dominant_complaint")
    x_m, y_m = project_local(F.col("center_lat"), F.col("center_lon"), lat0, lon0)
    out = (
        agg.join(dom_b, on="spatial_id", how="left")
        .join(dom_c, on="spatial_id", how="left")
        .withColumn("x_m", F.round(x_m, 3))
        .withColumn("y_m", F.round(y_m, 3))
        .withColumn(
            "mean_resolution_hours", F.round("mean_resolution_hours", 3)
        )
        .withColumn("center_lat", F.round("center_lat", 5))
        .withColumn("center_lon", F.round("center_lon", 5))
    )
    return out.orderBy("spatial_id")


def cluster_centers_to_latlon(
    x_m: float, y_m: float, lat0: float, lon0: float
) -> tuple[float, float]:
    """Python inverse of the local projection (for reporting cluster centres)."""
    lat = lat0 + y_m / (DEG_LAT_KM * 1000.0)
    lon = lon0 + x_m / (
        DEG_LON_KM_AT_EQ * 1000.0 * math.cos(math.radians(lat0))
    )
    return lat, lon


def deterministic_sample(df: DataFrame, cap: int, key_col: str = "unique_key") -> DataFrame:
    """Deterministic, partition-independent bounded sample (crc32 order)."""
    return df.orderBy(F.crc32(F.col(key_col)), F.col(key_col).asc()).limit(cap)


def sample_geo_points(df: DataFrame, cap: int) -> DataFrame:
    """Bounded valid-coordinate sample for single-node density clustering."""
    return deterministic_sample(filter_valid_coords(df), cap).select(
        "unique_key", "latitude", "longitude"
    )


def dbscan_clusters(
    points: Any, eps_meters: float, min_pts: int
) -> tuple[Any, dict[str, Any]]:
    """Single-node DBSCAN with haversine distance.

    ``points`` is a pandas frame with latitude/longitude columns (bounded
    sample). DBSCAN here is explicitly NOT distributed, even though the
    rest of Phase 3 uses Spark; this is disclosed in the report.
    """
    import numpy as np
    from sklearn.cluster import DBSCAN

    coords = np.radians(points[["latitude", "longitude"]].to_numpy(dtype=float))
    model = DBSCAN(
        eps=eps_meters / EARTH_RADIUS_M,
        min_samples=min_pts,
        metric="haversine",
        algorithm="ball_tree",
        n_jobs=1,
    )
    labels = model.fit_predict(coords)
    out = points.copy()
    out["cluster"] = labels
    noise = int((labels == -1).sum())
    n_clusters = len({x for x in labels if x != -1})
    summary = {
        "algorithm": "sklearn DBSCAN (haversine, single-node)",
        "sample_rows": len(points),
        "eps_meters": float(eps_meters),
        "min_pts": int(min_pts),
        "n_clusters_excluding_noise": n_clusters,
        "noise_points": noise,
        "noise_share": float(noise / len(points)) if len(points) else 0.0,
    }
    return out, summary
