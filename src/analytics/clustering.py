"""Clustering study (Spark MLlib KMeans vs DBSCAN). ASCII only.

Feature space: aggregated spatial grid cells (see geospatial.cell_features)
with a local projected coordinate (metres) plus complaint volume. Scaling
is studied explicitly because raw projected metres dominate unscaled
distance and because count and metre scales differ by orders of magnitude.

KMeans runs as a genuine Spark MLlib pipeline (VectorAssembler ->
StandardScaler -> KMeans). DBSCAN is single-node on a bounded sample and
is labelled as such wherever it is reported. Clusters are descriptive
groups of reporting intensity, never "dangerous neighbourhoods".
"""

from __future__ import annotations

from typing import Any

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler

from src.analytics.geospatial import cluster_centers_to_latlon

BASE_FEATURES = ["x_m", "y_m"]


def _with_log_count(cell_df: Any) -> Any:
    return cell_df.withColumn("log_count", _log1p(cell_df["complaint_count"]))


def _log1p(col: Any) -> Any:
    from pyspark.sql import functions as F

    return F.log1p(col.cast("double"))


def build_variant(cell_df: Any, use_log: bool, scale: bool) -> tuple[Any, str]:
    """Fit preprocessing for one ablation variant; return (frame, feature_col).

    ``scale`` toggles StandardScaler (withMean/withStd). The returned
    frame carries an assembled feature vector plus a ``features`` column
    usable by KMeans.
    """
    df = _with_log_count(cell_df) if use_log else cell_df
    cols = BASE_FEATURES + (["log_count"] if use_log else ["complaint_count"])
    assembler = VectorAssembler(inputCols=cols, outputCol="assembled")
    stages = [assembler]
    feature_col = "assembled"
    if scale:
        scaler = StandardScaler(
            inputCol="assembled",
            outputCol="scaled",
            withMean=True,
            withStd=True,
        )
        stages.append(scaler)
        feature_col = "scaled"
    model = Pipeline(stages=stages).fit(df)
    return model.transform(df), feature_col


def evaluate_k(
    frame: Any, feature_col: str, k: int, seed: int
) -> tuple[Any, dict[str, Any]]:
    """Fit KMeans for one k; return (model, metrics)."""
    kmeans = KMeans(
        featuresCol=feature_col,
        predictionCol="cluster",
        k=k,
        seed=seed,
        initMode="k-means||",
        maxIter=100,
    )
    model = kmeans.fit(frame)
    predictions = model.transform(frame)
    evaluator = ClusteringEvaluator(
        featuresCol=feature_col,
        predictionCol="cluster",
        metricName="silhouette",
        distanceMeasure="squaredEuclidean",
    )
    metrics = {
        "k": int(k),
        "silhouette": float(evaluator.evaluate(predictions)),
        "wssse": float(model.summary.trainingCost),
    }
    return model.transform(frame), metrics


def select_k(
    frame: Any, feature_col: str, k_values: list[int], seed: int
) -> tuple[Any, list[dict[str, Any]]]:
    """Fit each k; return (best predictions by silhouette, metrics list)."""
    results = []
    best_pred, best_sil = None, float("-inf")
    for k in k_values:
        pred, m = evaluate_k(frame, feature_col, k, seed)
        results.append(m)
        if m["silhouette"] > best_sil:
            best_sil, best_pred = m["silhouette"], pred
    if best_pred is None:
        raise ValueError("no k values evaluated; check kmeans_k_values")
    results.sort(key=lambda r: r["k"])
    return best_pred, results


def _dominant_by_cluster(
    joined: Any, value_col: str, out_name: str
) -> dict[int, Any]:
    from pyspark.sql import functions as F

    counts = (
        joined.select("cluster", value_col)
        .fillna({value_col: "UNKNOWN"})
        .groupBy("cluster", value_col)
        .count()
        .withColumnRenamed("count", "_n")
    )
    rows = (
        counts.orderBy(F.col("_n").desc(), F.col("cluster").asc(), F.col(value_col).asc())
        .groupBy("cluster")
        .agg(F.first(value_col).alias(out_name))
        .collect()
    )
    return {int(r["cluster"]): r[out_name] for r in rows}


def cluster_summary(
    predictions: Any, cell_df: Any, lat0: float, lon0: float
) -> Any:
    """Per-cluster description: size, volume, centre, dominant labels."""
    from pyspark.sql import functions as F

    keyed = predictions.select("spatial_id", "cluster")
    cols = [
        "spatial_id",
        "x_m",
        "y_m",
        "complaint_count",
        "mean_resolution_hours",
        "dominant_borough",
        "dominant_complaint",
    ]
    joined = cell_df.select(*cols).join(keyed, on="spatial_id", how="inner")
    agg = joined.groupBy("cluster").agg(
        F.count(F.lit(1)).alias("cells"),
        F.sum("complaint_count").alias("complaints"),
        F.round(F.avg("x_m"), 2).alias("center_x_m"),
        F.round(F.avg("y_m"), 2).alias("center_y_m"),
        F.round(F.avg("complaint_count"), 3).alias("mean_complaints_per_cell"),
        F.round(F.avg("mean_resolution_hours"), 3).alias("mean_resolution_hours"),
    )
    borough = _dominant_by_cluster(joined, "dominant_borough", "dominant_borough")
    complaint = _dominant_by_cluster(joined, "dominant_complaint", "dominant_complaint")
    pdf = agg.orderBy("cluster").toPandas()
    pdf["center_lat"] = [
        cluster_centers_to_latlon(x, y, lat0, lon0)[0]
        for x, y in zip(pdf["center_x_m"], pdf["center_y_m"])
    ]
    pdf["center_lon"] = [
        cluster_centers_to_latlon(x, y, lat0, lon0)[1]
        for x, y in zip(pdf["center_x_m"], pdf["center_y_m"])
    ]
    pdf["dominant_borough"] = pdf["cluster"].map(borough)
    pdf["dominant_complaint"] = pdf["cluster"].map(complaint)
    pdf["center_lat"] = pdf["center_lat"].round(5)
    pdf["center_lon"] = pdf["center_lon"].round(5)
    return pdf


ABLATION_VARIANTS = [
    ("log_count_scaled", True, True),
    ("log_count_unscaled", True, False),
    ("count_scaled", False, True),
]


def preprocessing_ablation(
    cell_df: Any, k_values: list[int], seed: int
) -> list[dict[str, Any]]:
    """    Compare scaling/log variants on KMeans silhouette (same k grid)."""
    out: list[dict[str, Any]] = []
    n_cells = cell_df.count()
    for name, use_log, scale in ABLATION_VARIANTS:
        if n_cells < 2:
            out.append(
                {
                    "variant": name,
                    "use_log_count": use_log,
                    "scaled": scale,
                    "k": None,
                    "silhouette": None,
                    "wssse": None,
                    "note": "insufficient cells",
                }
            )
            continue
        frame, feature_col = build_variant(cell_df, use_log, scale)
        for k in k_values:
            if k > n_cells:
                continue
            _, m = evaluate_k(frame, feature_col, k, seed)
            out.append(
                {
                    "variant": name,
                    "use_log_count": use_log,
                    "scaled": scale,
                    "k": m["k"],
                    "silhouette": m["silhouette"],
                    "wssse": m["wssse"],
                    "note": "",
                }
            )
    return out
