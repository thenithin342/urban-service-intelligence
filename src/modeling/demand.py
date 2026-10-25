"""Retrospective borough demand experiments with purged chronological evaluation."""

from __future__ import annotations

import json
import math
import time
from datetime import date, timedelta
from functools import reduce
from pathlib import Path

import yaml
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    OneHotEncoder,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.ingestion.downloader import sha256_file
from src.ingestion.history import daily_windows, save_json

HORIZON = 7
WARMUP = 28
BOROUGHS = ["BRONX", "BROOKLYN", "MANHATTAN", "QUEENS", "STATEN ISLAND", "UNKNOWN"]
NUMERIC = ["count_0", "lag_1", "lag_7", "lag_14", "lag_28", "mean_7", "mean_28",
           "seasonal_naive", "weekday_mean", "horizon_days", "weekday_sin", "weekday_cos"]
MODELS = ["seasonal_naive", "weekday_mean", "lr_scaled", "lr_unscaled", "random_forest", "gbt"]


def load_demand_config(path):
    with open(path, encoding="utf-8") as stream:
        cfg = yaml.safe_load(stream)
    if not isinstance(cfg, dict):
        raise TypeError("demand configuration must be a mapping")
    for key in ("input_parquet", "source_manifest", "quality_report", "output_dir", "seed", "train_end", "validation_end"):
        if key not in cfg:
            raise ValueError(f"missing demand configuration: {key}")
    train, validation = date.fromisoformat(str(cfg["train_end"])), date.fromisoformat(str(cfg["validation_end"]))
    cfg.update(train_end=train.isoformat(), validation_end=validation.isoformat(), seed=int(cfg["seed"]))
    if train >= validation or cfg["seed"] < 0:
        raise ValueError("ordered train/validation dates and nonnegative seed required")
    return cfg


def verified_daily(spark, cfg, root):
    """Zero-fill borough days only within a source-verified complete calendar."""
    manifest_path = root / cfg["source_manifest"]
    manifest_hash = sha256_file(manifest_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    quality = json.loads((root / cfg["quality_report"]).read_text(encoding="utf-8"))
    if not manifest["complete"] or quality["source_manifest_sha256"] != manifest_hash:
        raise ValueError("complete source manifest and matching ETL provenance required")
    days = [d for d, _ in daily_windows(manifest["start"], manifest["end"])]
    if len(days) < 84 or [r["day"] for r in manifest["days"]] != days:
        raise ValueError("at least 84 consecutive verified days required; sample data is insufficient")
    # Outcomes and snapshot status are deliberately absent from the feature input.
    raw = spark.read.parquet(str(root / cfg["input_parquet"]))
    observed = {str(r["created_day"]): int(r["count"])
                for r in raw.groupBy("created_day").count().collect()}
    if (set(observed) - set(days)
            or any(observed.get(r["day"], 0) != r["rows"] for r in manifest["days"])
            or sum(observed.values()) != quality["output_rows"]
            or raw.select("unique_key").distinct().count() != quality["output_rows"]):
        raise ValueError("cleaned counts/keys do not reconcile with the verified source")
    daily = raw.select("created_day", F.coalesce("borough", F.lit("UNKNOWN")).alias("borough"))
    if daily.filter(~F.col("borough").isin(BOROUGHS)).limit(1).count():
        raise ValueError("unexpected borough; update the documented geography contract first")
    counts = daily.groupBy("created_day", "borough").count()
    calendar = spark.sql(
        f"SELECT explode(sequence(DATE '{days[0]}', DATE '{days[-1]}', INTERVAL 1 DAY)) created_day"
    )
    boroughs = spark.sql("SELECT explode(array(" + ",".join(f"'{b}'" for b in BOROUGHS) + ")) borough")
    dense = (calendar.crossJoin(boroughs).join(counts, ["created_day", "borough"], "left")
             .fillna(0, subset=["count"]).withColumnRenamed("count", "count_0"))
    return dense, {"start": days[0], "end": days[-1], "days": len(days),
                   "source_manifest_sha256": manifest_hash, "rows": quality["output_rows"],
                   "snapshot_dates": quality["snapshot_dates"]}


def build_features(daily):
    """An origin is the end of its NYC wall-calendar day; all features stop there."""
    order = Window.partitionBy("borough").orderBy("created_day")
    base = daily
    for lag in range(1, WARMUP + 1):
        base = base.withColumn(f"lag_{lag}", F.lag("count_0", lag).over(order))
    for width in (7, 28):
        base = base.withColumn(f"mean_{width}", F.avg("count_0").over(order.rowsBetween(1 - width, 0)))
    base = base.filter(F.col("lag_28").isNotNull()).withColumnRenamed("created_day", "forecast_origin")
    horizons = daily.sparkSession.range(1, HORIZON + 1).select(F.col("id").cast("int").alias("horizon_days"))
    frame = base.crossJoin(horizons).withColumn("target_date", F.expr("date_add(forecast_origin, horizon_days)"))
    history = F.array("count_0", *[f"lag_{lag}" for lag in range(1, 29)])
    frame = frame.withColumn("seasonal_naive", F.element_at(history, 8 - F.col("horizon_days")))
    weekday_values = [F.element_at(history, offset - F.col("horizon_days")) for offset in (8, 15, 22, 29)]
    frame = frame.withColumn("weekday_mean", reduce(lambda a, b: a + b, weekday_values) / 4.0)
    angle = (F.dayofweek("target_date") - 1) * (2.0 * math.pi / 7.0)
    frame = frame.withColumn("weekday_sin", F.sin(angle)).withColumn("weekday_cos", F.cos(angle))
    labels = daily.select(F.col("created_day").alias("target_date"), "borough",
                          F.col("count_0").alias("actual_count"))
    return (frame.join(labels, ["target_date", "borough"], "left")
            .select("forecast_origin", "target_date", "borough", "actual_count", *NUMERIC)
            .withColumn("complaint_type", F.lit("ALL")))


def training_rows(features, cutoff):
    return features.filter((F.col("target_date") <= F.lit(cutoff).cast("date"))
                           & F.col("actual_count").isNotNull())


def evaluation_rows(features, cutoff, end):
    """Every forecast origin is at or after its model's last training label."""
    last_origin = (date.fromisoformat(end) - timedelta(days=HORIZON)).isoformat()
    return features.filter((F.col("forecast_origin") >= F.lit(cutoff).cast("date"))
                           & (F.col("forecast_origin") <= F.lit(last_origin).cast("date")))


def pipeline(name, seed):
    stages = [StringIndexer(inputCol="borough", outputCol="borough_id", handleInvalid="keep"),
              OneHotEncoder(inputCol="borough_id", outputCol="borough_vector", handleInvalid="keep"),
              VectorAssembler(inputCols=NUMERIC + ["borough_vector"], outputCol="assembled")]
    feature_col = "assembled"
    if name == "lr_scaled":
        stages.append(StandardScaler(inputCol="assembled", outputCol="features", withMean=False, withStd=True))
        feature_col = "features"
    common = {"featuresCol": feature_col, "labelCol": "label", "predictionCol": "raw_prediction"}
    if name.startswith("lr_"):
        estimator = LinearRegression(**common, regParam=0.1, elasticNetParam=0.0, maxIter=100,
                                     standardization=False)
    elif name == "random_forest":
        estimator = RandomForestRegressor(**common, numTrees=32, maxDepth=5, seed=seed)
    elif name == "gbt":
        estimator = GBTRegressor(**common, maxIter=30, maxDepth=3, seed=seed)
    else:
        raise ValueError(f"unknown MLlib model: {name}")
    return Pipeline(stages=stages + [estimator])


def predict(frame, name, model, cutoff, split):
    result = (frame.withColumn("raw_prediction", F.col(name).cast("double")) if model is None
              else model.transform(frame))
    if result.filter(F.col("raw_prediction").isNull() | F.isnan("raw_prediction")
                     | (F.abs("raw_prediction") == float("inf"))).limit(1).count():
        raise ValueError("null or nonfinite model output cannot become a forecast")
    return (result.withColumn("predicted_count", F.greatest(F.lit(0.0), "raw_prediction"))
            .withColumn("model", F.lit(name)).withColumn("split", F.lit(split))
            .withColumn("fit_cutoff", F.lit(cutoff).cast("date"))
            .withColumn("model_version", F.lit(f"demand-v1-{name}-{cutoff}"))
            .select("forecast_origin", "target_date", "borough", "complaint_type", "horizon_days",
                    "actual_count", "predicted_count", "raw_prediction", "model", "model_version", "fit_cutoff", "split"))


def error_metrics(predictions, groups):
    errors = predictions.withColumn("error", F.col("predicted_count") - F.col("actual_count"))
    return errors.groupBy(*groups).agg(
        F.count("error").alias("rows"), F.avg(F.abs("error")).alias("mae"),
        F.sqrt(F.avg(F.pow("error", 2))).alias("rmse"),
        (F.sum(F.abs("error")) / F.when(F.sum("actual_count") > 0, F.sum("actual_count"))).alias("wape"))


def write_features(spark, cfg, root):
    daily, coverage = verified_daily(spark, cfg, root)
    out = root / cfg["output_dir"]
    out.mkdir(parents=True, exist_ok=True)
    daily.write.mode("overwrite").parquet(str(out / "daily"))
    features = build_features(spark.read.parquet(str(out / "daily")))
    features.write.mode("overwrite").parquet(str(out / "features"))
    coverage.update(numeric_features=NUMERIC, horizon_days=HORIZON, warmup_days=WARMUP,
                    snapshot_semantics="acquisition dates, not forecast origins",
                    feature_source="created_day and borough counts only")
    save_json(out / "feature_manifest.json", coverage)
    return coverage


def train_and_forecast(spark, cfg, root):
    out = root / cfg["output_dir"]
    coverage = json.loads((out / "feature_manifest.json").read_text())
    if coverage["source_manifest_sha256"] != sha256_file(root / cfg["source_manifest"]):
        raise ValueError("source provenance changed; rebuild features")
    if not coverage["start"] < cfg["train_end"] < cfg["validation_end"] < coverage["end"]:
        raise ValueError("split dates must be inside the observed window")
    features = spark.read.parquet(str(out / "features"))
    metadata, predictions = [], []
    validation_scores = None
    for split, cutoff, end in (("validation", cfg["train_end"], cfg["validation_end"]),
                                ("test", cfg["validation_end"], coverage["end"])):
        train = training_rows(features, cutoff).withColumn("label", F.col("actual_count").cast("double")).cache()
        evaluate = evaluation_rows(features, cutoff, end).cache()
        if train.count() < 100 or evaluate.count() == 0:
            raise ValueError("insufficient chronological training/evaluation rows")
        for name in MODELS:
            print(f"[demand] {split}: {name}; fit labels through {cutoff}", flush=True)
            started = time.perf_counter()
            model = None if name in ("seasonal_naive", "weekday_mean") else pipeline(name, cfg["seed"]).fit(train)
            fit_seconds = time.perf_counter() - started
            result = predict(evaluate, name, model, cutoff, split).localCheckpoint(eager=True)
            predictions.append(result)
            metadata.append({"model": name, "split": split, "fit_cutoff": cutoff,
                             "fit_seconds": fit_seconds, "training_rows": train.count(),
                             "evaluation_rows": result.count(), "seed": cfg["seed"]})
        train.unpersist()
        evaluate.unpersist()
        if split == "validation":
            val = reduce(lambda a, b: a.unionByName(b), predictions)
            validation_scores = [r.asDict() for r in error_metrics(val, ["model", "split"]).collect()]
    heldout = reduce(lambda a, b: a.unionByName(b), predictions)
    if heldout.filter(F.col("fit_cutoff") > F.col("forecast_origin")).count():
        raise ValueError("training labels extend beyond a forecast origin")
    heldout.write.mode("overwrite").parquet(str(out / "predictions"))
    heldout = spark.read.parquet(str(out / "predictions"))
    for name, groups in (("metrics", ["model", "split"]), ("metrics_borough", ["model", "split", "borough"]),
                          ("metrics_horizon", ["model", "split", "horizon_days"]),
                          ("metrics_origins", ["model", "split", "forecast_origin"])):
        error_metrics(heldout, groups).write.mode("overwrite").parquet(str(out / name))
    selected = min(validation_scores, key=lambda r: (r["mae"], r["model"]))["model"]
    best_ml = min((r for r in validation_scores if r["model"] not in MODELS[:2]),
                  key=lambda r: (r["mae"], r["model"]))["model"]
    full_train = training_rows(features, coverage["end"]).withColumn("label", F.col("actual_count").cast("double")).cache()
    future_input = features.filter(F.col("forecast_origin") == F.lit(coverage["end"]).cast("date"))
    final_models = {}
    final_training_rows = full_train.count()
    for name in sorted({selected, best_ml} - set(MODELS[:2])):
        final_models[name] = pipeline(name, cfg["seed"]).fit(full_train)
        final_models[name].write().overwrite().save(str(out / "models" / name))
    future = predict(future_input, selected, final_models.get(selected), coverage["end"], "future")
    future.write.mode("overwrite").parquet(str(out / "future"))
    full_train.unpersist()
    summary = {"coverage": coverage, "selected_model": selected, "best_validation_mllib": best_ml,
               "selection_rule": "lowest validation MAE; test scores never select the model",
               "validation_scores": validation_scores, "fits": metadata,
               "seed": cfg["seed"], "producer_sha256": sha256_file(Path(__file__)),
               "final_fit_cutoff": coverage["end"], "final_training_rows": final_training_rows,
               "retained_models": {name: str(out / "models" / name) for name in final_models},
               "overall_metrics": [r.asDict() for r in error_metrics(heldout, ["model", "split"]).collect()],
               "limitations": "Retrospective winter-only experiment; assumes counts available at each day end; no annual accuracy claim."}
    save_json(out / "model_manifest.json", summary)
    return summary
