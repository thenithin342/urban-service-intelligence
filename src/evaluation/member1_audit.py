"""Independent bounded checks of forecasting exports, saved MLlib models and benchmarks."""

import json
import math
import statistics
from collections import defaultdict
from datetime import timedelta
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.sql import functions as F

from src.evaluation.benchmark import read_counts
from src.ingestion.downloader import sha256_file
from src.ingestion.history import save_json


def independent_metrics(rows):
    errors = [float(r["predicted_count"]) - float(r["actual_count"]) for r in rows]
    actual = sum(r["actual_count"] for r in rows)
    return {"rows": len(rows), "mae": sum(abs(e) for e in errors) / len(rows),
            "rmse": math.sqrt(sum(e * e for e in errors) / len(rows)),
            "wape": sum(abs(e) for e in errors) / actual if actual else None}


def check_prediction_rows(rows, daily):
    seen = set()
    for row in rows:
        key = tuple(row[c] for c in ("model", "split", "forecast_origin", "borough", "horizon_days"))
        if key in seen:
            raise ValueError("duplicate prediction key")
        seen.add(key)
        if (row["fit_cutoff"] > row["forecast_origin"]
                or (row["target_date"] - row["forecast_origin"]).days != row["horizon_days"]
                or not 1 <= row["horizon_days"] <= 7
                or not math.isfinite(row["predicted_count"]) or row["predicted_count"] < 0):
            raise ValueError("invalid time boundary or prediction")
        key = (row["target_date"], row["borough"])
        if row["actual_count"] != daily[key]:
            raise ValueError("prediction label differs from verified daily count")
        if row["model"] in ("seasonal_naive", "weekday_mean"):
            offsets = (7,) if row["model"] == "seasonal_naive" else (7, 14, 21, 28)
            expected = sum(daily[(row["target_date"] - timedelta(days=d), row["borough"])] for d in offsets) / len(offsets)
            if not math.isclose(row["predicted_count"], expected, abs_tol=1e-9):
                raise ValueError("baseline prediction differs from independent lookup")


def bounded_rows(frame, cap=20000):
    rows = frame.limit(cap + 1).collect()
    if len(rows) > cap:
        raise ValueError("audit collection bound exceeded; aggregate or narrow the evaluation")
    return [r.asDict() for r in rows]


def audit_member1(spark, cfg, benchmark_cfg, root):
    out = root / cfg["output_dir"]
    meta = json.loads((out / "model_manifest.json").read_text())
    if meta["seed"] != cfg["seed"]:
        raise ValueError("configuration seed changed since training")
    if meta["producer_sha256"] != sha256_file(Path(__file__).parents[1] / "modeling/demand.py"):
        raise ValueError("demand producer changed since training; rerun the experiment")
    if meta["coverage"]["source_manifest_sha256"] != sha256_file(root / cfg["source_manifest"]):
        raise ValueError("audit source hash mismatch")
    daily = {(r["created_day"], r["borough"]): r["count_0"]
             for r in bounded_rows(spark.read.parquet(str(out / "daily")))}
    predictions = bounded_rows(spark.read.parquet(str(out / "predictions")))
    check_prediction_rows(predictions, daily)
    cutoffs = {"validation": cfg["train_end"], "test": cfg["validation_end"]}
    ends = {"validation": cfg["validation_end"], "test": meta["coverage"]["end"]}
    for row in predictions:
        if (str(row["fit_cutoff"]) != cutoffs.get(row["split"])
                or str(row["target_date"]) > ends[row["split"]]):
            raise ValueError("prediction splits differ from the requested chronological configuration")
    for table, dimensions in (("metrics", ["model", "split"]),
                               ("metrics_borough", ["model", "split", "borough"]),
                               ("metrics_horizon", ["model", "split", "horizon_days"]),
                               ("metrics_origins", ["model", "split", "forecast_origin"])):
        groups = defaultdict(list)
        for row in predictions:
            groups[tuple(row[c] for c in dimensions)].append(row)
        stored_rows = bounded_rows(spark.read.parquet(str(out / table)))
        if (len(stored_rows) != len(groups)
                or {tuple(r[c] for c in dimensions) for r in stored_rows} != set(groups)):
            raise ValueError(f"metric groups missing or duplicated: {table}")
        if table == "metrics":
            manifest_rows = meta["overall_metrics"]
            if (len(manifest_rows) != len(groups)
                    or {tuple(r[c] for c in dimensions) for r in manifest_rows} != set(groups)):
                raise ValueError("model manifest metric groups differ from predictions")
            stored_rows += manifest_rows
        for stored in stored_rows:
            expected = independent_metrics(groups[tuple(stored[c] for c in dimensions)])
            for field, value in expected.items():
                actual = stored[field]
                if (value is None and actual is not None) or (value is not None and
                        (actual is None or not math.isclose(actual, value, rel_tol=1e-9, abs_tol=1e-8))):
                    raise ValueError(f"independent metric mismatch: {table}/{field}")
    validation = [r for r in meta["overall_metrics"] if r["split"] == "validation"]
    if meta["selected_model"] != min(validation, key=lambda r: (r["mae"], r["model"]))["model"]:
        raise ValueError("model was not selected by validation MAE")
    future = bounded_rows(spark.read.parquet(str(out / "future")))
    if len(future) != 7 * len({key[1] for key in daily}):
        raise ValueError("future forecast is missing geography/horizon rows")
    future_keys = set()
    for row in future:
        key = (row["borough"], row["horizon_days"])
        if (key in future_keys or row["actual_count"] is not None
                or row["model"] != meta["selected_model"]
                or not 1 <= row["horizon_days"] <= 7
                or row["fit_cutoff"] != row["forecast_origin"]
                or not math.isfinite(row["predicted_count"]) or row["predicted_count"] < 0
                or str(row["forecast_origin"]) != meta["coverage"]["end"]
                or (row["target_date"] - row["forecast_origin"]).days != row["horizon_days"]):
            raise ValueError("future forecast contract failed")
        future_keys.add(key)
    model_checks = {}
    features = spark.read.parquet(str(out / "features"))
    for name, path in meta["retained_models"].items():
        model = PipelineModel.load(path)
        check = model.transform(features.filter(F.col("forecast_origin") == F.lit(meta["coverage"]["end"]).cast("date")))
        if (check.count() != len(future) or check.filter(F.isnan("raw_prediction")
                | F.col("raw_prediction").isNull()).limit(1).count()):
            raise ValueError("retained Spark PipelineModel could not reproduce valid predictions")
        model_checks[name] = [type(stage).__name__ for stage in model.stages]
    handoff = bounded_rows(spark.read.parquet(str(out / "priority_features")))
    if (len(handoff) != len({p["borough"] for p in future})
            or {r["borough"] for r in handoff} != {p["borough"] for p in future}):
        raise ValueError("handoff borough coverage differs from forecast coverage")
    quality = json.loads((root / cfg["quality_report"]).read_text())
    if sum(r["valid_geo_count"] for r in handoff) != quality["output_rows"] - quality["invalid_coords"]:
        raise ValueError("handoff spatial counts do not reconcile with ETL")
    for row in handoff:
        expected = sum(p["predicted_count"] for p in future if p["borough"] == row["borough"])
        if not math.isclose(row["forecast_7d"], expected, abs_tol=1e-8):
            raise ValueError("handoff and forecast totals differ")
    benchmark_path = root / benchmark_cfg["output_dir"] / "benchmark.json"
    bench = json.loads(benchmark_path.read_text())
    for info in bench["inputs"].values():
        if sha256_file(info["path"]) != info["sha256"]:
            raise ValueError("benchmark input checksum changed")
    references = {}
    for trial in bench["trials"]:
        path = benchmark_path.parent / f"{trial['engine']}_{trial['rows']}_p{trial['partitions']}_r{trial['repeat']}"
        if trial["engine"] == "pandas":
            path = path.with_suffix(".csv")
        counts = read_counts(path)
        if trial["rows"] not in references:
            references[trial["rows"]] = counts
        if counts != references[trial["rows"]]:
            raise ValueError("saved benchmark engine outputs differ")
    for group in bench["summary"]:
        samples = [r["seconds"] for r in bench["trials"] if all(r[k] == group[k] for k in ("engine", "rows", "partitions"))]
        if len(samples) < 3 or any(not math.isfinite(t) or t <= 0 for t in samples):
            raise ValueError("invalid benchmark trials")
        if not math.isclose(statistics.median(samples), group["median_seconds"], abs_tol=1e-9):
            raise ValueError("benchmark median mismatch")
    summary = {"predictions_checked": len(predictions), "future_rows": len(future),
               "handoff_rows": len(handoff), "retained_models_loaded": model_checks,
               "metric_tables_reconciled": 4, "benchmark_trials_checked": len(bench["trials"]),
               "selected_model": meta["selected_model"], "status": "PASS"}
    save_json(out / "final_audit.json", summary)
    return summary
