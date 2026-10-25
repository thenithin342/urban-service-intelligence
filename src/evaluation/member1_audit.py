"""Independent bounded checks of forecasting exports, saved MLlib models and benchmarks."""

import json
import math
import statistics
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.sql import functions as F

from src.evaluation.benchmark import read_counts
from src.ingestion.downloader import sha256_file
from src.ingestion.history import save_json
from src.modeling.demand import MODELS, category_policy


def independent_metrics(rows):
    errors = [float(r["predicted_count"]) - float(r["actual_count"]) for r in rows]
    actual = sum(r["actual_count"] for r in rows)
    return {"rows": len(rows), "mae": sum(abs(e) for e in errors) / len(rows),
            "rmse": math.sqrt(sum(e * e for e in errors) / len(rows)),
            "wape": sum(abs(e) for e in errors) / actual if actual else None}


def check_prediction_rows(rows, daily, expected_keys=None):
    daily = {key if len(key) == 3 else (*key, "ALL"): value for key, value in daily.items()}
    seen = set()
    for row in rows:
        category = row.get("complaint_type", "ALL")
        key = (row["model"], row["split"], row["forecast_origin"], row["borough"], category, row["horizon_days"])
        if key in seen:
            raise ValueError("duplicate prediction key")
        seen.add(key)
        if (row["fit_cutoff"] > row["forecast_origin"]
                or (row["target_date"] - row["forecast_origin"]).days != row["horizon_days"]
                or not 1 <= row["horizon_days"] <= 7
                or not math.isfinite(row["predicted_count"]) or row["predicted_count"] < 0):
            raise ValueError("invalid time boundary or prediction")
        key = (row["target_date"], row["borough"], category)
        if row["actual_count"] != daily[key]:
            raise ValueError("prediction label differs from verified daily count")
        if row["model"] in ("seasonal_naive", "weekday_mean"):
            offsets = (7,) if row["model"] == "seasonal_naive" else (7, 14, 21, 28)
            expected = sum(daily[(row["target_date"] - timedelta(days=d), row["borough"], category)] for d in offsets) / len(offsets)
            if not math.isclose(row["predicted_count"], expected, abs_tol=1e-9):
                raise ValueError("baseline prediction differs from independent lookup")
    if expected_keys is not None and seen != expected_keys:
        raise ValueError("held-out model/origin/geography/horizon coverage is incomplete")


def check_future_rows(future, daily, selected, end):
    daily = {key if len(key) == 3 else (*key, "ALL"): value for key, value in daily.items()}
    origin = date.fromisoformat(end)
    expected = {(borough, category, h) for _, borough, category in daily for h in range(1, 8)}
    keys = [(row["borough"], row.get("complaint_type", "ALL"), row["horizon_days"]) for row in future]
    if len(keys) != len(expected) or set(keys) != expected:
        raise ValueError("future forecast is missing geography/horizon rows")
    for row in future:
        if (row["actual_count"] is not None or row["model"] != selected
                or row["fit_cutoff"] != origin or row["forecast_origin"] != origin
                or row["target_date"] != origin + timedelta(days=row["horizon_days"])
                or not math.isfinite(row["predicted_count"]) or row["predicted_count"] < 0):
            raise ValueError("future forecast contract failed")
        if selected in MODELS[:2]:
            offsets = (7,) if selected == "seasonal_naive" else (7, 14, 21, 28)
            value = sum(daily[(row["target_date"] - timedelta(days=d), row["borough"], row.get("complaint_type", "ALL"))]
                        for d in offsets) / len(offsets)
            if not math.isclose(row["predicted_count"], value, abs_tol=1e-9):
                raise ValueError("future baseline differs from independent lookup")


def check_benchmark_coverage(bench, cfg):
    scales = {int(n) for n in cfg["scales"]}
    settings = {(engine, n, p) for n in scales for engine, partitions in
                (("pandas", [0]), ("spark", sorted({2, *map(int, cfg["partitions"])}) if n == max(scales) else [2]))
                for p in partitions}
    expected = {(*setting, repeat) for setting in settings for repeat in range(int(cfg["repetitions"]))}
    keys = [(r["engine"], r["rows"], r["partitions"], r["repeat"]) for r in bench["trials"]]
    groups = [(r["engine"], r["rows"], r["partitions"]) for r in bench["summary"]]
    if (len(keys) != len(expected) or set(keys) != expected
            or len(groups) != len(settings) or set(groups) != settings
            or {int(n) for n in bench["inputs"]} != scales):
        raise ValueError("benchmark trials, summaries or input scales are incomplete")


def bounded_rows(frame, cap=50000):
    rows = frame.limit(cap + 1).collect()
    if len(rows) > cap:
        raise ValueError("audit collection bound exceeded; aggregate or narrow the evaluation")
    return [r.asDict() for r in rows]


def audit_member1(spark, cfg, benchmark_cfg, root):
    out = root / cfg["output_dir"]
    (out / "final_audit.json").unlink(missing_ok=True)
    meta = json.loads((out / "model_manifest.json").read_text())
    if meta["seed"] != cfg["seed"]:
        raise ValueError("configuration seed changed since training")
    if meta["producer_sha256"] != sha256_file(Path(__file__).parents[1] / "modeling/demand.py"):
        raise ValueError("demand producer changed since training; rerun the experiment")
    if (meta["coverage"].get("category_policy") != category_policy(cfg)
            or meta["coverage"].get("feature_producer_sha256") != meta["producer_sha256"]):
        raise ValueError("feature producer or category policy differs from the audited experiment")
    if meta["coverage"]["source_manifest_sha256"] != sha256_file(root / cfg["source_manifest"]):
        raise ValueError("audit source hash mismatch")
    daily = {(r["created_day"], r["borough"], r.get("complaint_type", "ALL")): r["count_0"]
             for r in bounded_rows(spark.read.parquet(str(out / "daily")))}
    support = {(r["borough"], r["complaint_type"]): r for r in meta["coverage"].get("category_support", [])}
    for borough, category in {(b, c) for _, b, c in daily if c != "ALL"}:
        counts = [n for (day, b, c), n in daily.items()
                  if (b, c) == (borough, category) and str(day) <= cfg["train_end"]]
        recorded = support.get((borough, category), {})
        if (not recorded.get("eligible") or sum(counts) != recorded["training_count"]
                or sum(n > 0 for n in counts) != recorded["nonzero_training_days"]):
            raise ValueError("category support differs from training-only daily counts")
    predictions = bounded_rows(spark.read.parquet(str(out / "predictions")))
    cutoffs = {"validation": cfg["train_end"], "test": cfg["validation_end"]}
    ends = {"validation": cfg["validation_end"], "test": meta["coverage"]["end"]}
    expected = {(model, split, origin, borough, category, h)
                for split, cutoff in cutoffs.items() for origin, borough, category in daily
                if date.fromisoformat(cutoff) <= origin <= date.fromisoformat(ends[split]) - timedelta(days=7)
                and (origin - date.fromisoformat(cutoff)).days % cfg.get("origin_step_days", 1) == 0
                for model in MODELS for h in range(1, 8)}
    check_prediction_rows(predictions, daily, expected)
    for row in predictions:
        if (str(row["fit_cutoff"]) != cutoffs.get(row["split"])
                or str(row["target_date"]) > ends[row["split"]]):
            raise ValueError("prediction splits differ from the requested chronological configuration")
    for table, dimensions in (("metrics", ["model", "split"]),
                               ("metrics_borough", ["model", "split", "borough", "complaint_type"]),
                               ("metrics_category", ["model", "split", "complaint_type"]),
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
    check_future_rows(future, daily, meta["selected_model"], meta["coverage"]["end"])
    best_ml = min((r for r in validation if r["model"] not in MODELS[:2]),
                  key=lambda r: (r["mae"], r["model"]))["model"]
    required = {meta["selected_model"], best_ml} - set(MODELS[:2])
    if set(meta["retained_models"]) != required or meta["best_validation_mllib"] != best_ml:
        raise ValueError("required retained MLlib models are missing or differ from validation selection")
    model_checks = {}
    features = spark.read.parquet(str(out / "features"))
    for name, path in meta["retained_models"].items():
        model = PipelineModel.load(path)
        check = model.transform(features.filter(F.col("forecast_origin") == F.lit(meta["coverage"]["end"]).cast("date")))
        if (check.count() != len(future) or check.filter(F.isnan("raw_prediction")
                | F.col("raw_prediction").isNull() | (F.abs("raw_prediction") == float("inf"))).limit(1).count()):
            raise ValueError("retained Spark PipelineModel could not reproduce valid predictions")
        model_checks[name] = [type(stage).__name__ for stage in model.stages]
    handoff = bounded_rows(spark.read.parquet(str(out / "priority_features")))
    series = {(p["borough"], p["complaint_type"]) for p in future}
    if (len(handoff) != len(series)
            or {(r["borough"], r["complaint_type"]) for r in handoff} != series):
        raise ValueError("handoff borough coverage differs from forecast coverage")
    quality = json.loads((root / cfg["quality_report"]).read_text())
    if sum(r["valid_geo_count"] for r in handoff if r["complaint_type"] == "ALL") != quality["output_rows"] - quality["invalid_coords"]:
        raise ValueError("handoff spatial counts do not reconcile with ETL")
    for row in handoff:
        expected = sum(p["predicted_count"] for p in future
                       if (p["borough"], p["complaint_type"]) == (row["borough"], row["complaint_type"]))
        if not math.isclose(row["forecast_7d"], expected, abs_tol=1e-8):
            raise ValueError("handoff and forecast totals differ")
        trailing = sum(n for (day, b, c), n in daily.items()
                       if (b, c) == (row["borough"], row["complaint_type"])
                       and date.fromisoformat(meta["coverage"]["end"]) - timedelta(days=7) < day <= date.fromisoformat(meta["coverage"]["end"]))
        if row["trailing_7d"] != trailing or not math.isclose(row["absolute_change"], expected - trailing, abs_tol=1e-8):
            raise ValueError("handoff baseline/change differs from observed series counts")
    benchmark_path = root / benchmark_cfg["output_dir"] / "benchmark.json"
    bench = json.loads(benchmark_path.read_text())
    check_benchmark_coverage(bench, benchmark_cfg)
    for info in bench["inputs"].values():
        if sha256_file(info["path"]) != info["sha256"]:
            raise ValueError("benchmark input checksum changed")
    references = {}
    for trial in bench["trials"]:
        path = benchmark_path.parent / f"{trial['engine']}_{trial['rows']}_p{trial['partitions']}_r{trial['repeat']}"
        if trial["engine"] == "pandas":
            path = path.with_suffix(".csv")
        counts = read_counts(path)
        if sum(counts.values()) != trial["valid_coordinate_rows"]:
            raise ValueError("benchmark result row totals differ from the measured trial")
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
               "metric_tables_reconciled": 5, "benchmark_trials_checked": len(bench["trials"]),
               "selected_model": meta["selected_model"], "status": "PASS"}
    save_json(out / "final_audit.json", summary)
    return summary
