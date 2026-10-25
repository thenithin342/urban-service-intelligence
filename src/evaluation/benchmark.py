"""Equivalent Pandas/Spark CSV read-filter-aggregate-write experiments."""

import csv
import json
import os
import platform
import statistics
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from pyspark.sql import functions as F

from src.ingestion.downloader import DEFAULT_FIELDS, sha256_file
from src.ingestion.history import save_json
from src.processing.clean import read_raw


def prepare_inputs(cfg, root):
    source = root / cfg["source_dir"]
    manifest = json.loads((source / "manifest.json").read_text())
    scales = sorted({int(n) for n in cfg["scales"]})
    if not manifest["complete"] or not scales or scales[0] <= 0 or scales[-1] > manifest["rows_written"]:
        raise ValueError("positive benchmark scales must fit the complete real source")
    out = root / cfg["output_dir"] / "inputs"
    out.mkdir(parents=True, exist_ok=True)
    # One sequential pass per scale keeps memory independent of record count.
    result = {}
    for n in scales:
        path = out / f"rows_{n}.csv"
        written = 0
        with path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=DEFAULT_FIELDS)
            writer.writeheader()
            for day in manifest["days"]:
                original = source / f"{day['day']}.csv"
                if sha256_file(original) != day["sha256"]:
                    raise ValueError("benchmark source checksum mismatch")
                with original.open(encoding="utf-8", newline="") as raw:
                    for row in csv.DictReader(raw):
                        writer.writerow(row)
                        written += 1
                        if written == n:
                            break
                if written == n:
                    break
        if written != n:
            raise ValueError("benchmark input was truncated")
        result[n] = {"path": str(path), "rows": n, "bytes": path.stat().st_size,
                     "sha256": sha256_file(path)}
    return result


def pandas_task(path, output):
    frame = pd.read_csv(path, dtype=str, keep_default_na=False,
                        usecols=["borough", "complaint_type", "latitude", "longitude"])
    lat = pd.to_numeric(frame["latitude"], errors="coerce")
    lon = pd.to_numeric(frame["longitude"], errors="coerce")
    valid = frame.loc[lat.between(40.49, 40.92) & lon.between(-74.26, -73.70)].copy()
    for name in ("borough", "complaint_type"):
        valid[name] = valid[name].str.replace("\r\n", "\n", regex=False).str.replace("\r", "\n", regex=False).str.strip(" ")
    valid["borough"] = valid["borough"].str.upper().replace({"": "UNKNOWN", "UNSPECIFIED": "UNKNOWN"})
    valid["complaint_type"] = valid["complaint_type"].replace("", "UNKNOWN")
    grouped = valid.groupby(["borough", "complaint_type"]).size().rename("complaint_count").reset_index()
    grouped.sort_values(["borough", "complaint_type"]).to_csv(output, index=False, lineterminator="\n")


def spark_task(spark, path, output, partitions):
    frame = read_raw(spark, str(path)).repartition(partitions)
    lat, lon = F.col("latitude").cast("double"), F.col("longitude").cast("double")
    valid = frame.filter(lat.between(40.49, 40.92) & lon.between(-74.26, -73.70))
    borough = F.upper(F.trim(F.regexp_replace("borough", r"\r\n?", "\n")))
    category = F.trim(F.regexp_replace("complaint_type", r"\r\n?", "\n"))
    valid = valid.select(
        F.when(borough.isNull() | borough.isin("", "UNSPECIFIED"), "UNKNOWN").otherwise(borough).alias("borough"),
        F.when(category.isNull() | (category == ""), "UNKNOWN").otherwise(category).alias("complaint_type"))
    grouped = valid.groupBy("borough", "complaint_type").count().withColumnRenamed("count", "complaint_count")
    (grouped.orderBy("borough", "complaint_type").coalesce(1).write.mode("overwrite")
     .option("header", "true").option("escape", '"').option("lineSep", "\n").csv(str(output)))


def read_counts(path):
    path = Path(path)
    files = sorted(path.glob("part-*.csv")) if path.is_dir() else [path]
    values = {}
    for item in files:
        with item.open(encoding="utf-8", newline="") as stream:
            for row in csv.DictReader(stream):
                key = (row["borough"], row["complaint_type"])
                if key in values:
                    raise ValueError("duplicate benchmark output group")
                values[key] = int(row["complaint_count"])
    return values


def run_benchmark(spark, cfg, root):
    repetitions = int(cfg["repetitions"])
    partitions = sorted({int(p) for p in cfg["partitions"]})
    if repetitions < 3 or not partitions or partitions[0] < 1:
        raise ValueError("at least three repetitions and positive partition counts required")
    inputs = prepare_inputs(cfg, root)
    source_meta = json.loads((root / cfg["source_dir"] / "manifest.json").read_text())
    out = root / cfg["output_dir"]
    trials, reference = [], {}
    base_partitions = 2
    old_shuffle = spark.conf.get("spark.sql.shuffle.partitions")
    try:
        for n, info in inputs.items():
            for engine in ("pandas", "spark"):
                settings = [base_partitions] if engine == "spark" else [0]
                if engine == "spark" and n == max(inputs):
                    settings = sorted(set(partitions + [base_partitions]))
                for p in settings:
                    if p:
                        spark.conf.set("spark.sql.shuffle.partitions", str(p))
                    for repeat in range(-1, repetitions):
                        output = out / f"{engine}_{n}_p{p}_r{repeat}"
                        started = time.perf_counter()
                        if engine == "pandas":
                            output = output.with_suffix(".csv")
                            pandas_task(info["path"], output)
                        else:
                            spark_task(spark, info["path"], output, p)
                        elapsed = time.perf_counter() - started
                        counts = read_counts(output)
                        if n not in reference:
                            reference[n] = counts
                        if counts != reference[n]:
                            raise ValueError("Pandas/Spark benchmark results differ")
                        if repeat >= 0:
                            trials.append({"engine": engine, "rows": n, "partitions": p,
                                           "repeat": repeat, "seconds": elapsed,
                                           "rows_per_second": n / elapsed,
                                           "valid_coordinate_rows": sum(counts.values())})
        groups = sorted({(r["engine"], r["rows"], r["partitions"]) for r in trials})
        summaries = []
        for engine, n, p in groups:
            times = [r["seconds"] for r in trials if (r["engine"], r["rows"], r["partitions"]) == (engine, n, p)]
            summaries.append({"engine": engine, "rows": n, "partitions": p,
                              "median_seconds": statistics.median(times), "min_seconds": min(times),
                              "max_seconds": max(times), "stdev_seconds": statistics.stdev(times)})
        report = {"inputs": inputs, "trials": trials, "summary": summaries,
                  "source_window": {"start": source_meta["start"],
                                    "end": (date.fromisoformat(source_meta["end"]) - timedelta(days=1)).isoformat()},
                  "hardware": {"platform": platform.platform(), "processor": platform.processor(),
                               "logical_cpus": os.cpu_count(), "python": sys.version, "spark": spark.version},
                  "cache": "One untimed warmup per setting; OS cache uncontrolled; timed runs are warm",
                  "memory": "No comparable Pandas/native/JVM peak RSS measurement; no memory ranking claimed",
                  "source": "Real chronological CSV prefixes, no synthetic expansion",
                  "reading": "Multiline CSV is one file per scale and is unsplittable; partition sensitivity adds redistribution after reading",
                  "producer_sha256": sha256_file(Path(__file__))}
        save_json(out / "benchmark.json", report)
        plot_benchmark(report, out)
        return report
    finally:
        spark.conf.set("spark.sql.shuffle.partitions", old_shuffle)


def plot_benchmark(report, out):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 4.5))
    for engine, p in (("pandas", 0), ("spark", 2)):
        rows = sorted([r for r in report["summary"] if r["engine"] == engine and r["partitions"] == p], key=lambda r: r["rows"])
        ax.plot([r["rows"] for r in rows], [r["median_seconds"] for r in rows], marker="o", label=engine)
    ax.set_xlabel("Real input records")
    ax.set_ylabel("Median read/filter/group/write time (seconds)")
    period = report["source_window"]
    ax.set_title(f"Real prefixes from {period['start']}..{period['end']}; warm medians")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out / "runtime_by_scale.png", dpi=160)
    plt.close(fig)
    largest = max(r["rows"] for r in report["summary"])
    rows = [r for r in report["summary"] if r["engine"] == "spark" and r["rows"] == largest]
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.plot([r["partitions"] for r in rows], [r["median_seconds"] for r in rows], marker="o")
    ax.set_xlabel("Spark redistribution and shuffle partitions")
    ax.set_ylabel("Median pipeline time (seconds)")
    ax.set_title(f"{largest:,} records, {period['start']}..{period['end']}: partition sensitivity")
    fig.tight_layout()
    fig.savefig(out / "partition_sensitivity.png", dpi=160)
    plt.close(fig)
