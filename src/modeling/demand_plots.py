"""Small report figures from exported demand aggregates and predictions."""

import json
from datetime import date, timedelta

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from pyspark.sql import functions as F

from src.analytics.spark_eda import bounded_pandas


def write_plots(spark, out, quality=None):
    meta = json.loads((out / "model_manifest.json").read_text())
    test_cutoff = next(r["fit_cutoff"] for r in meta["fits"] if r["split"] == "test")
    test_start = date.fromisoformat(test_cutoff) + timedelta(days=1)
    metrics = bounded_pandas(spark.read.parquet(str(out / "metrics")), 1000)
    test = metrics[metrics["split"] == "test"].sort_values("mae")
    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.bar(test["model"], test["mae"])
    ax.set_ylabel("MAE (complaints per modeled series-day)")
    ax.set_title(f"Retrospective test {test_start}..{meta['coverage']['end']}; horizons 1-7 days")
    ax.tick_params(axis="x", rotation=25)
    fig.tight_layout()
    fig.savefig(out / "model_comparison.png", dpi=160)
    plt.close(fig)
    daily = spark.read.parquet(str(out / "daily"))
    end = daily.agg(F.max("created_day")).first()[0]
    city = bounded_pandas(daily.filter(F.col("complaint_type") == "ALL").groupBy("created_day")
                          .sum("count_0").orderBy("created_day"), 5000)
    fig, ax = plt.subplots(figsize=(11, 4))
    ax.plot(city["created_day"], city["sum(count_0)"])
    ax.set_ylabel("Reported complaints/day (ALL boroughs)")
    ax.set_title(f"Verified source coverage {meta['coverage']['start']}..{end}; snapshot {meta['coverage']['snapshot_dates']}")
    for split in ("validation", "test"):
        cutoff = next(r["fit_cutoff"] for r in meta["fits"] if r["split"] == split)
        ax.axvline(date.fromisoformat(cutoff), linestyle="--", label=f"{split} fit cutoff")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out / "coverage_daily.png", dpi=160)
    plt.close(fig)
    if quality:
        flags = sorted(quality["flag_breakdown"].items())
        fig, ax = plt.subplots(figsize=(10, 4.5))
        ax.bar([r[0] for r in flags], [r[1] for r in flags])
        ax.set_ylabel("Flag occurrences (conditions overlap)")
        ax.set_title(f"Data quality {meta['coverage']['start']}..{end}; {quality['output_rows']:,} cleaned records")
        ax.tick_params(axis="x", rotation=25)
        fig.tight_layout()
        fig.savefig(out / "data_quality.png", dpi=160)
        plt.close(fig)
    recent = daily.filter(F.col("created_day") > F.date_sub(F.lit(end), 28))
    future_frame = spark.read.parquet(str(out / "future"))
    observed = bounded_pandas(recent.filter(F.col("complaint_type") == "ALL"), 1000)
    future = bounded_pandas(future_frame.filter(F.col("complaint_type") == "ALL"), 1000)
    fig, axes = plt.subplots(2, 3, figsize=(13, 7))
    for ax, borough in zip(axes.flat, sorted(future["borough"].unique())):
        before = observed[observed["borough"] == borough].sort_values("created_day")
        after = future[future["borough"] == borough].sort_values("target_date")
        ax.plot(before["created_day"], before["count_0"], label="Observed")
        ax.plot(after["target_date"], after["predicted_count"], marker="o", label="Forecast")
        ax.set_title(borough)
        ax.set_ylabel("Reported complaints/day")
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=7))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
        ax.tick_params(axis="x", rotation=30)
    axes.flat[0].legend()
    fig.suptitle(f"Observed {observed['created_day'].min()}..{end}; next 7 days from historical origin (not today)")
    fig.tight_layout()
    fig.savefig(out / "forecast_7d.png", dpi=160)
    plt.close(fig)
    categories = [r[0] for r in daily.select("complaint_type").distinct().orderBy("complaint_type").collect() if r[0] != "ALL"]
    if categories:
        category_metrics = bounded_pandas(spark.read.parquet(str(out / "metrics_category")), 1000)
        rows = (len(categories) + 2) // 2
        fig, axes = plt.subplots(rows, 2, figsize=(12, 4 * rows), squeeze=False)
        for ax, category in zip(axes.flat, ["ALL", *categories]):
            values = category_metrics[(category_metrics["split"] == "test")
                                      & (category_metrics["complaint_type"] == category)].sort_values("mae")
            ax.bar(values["model"], values["mae"])
            ax.set_title(category)
            ax.set_ylabel("MAE (complaints per borough-day)")
            ax.tick_params(axis="x", rotation=30)
        for ax in axes.flat[len(categories) + 1:]:
            ax.set_visible(False)
        fig.suptitle(f"Retrospective category test {test_start}..{end}; eligible boroughs; horizons 1-7")
        fig.tight_layout()
        fig.savefig(out / "category_model_comparison.png", dpi=160)
        plt.close(fig)
        counts = bounded_pandas(recent.groupBy("created_day", "complaint_type").sum("count_0"), 1000)
        predictions = bounded_pandas(future_frame.groupBy("target_date", "complaint_type").sum("predicted_count"), 1000)
        fig, axes = plt.subplots(len(categories), 1, figsize=(10, 3 * len(categories)), squeeze=False)
        for ax, category in zip(axes.flat, categories):
            before = counts[counts["complaint_type"] == category].sort_values("created_day")
            after = predictions[predictions["complaint_type"] == category].sort_values("target_date")
            ax.plot(before["created_day"], before["sum(count_0)"], label="Observed")
            ax.plot(after["target_date"], after["sum(predicted_count)"], marker="o", label="Forecast")
            ax.set_title(category + " (eligible boroughs only)")
            ax.set_ylabel("Reported complaints/day")
            ax.xaxis.set_major_locator(mdates.DayLocator(interval=7))
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
        axes.flat[0].legend()
        fig.suptitle(f"Historical category forecasts from {end}; observed prior 28 days; next 7 days")
        fig.tight_layout()
        fig.savefig(out / "category_forecasts.png", dpi=160)
        plt.close(fig)
