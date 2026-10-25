"""Small report figures from exported demand aggregates and predictions."""

import json
from datetime import date, timedelta

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from pyspark.sql import functions as F

from src.analytics.spark_eda import bounded_pandas


def write_plots(spark, out):
    meta = json.loads((out / "model_manifest.json").read_text())
    test_cutoff = next(r["fit_cutoff"] for r in meta["fits"] if r["split"] == "test")
    test_start = date.fromisoformat(test_cutoff) + timedelta(days=1)
    metrics = bounded_pandas(spark.read.parquet(str(out / "metrics")), 1000)
    test = metrics[metrics["split"] == "test"].sort_values("mae")
    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.bar(test["model"], test["mae"])
    ax.set_ylabel("MAE (complaints per borough-day)")
    ax.set_title(f"Retrospective test {test_start}..{meta['coverage']['end']}; horizons 1-7 days")
    ax.tick_params(axis="x", rotation=25)
    fig.tight_layout()
    fig.savefig(out / "model_comparison.png", dpi=160)
    plt.close(fig)
    daily = spark.read.parquet(str(out / "daily"))
    end = daily.agg(F.max("created_day")).first()[0]
    observed = bounded_pandas(daily.filter(F.col("created_day") > F.date_sub(F.lit(end), 28)), 1000)
    future = bounded_pandas(spark.read.parquet(str(out / "future")), 1000)
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
