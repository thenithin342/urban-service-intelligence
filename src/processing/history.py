"""Clean only manifest-verified historical downloads, retaining acquisition dates."""

import json
from pathlib import Path

from pyspark.sql import functions as F

from src.ingestion.downloader import sha256_file
from src.ingestion.history import daily_windows, save_json
from src.processing.clean import clean, read_raw, write_clean


def process_history(config: dict, root: Path, spark) -> dict:
    folder = root / config["out_dir"]
    manifest_path = folder / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not manifest["complete"]:
        raise ValueError("historical download is incomplete")
    if not manifest["rows_written"]:
        raise ValueError("historical window has no records to clean")
    days = manifest["days"]
    expected_days = [d for d, _ in daily_windows(config["start"], config["end"])]
    if (manifest["start"] != config["start"] or manifest["end"] != config["end"]
            or [item["day"] for item in days] != expected_days):
        raise ValueError("manifest date coverage differs from requested window")
    for item in days:
        if sha256_file(folder / f"{item['day']}.csv") != item["sha256"]:
            raise ValueError(f"checkpoint checksum mismatch: {item['day']}")
    frame, metrics = clean(read_raw(spark, str(folder / "*.csv")))
    if (metrics["input_rows"] != manifest["rows_written"]
            or metrics["output_rows"] != manifest["rows_written"]
            or frame.select("unique_key").distinct().count() != manifest["rows_written"]):
        raise ValueError("historical ETL row/key reconciliation failed")
    counts = {str(r["created_day"]): r["count"]
              for r in frame.groupBy("created_day").count().collect()}
    if (set(counts) - set(expected_days)
            or any(counts.get(item["day"], 0) != item["rows"] for item in days)):
        raise ValueError("historical ETL daily reconciliation failed")
    snapshots = F.create_map(*[value for item in days for value in (
        F.lit(item["day"]), F.lit(item["acquired_at_utc"][:10]))])
    frame = frame.withColumn("data_snapshot_date", F.to_date(
        F.element_at(snapshots, F.col("created_day").cast("string"))))
    output = root / "data/processed/member1_history"
    write_clean(frame, str(output))
    written = spark.read.parquet(str(output))
    if written.count() != metrics["output_rows"]:
        raise ValueError("Parquet readback reconciliation failed")
    metrics.pop("snapshot_date")
    metrics.update(
        source_manifest_sha256=sha256_file(manifest_path),
        snapshot_dates=sorted({item["acquired_at_utc"][:10] for item in days}),
        event_start=config["start"], event_end_exclusive=config["end"],
        daily_rows={day: counts.get(day, 0) for day in expected_days},
        monthly_rows=manifest["monthly_rows"],
        missing_days=[], output_path=str(output), spark_version=spark.version,
        snapshot_semantics="UTC acquisition date per daily checkpoint, not event date",
    )
    report = root / "artifacts/member1/phase4/etl_quality.json"
    report.parent.mkdir(parents=True, exist_ok=True)
    save_json(report, metrics)
    return metrics
