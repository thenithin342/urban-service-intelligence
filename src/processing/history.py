"""Clean only manifest-verified historical downloads, retaining acquisition dates."""

import hashlib
import json
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark.sql import functions as F

from src.ingestion.downloader import sha256_file
from src.ingestion.history import daily_windows, save_json
from src.processing.clean import clean, read_raw, write_clean


def process_history(config: dict, root: Path, spark) -> dict:
    folder = root / config["out_dir"]
    manifest_path = folder / "manifest.json"
    manifest_bytes = manifest_path.read_bytes()
    manifest_hash = hashlib.sha256(manifest_bytes).hexdigest()
    manifest = json.loads(manifest_bytes)
    if not manifest["complete"]:
        raise ValueError("historical download is incomplete")
    if not manifest["rows_written"]:
        raise ValueError("historical window has no records to clean")
    days = manifest["days"]
    expected_days = [d for d, _ in daily_windows(config["start"], config["end"])]
    if (manifest["base_url"] != config["base_url"]
            or manifest["start"] != config["start"] or manifest["end"] != config["end"]
            or [item["day"] for item in days] != expected_days):
        raise ValueError("manifest source or date coverage differs from requested window")
    monthly = Counter()
    snapshot_dates = {}
    for item in days:
        acquired = datetime.fromisoformat(item["acquired_at_utc"])
        if acquired.tzinfo is None or acquired.utcoffset() != timedelta(0):
            raise ValueError("checkpoint acquisition time must include UTC offset")
        snapshot_dates[item["day"]] = acquired.date().isoformat()
        if not isinstance(item["rows"], int) or item["rows"] < 0:
            raise ValueError("invalid checkpoint row count")
        monthly[item["day"][:7]] += item["rows"]
        if sha256_file(folder / f"{item['day']}.csv") != item["sha256"]:
            raise ValueError(f"checkpoint checksum mismatch: {item['day']}")
    if dict(monthly) != manifest["monthly_rows"] or sum(monthly.values()) != manifest["rows_written"]:
        raise ValueError("manifest monthly/total counts do not reconcile")
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
    snapshots = F.create_map(*[value for day, snapshot in snapshot_dates.items()
                              for value in (F.lit(day), F.lit(snapshot))])
    frame = frame.withColumn("data_snapshot_date", F.to_date(
        F.element_at(snapshots, F.col("created_day").cast("string"))))
    output = root / config.get("etl_output", "data/processed/member1_history")
    output.parent.mkdir(parents=True, exist_ok=True)
    if not output.resolve().is_relative_to(root.resolve()):
        raise ValueError("historical output must stay inside the project root")
    # shortcut: one writer per output; add locking before concurrent scheduling.
    with TemporaryDirectory(prefix=".history-", dir=output.parent) as scratch:
        staged = Path(scratch) / "next"
        write_clean(frame, str(staged))
        written = spark.read.parquet(str(staged))
        if written.count() != metrics["output_rows"]:
            raise ValueError("Parquet readback reconciliation failed")
        if sha256_file(manifest_path) != manifest_hash:
            raise ValueError("source manifest changed during ETL; rerun after acquisition finishes")
        previous = Path(scratch) / "previous"
        if output.exists():
            output.rename(previous)
        try:
            staged.rename(output)
        except OSError:
            if previous.exists():
                previous.rename(output)
            raise
    metrics.pop("snapshot_date")
    metrics.update(
        source_manifest_sha256=manifest_hash,
        snapshot_dates=sorted(set(snapshot_dates.values())),
        event_start=config["start"], event_end_exclusive=config["end"],
        daily_rows={day: counts.get(day, 0) for day in expected_days},
        monthly_rows=manifest["monthly_rows"],
        missing_days=[], output_path=str(output), spark_version=spark.version,
        snapshot_semantics="UTC acquisition date per daily checkpoint, not event date",
    )
    report = root / config.get("etl_quality_report", "artifacts/member1/phase4/etl_quality.json")
    report.parent.mkdir(parents=True, exist_ok=True)
    save_json(report, metrics)
    return metrics
