"""Regression checks for failures found during the Member 1 merge audit."""

import csv
import json
from pathlib import Path

import pytest
from py4j.protocol import Py4JJavaError

from scripts.run_member1_analytics import main
from src.analytics import clustering
from src.ingestion.downloader import DEFAULT_FIELDS
from src.processing.clean import clean, read_raw
from src.utils.spark import get_spark


def test_csv_reader_rejects_reordered_header(tmp_path):
    path = tmp_path / "reordered.csv"
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(reversed(DEFAULT_FIELDS)))
        writer.writeheader()
        writer.writerow({"unique_key": "123", "latitude": "40.7", "longitude": "-73.9"})
    spark = get_spark("audit-csv-header")
    try:
        with pytest.raises(Py4JJavaError, match="CSV header does not conform"):
            read_raw(spark, str(path)).collect()
    finally:
        spark.stop()


def test_csv_reader_preserves_quoted_newlines(tmp_path):
    path = tmp_path / "quoted.csv"
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=DEFAULT_FIELDS)
        writer.writeheader()
        writer.writerow({"unique_key": "1", "descriptor": 'line one\nline "two"',
                         "created_date": "2024-01-01T00:00:00.000"})
    spark = get_spark("audit-multiline")
    try:
        rows = read_raw(spark, str(path)).collect()
        assert len(rows) == 1
        assert rows[0]["descriptor"] == 'line one\nline "two"'
    finally:
        spark.stop()


@pytest.mark.parametrize("geo_cells", [0, 1, 2])
def test_analytics_cli_with_insufficient_geo_and_unresolved_only(tmp_path, geo_cells):
    raw = tmp_path / "one.csv"
    with raw.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=DEFAULT_FIELDS)
        writer.writeheader()
        for i in range(max(1, geo_cells)):
            writer.writerow({"unique_key": str(i + 1), "created_date": "2024-01-01T00:00:00.000",
                             "borough": "QUEENS", "complaint_type": "Noise",
                             "latitude": str(40.745 + i * 0.02) if geo_cells else "",
                             "longitude": "-73.945" if geo_cells else ""})
    input_path, output = tmp_path / "input", tmp_path / "analytics"
    spark = get_spark("audit-edge-seed")
    try:
        frame, _ = clean(read_raw(spark, str(raw)), snapshot="2026-10-25")
        frame.write.parquet(str(input_path))
    finally:
        spark.stop()
    assert main(["--config", "configs/sample.yaml", "--input", str(input_path),
                 "--outdir", str(output)]) == 0
    metrics = json.loads((output / "phase3_metrics.json").read_text())
    manifest = json.loads((output / "phase3_manifest.json").read_text())
    assert metrics["verify"]["row_count"] == max(1, geo_cells)
    assert metrics["clustering"]["n_cells"] == geo_cells
    assert metrics["ingestion_manifest_path"] is None
    assert "ingestion_filter" not in metrics["coverage"]
    assert manifest["tables"]["kmeans_ablation"]["rows"] == 3
    assert "cluster:int" in manifest["tables"]["spatial_grid_cells"]["columns"]
    assert "resolution_hist" not in manifest["figures"]
    assert all(Path(item["path"]).is_file() for item in manifest["figures"].values())
    if geo_cells == 2:
        spark = get_spark("audit-null-cluster-duration")
        try:
            summary = spark.read.parquet(manifest["tables"]["cluster_summary"]["path"])
            assert summary.count() == 2
            assert summary.filter("mean_resolution_hours IS NOT NULL").count() == 0
        finally:
            spark.stop()


def test_select_k_rejects_oversized_grid_before_training():
    spark = get_spark("audit-k-guard")
    try:
        frame = spark.range(1)
        with pytest.raises(ValueError, match="no k values evaluated"):
            clustering.select_k(frame, "missing_features", [2, 8], 42)
    finally:
        spark.stop()
