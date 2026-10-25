"""Real Spark integration on a tiny synthetic manifest-backed acquisition."""

import json

import pytest

from src.ingestion.downloader import sha256_file
from src.ingestion.history import download_history, save_json
from src.processing import history
from src.processing.history import process_history
from src.utils.spark import get_spark
from tests.unit.test_history import config, mock_source


def test_verified_history_to_parquet_snapshot_and_fail_closed(tmp_path, monkeypatch):
    mock_source(monkeypatch)
    cfg = config()
    manifest = download_history(cfg, tmp_path)
    manifest["days"][0]["acquired_at_utc"] = "2026-10-24T12:00:00+00:00"
    path = tmp_path / "raw/manifest.json"
    save_json(path, manifest)
    spark = get_spark("history-integration")
    try:
        metrics = process_history(cfg, tmp_path, spark)
        assert metrics["input_rows"] == metrics["output_rows"] == 2
        assert metrics["daily_rows"] == {"2024-02-28": 2, "2024-02-29": 0}
        assert metrics["snapshot_dates"] == sorted({
            "2026-10-24", manifest["days"][1]["acquired_at_utc"][:10]})
        frame = spark.read.parquet(metrics["output_path"])
        assert {str(r["data_snapshot_date"]) for r in frame.collect()} == {"2026-10-24"}
        assert frame.filter("resolution_hours IS NOT NULL").count() == 0
        saved = json.loads((tmp_path / "artifacts/member1/phase4/etl_quality.json").read_text())
        assert saved["monthly_rows"] == {"2024-02": 2}
        previous_files = {str(p.relative_to(metrics["output_path"])): sha256_file(p)
                          for p in (tmp_path / "data/processed/member1_history").rglob("*")
                          if p.is_file()}
        real_write = history.write_clean

        for failure in ("write", "manifest_change"):
            def broken_write(frame, output, failure=failure):
                real_write(frame, output)
                if failure == "write":
                    raise OSError("simulated write failure")
                save_json(path, {**manifest, "changed_during_etl": True})

            with monkeypatch.context() as patch:
                patch.setattr(history, "clean", lambda *a: (frame, {**metrics}))
                patch.setattr(history, "write_clean", broken_write)
                error, message = ((OSError, "simulated write") if failure == "write"
                                  else (ValueError, "manifest changed"))
                with pytest.raises(error, match=message):
                    process_history(cfg, tmp_path, spark)
            after_files = {str(p.relative_to(metrics["output_path"])): sha256_file(p)
                           for p in (tmp_path / "data/processed/member1_history").rglob("*")
                           if p.is_file()}
            assert after_files == previous_files
            save_json(path, manifest)
        assert not list((tmp_path / "data/processed").glob(".history-*"))
        (tmp_path / "raw/2024-02-28.csv").write_text("tampered", encoding="utf-8")
        with pytest.raises(ValueError, match="checksum mismatch"):
            process_history(cfg, tmp_path, spark)
        manifest["complete"] = False
        save_json(path, manifest)
        with pytest.raises(ValueError, match="incomplete"):
            process_history(cfg, tmp_path, spark)
    finally:
        spark.stop()
