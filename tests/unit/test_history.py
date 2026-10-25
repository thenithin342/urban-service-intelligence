"""Small synthetic acquisition fixtures; never used for research findings."""

import csv
import json

import pytest

from src.ingestion.downloader import DEFAULT_FIELDS
from src.ingestion.history import daily_windows, download_history


def config():
    return {"start": "2024-02-28", "end": "2024-03-01", "page_size": 2,
            "max_rows": 10, "max_bytes": 10000, "out_dir": "raw",
            "base_url": "https://example.test/data", "metadata_url": "https://example.test/meta"}


def mock_source(monkeypatch, *, duplicate=False, drift=False, fail=False):
    calls = []
    metadata = {"columns": [{"fieldName": field} for field in DEFAULT_FIELDS]}

    class Response:
        def raise_for_status(self):
            pass

        def json(self):
            return metadata

    monkeypatch.setattr("requests.Session.get", lambda *a, **k: Response())
    count_calls = 0

    def fetch(session, url, params, **kwargs):
        nonlocal count_calls
        calls.append(params)
        if "count(*)" in params["$select"]:
            count_calls += 1
            return [{"day": "2024-02-28T00:00:00", "n": str(3 if drift and count_calls > 1 else 2)}]
        if fail:
            raise RuntimeError("network interruption")
        if "unique_key >" in params["$where"] or "2024-02-29T00:00:00' AND" in params["$where"]:
            return []
        return [{"unique_key": key, "created_date": "2024-02-28T01:00:00.000"}
                for key in (["1", "1"] if duplicate else ["1", "2"])]

    monkeypatch.setattr("src.ingestion.history.fetch_page", fetch)
    return calls


def test_windows_leap_year_and_exclusive_end():
    assert list(daily_windows("2024-02-28", "2024-03-01")) == [
        ("2024-02-28", "2024-02-29"), ("2024-02-29", "2024-03-01")]
    with pytest.raises(ValueError):
        list(daily_windows("2024-03-01", "2024-02-28"))


def test_download_resume_checksums_zero_days_and_keyset(tmp_path, monkeypatch):
    calls = mock_source(monkeypatch)
    first = download_history(config(), tmp_path)
    assert first["complete"] and first["rows_written"] == 2
    assert first["monthly_rows"] == {"2024-02": 2}
    assert first["zero_complaint_days"] == ["2024-02-29"]
    assert any("unique_key > '2'" in c["$where"] for c in calls)
    calls.clear()
    again = download_history(config(), tmp_path)
    assert again["days"] == first["days"]
    assert len(calls) == 2  # count validation only; no data download
    (tmp_path / "raw/2024-02-28.csv").write_text("corrupt")
    repaired = download_history(config(), tmp_path)
    assert repaired["rows_written"] == 2
    with (tmp_path / "raw/2024-02-28.csv").open(newline="") as stream:
        assert len(list(csv.DictReader(stream))) == 2


@pytest.mark.parametrize("mode", ["duplicate", "drift", "fail", "row_bound", "byte_bound"])
def test_failures_never_publish_complete_manifest(tmp_path, monkeypatch, mode):
    mock_source(monkeypatch, **{mode: True} if mode in {"duplicate", "drift", "fail"} else {})
    cfg = config()
    if mode == "row_bound":
        cfg["max_rows"] = 1
    if mode == "byte_bound":
        cfg["max_bytes"] = 1
    with pytest.raises((ValueError, RuntimeError)):
        download_history(cfg, tmp_path)
    manifest = json.loads((tmp_path / "raw/manifest.json").read_text())
    assert not manifest["complete"] and manifest["failures"]
    assert not list((tmp_path / "raw").glob("*.partial"))


def test_resume_rejects_different_scope(tmp_path, monkeypatch):
    mock_source(monkeypatch)
    download_history(config(), tmp_path)
    with pytest.raises(ValueError, match="different request"):
        download_history({**config(), "end": "2024-03-02"}, tmp_path)
