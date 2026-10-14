"""Unit tests for ingestion helpers. ASCII only."""

from __future__ import annotations

import json
from pathlib import Path

import requests

from src.ingestion.downloader import (
    build_params,
    dedupe_records,
    download_sample,
    fetch_page,
    sha256_file,
)


class FakeResp:
    def __init__(self, status, payload=None):
        self.status_code = status
        self._payload = payload

    def json(self):
        return self._payload

    def raise_for_status(self):
        raise requests.HTTPError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self, script):
        self.script = list(script)
        self.calls = 0

    def get(self, url, params=None, timeout=None):
        self.calls += 1
        item = self.script[min(self.calls - 1, len(self.script) - 1)]
        if isinstance(item, Exception):
            raise item
        return item


def test_build_params():
    p = build_params(["a", "b"], "x", "unique_key", 10, 20)
    assert p["$select"] == "a,b"
    assert p["$limit"] == "10"
    assert p["$offset"] == "20"
    assert p["$where"] == "x"
    assert p["$order"] == "unique_key"


def test_fetch_page_retries_then_succeeds():
    sleeps = []
    sess = FakeSession([FakeResp(503), FakeResp(200, [{"a": 1}])])
    out = fetch_page(
        sess, "http://x", {"$limit": "1"}, max_retries=3,
        backoff_base=0.0, sleep=sleeps.append,
    )
    assert out == [{"a": 1}]
    assert sess.calls == 2
    assert len(sleeps) == 1


def test_fetch_page_raises_after_retries():
    sess = FakeSession([FakeResp(503)])
    try:
        fetch_page(sess, "http://x", {}, max_retries=2,
                   backoff_base=0.0, sleep=lambda s: None)
    except RuntimeError:
        pass
    else:
        raise AssertionError("expected RuntimeError")


def test_dedupe_records():
    recs = [{"unique_key": "1"}, {"unique_key": "1"}, {"unique_key": "2"}]
    out, dups = dedupe_records(recs)
    assert len(out) == 2
    assert dups == 1


def test_download_sample_paging_and_manifest(tmp_path):
    pages = [
        [{"unique_key": "1", "borough": "QUEENS"},
         {"unique_key": "2", "borough": "BRONX"}],
        [{"unique_key": "2", "borough": "BRONX"},
         {"unique_key": "3", "borough": "QUEENS"}],
        [],
    ]
    state = {"i": 0}

    def fetcher(params):
        i = state["i"]
        state["i"] += 1
        return pages[min(i, len(pages) - 1)]

    cfg = {
        "dataset_id": "erm2-nwe9",
        "base_url": "http://example",
        "metadata_url": "http://example/meta",
        "fields": ["unique_key", "borough"],
        "where": "x",
        "order": "unique_key",
        "target_rows": 10,
        "page_size": 2,
        "timeout_seconds": 5,
        "max_retries": 1,
        "backoff_base_seconds": 0.0,
        "seed": 42,
        "out_raw": "data/raw/t.csv",
        "out_manifest": "data/manifests/t.json",
    }
    manifest = download_sample(cfg, tmp_path, fetcher=fetcher)
    raw = tmp_path / "data" / "raw" / "t.csv"
    man = tmp_path / "data" / "manifests" / "t.json"
    assert raw.exists() and man.exists()
    assert manifest["rows_written"] == 3
    assert manifest["duplicates_removed"] == 1
    assert manifest["sha256"] == sha256_file(raw)
    saved = json.loads(Path(man).read_text(encoding="utf-8"))
    assert saved["rows_written"] == 3
    # provenance present
    assert saved["request_count"] >= 2
    assert saved["where"] == "x"
