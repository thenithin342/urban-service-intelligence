"""SODA ingestion with retries, paging, dedupe and manifest. ASCII only."""

from __future__ import annotations

import csv
import hashlib
import json
import time
from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
import yaml

DEFAULT_FIELDS = [
    "unique_key",
    "created_date",
    "closed_date",
    "complaint_type",
    "descriptor",
    "agency",
    "status",
    "borough",
    "incident_zip",
    "latitude",
    "longitude",
]


def sha256_file(path: str | Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def build_params(
    fields: Sequence[str],
    where: str | None,
    order: str | None,
    limit: int,
    offset: int,
) -> dict[str, str]:
    params: dict[str, str] = {
        "$select": ",".join(fields),
        "$limit": str(limit),
        "$offset": str(offset),
    }
    if where:
        params["$where"] = where
    if order:
        params["$order"] = order
    return params


def fetch_page(
    session: requests.Session,
    url: str,
    params: dict[str, str],
    timeout: int = 30,
    max_retries: int = 5,
    backoff_base: float = 1.0,
    sleep: Callable[[float], None] | None = None,
) -> list[dict[str, Any]]:
    """GET one SODA page with exponential backoff. Returns list of records.

    Retries transient failures (429/5xx, timeouts, malformed JSON).
    Other 4xx client errors fail fast without retrying.
    """
    if sleep is None:
        sleep = time.sleep
    last_err: Exception | None = None
    for attempt in range(max_retries):
        try:
            resp = session.get(url, params=params, timeout=timeout)
            if resp.status_code == 200:
                data = resp.json()
                if not isinstance(data, list):
                    raise ValueError("SODA response is not a JSON array")
                return data
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                raise RuntimeError(f"transient HTTP {resp.status_code}")
            resp.raise_for_status()  # non-retryable 4xx: propagates, no retry
        except RuntimeError as e:
            last_err = e
        except ValueError as e:
            last_err = e
        except requests.RequestException as e:
            # HTTPError from raise_for_status carries the response; retry
            # only if it was transient, else re-raise. Transport errors
            # with no response (timeouts, DNS) are always retried.
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status is None or status == 429 or (
                isinstance(status, int) and 500 <= status < 600
            ):
                last_err = e
            else:
                raise
        if attempt < max_retries - 1:
            sleep(backoff_base * (2**attempt))
    raise RuntimeError(f"fetch_page failed after {max_retries} attempts: {last_err}")


def dedupe_records(
    records: list[dict[str, Any]], key: str = "unique_key"
) -> tuple[list[dict[str, Any]], int]:
    """Dedupe preserving first occurrence. Returns (deduped, n_duplicates).

    Records with a missing/blank key pass through untouched: collapsing
    distinct keyless rows into one "duplicate" would silently drop data.
    (The ETL excludes and counts keyless rows downstream.)
    """
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    dups = 0
    for r in records:
        k = str(r.get(key, "") or "").strip()
        if not k:
            out.append(r)
            continue
        if k in seen:
            dups += 1
            continue
        seen.add(k)
        out.append(r)
    return out, dups


def fetch_metadata(metadata_url: str, timeout: int = 30) -> dict[str, Any]:
    try:
        r = requests.get(metadata_url, timeout=timeout)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, dict):
                return {
                    "name": data.get("name"),
                    "rowsUpdatedAt": data.get("rowsUpdatedAt"),
                    "viewLastModified": data.get("viewLastModified"),
                }
    except requests.RequestException:
        pass
    return {}


def download_sample(
    config: dict[str, Any],
    project_root: str | Path,
    fetcher: Callable[..., list[dict[str, Any]]] | None = None,
    app_token: str | None = None,
) -> dict[str, Any]:
    """Page through SODA until target_rows or exhaustion. Writes CSV + manifest.

    Returns the manifest dict. A failed page raises before publishing any
    sample or manifest, preserving the previous successful download.
    """
    import os
    import sys

    def _log(msg: str) -> None:
        print(msg, flush=True)
        sys.stdout.flush()
    root = Path(project_root)
    fields = list(config.get("fields") or DEFAULT_FIELDS)
    where = config.get("where")
    order = config.get("order") or "unique_key"
    target = int(config["target_rows"])
    page_size = int(config["page_size"])
    timeout = int(config.get("timeout_seconds", 30))
    max_retries = int(config.get("max_retries", 5))
    backoff = float(config.get("backoff_base_seconds", 1.0))
    url = config["base_url"]
    out_raw = root / str(config["out_raw"])
    out_manifest = root / str(config["out_manifest"])
    out_raw.parent.mkdir(parents=True, exist_ok=True)
    out_manifest.parent.mkdir(parents=True, exist_ok=True)

    token = app_token or os.environ.get("SOCRATA_APP_TOKEN", "")
    session = requests.Session()
    if token:
        session.headers.update({"X-App-Token": token})

    if fetcher is None:
        def _fetch(params: dict[str, str]) -> list[dict[str, Any]]:
            return fetch_page(
                session, url, params, timeout=timeout,
                max_retries=max_retries, backoff_base=backoff,
            )
        fetcher = _fetch

    acquired_at = datetime.now(timezone.utc).isoformat()
    request_log: list[str] = []
    all_records: list[dict[str, Any]] = []
    offset = 0
    pages = 0
    failures: list[str] = []
    while len(all_records) < target:
        limit = min(page_size, target - len(all_records))
        params = build_params(fields, where, order, limit, offset)
        # log canonical request URL for provenance
        req = requests.PreparedRequest()
        req.prepare_url(url, params)
        request_log.append(req.url or url)
        try:
            _log(f"[download] page {pages + 1}: limit={limit} offset={offset}")
            page = fetcher(params)
        except Exception as e:
            _log(f"[download] FAILED offset={offset}: {e}")
            raise
        pages += 1
        if not page:
            _log("[download] empty page, stopping")
            break
        all_records.extend(page)
        offset += len(page)
        _log(f"[download] got {len(page)} rows (total {len(all_records)})")
        if len(page) < limit:
            break  # exhausted

    deduped, n_dups = dedupe_records(all_records)
    rows = deduped[:target]

    with open(out_raw, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in fields})

    checksum = sha256_file(out_raw)
    meta = fetch_metadata(str(config.get("metadata_url", "")))
    manifest = {
        "dataset_id": config.get("dataset_id"),
        "acquired_at_utc": acquired_at,
        "base_url": url,
        "where": where,
        "order": order,
        "fields": fields,
        "target_rows": target,
        "pages_fetched": pages,
        "rows_received": len(all_records),
        "rows_written": len(rows),
        "duplicates_removed": n_dups,
        "failures": failures,
        "request_urls_first": request_log[:3],
        "request_urls_last": request_log[-3:] if request_log else [],
        "request_count": len(request_log),
        "file": str(config["out_raw"]),
        "sha256": checksum,
        "source_metadata": meta,
        "seed": config.get("seed", 42),
    }
    with open(out_manifest, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    # also dump config snapshot next to manifest for reproducibility
    with open(str(out_manifest) + ".config.yaml", "w", encoding="utf-8") as f:
        yaml.safe_dump(config, f, sort_keys=False)
    return manifest
