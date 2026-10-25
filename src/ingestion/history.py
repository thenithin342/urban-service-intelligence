"""Bounded historical acquisition with verified daily checkpoints."""

from __future__ import annotations

import csv
import json
import os
import time
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

from src.ingestion.downloader import DEFAULT_FIELDS, fetch_page, sha256_file


def daily_windows(start: str, end: str):
    """Half-open NYC wall-date windows; end is exclusive."""
    first, stop = date.fromisoformat(start), date.fromisoformat(end)
    if first >= stop:
        raise ValueError("start must precede exclusive end")
    while first < stop:
        following = first + timedelta(days=1)
        yield first.isoformat(), following.isoformat()
        first = following


def save_json(path: Path, value: dict) -> None:
    temporary = path.with_suffix(".tmp")
    for attempt in range(6):
        try:
            temporary.write_text(json.dumps(value, indent=2), encoding="utf-8")
            temporary.replace(path)
            return
        except PermissionError:
            if attempt == 5:
                raise
            # Windows readers/sync tools can briefly lock an otherwise writable file.
            time.sleep(0.1 * 2**attempt)


def download_history(config: dict, root: Path) -> dict:
    """Resume complete days; interrupted days restart without publishing partial CSVs."""
    windows = list(daily_windows(config["start"], config["end"]))
    page_size = int(config["page_size"])
    max_rows, max_bytes = int(config["max_rows"]), int(config["max_bytes"])
    if not 1 <= page_size <= 50000 or min(max_rows, max_bytes) <= 0:
        raise ValueError("positive row/byte bounds and page_size <= 50000 required")
    folder = root / config["out_dir"]
    folder.mkdir(parents=True, exist_ok=True)
    fields = DEFAULT_FIELDS
    start, end = config["start"], config["end"]
    scope = f"created_date >= '{start}T00:00:00' AND created_date < '{end}T00:00:00'"
    identity = {"base_url": config["base_url"], "start": start, "end": end,
                "fields": fields}
    identity_path = folder / "request.json"
    if identity_path.exists() and json.loads(identity_path.read_text()) != identity:
        raise ValueError("output directory belongs to a different request")
    save_json(identity_path, identity)
    manifest_path = folder / "manifest.json"
    manifest = {**identity, "complete": False, "days": [], "failures": [],
                "started_at_utc": datetime.now(timezone.utc).isoformat()}
    save_json(manifest_path, manifest)
    with requests.Session() as session:
        token = os.environ.get("SOCRATA_APP_TOKEN")
        if token:
            session.headers["X-App-Token"] = token

        def query(params):
            return fetch_page(session, config["base_url"], params,
                              timeout=int(config.get("timeout_seconds", 180)),
                              max_retries=int(config.get("max_retries", 4)))

        def counts():
            rows = query({"$select": "date_trunc_ymd(created_date) as day,count(*) as n",
                          "$where": scope, "$group": "day", "$order": "day",
                          "$limit": str(len(windows) + 1)})
            result = {r["day"][:10]: int(r["n"]) for r in rows}
            if len(result) != len(rows) or set(result) - {d for d, _ in windows}:
                raise ValueError("invalid source daily counts")
            return {day: result.get(day, 0) for day, _ in windows}

        try:
            response = session.get(config["metadata_url"], timeout=30)
            response.raise_for_status()
            metadata = response.json()
            available = {c["fieldName"] for c in metadata["columns"]}
            if set(fields) - available:
                raise ValueError(f"source schema missing: {set(fields) - available}")
            save_json(folder / "source_metadata.json", metadata)
            expected = counts()
            manifest["expected_rows"] = sum(expected.values())
            manifest["source_rows_updated_at"] = metadata.get("rowsUpdatedAt")
            save_json(manifest_path, manifest)
            if sum(expected.values()) > max_rows:
                raise ValueError("source count exceeds max_rows; narrow dates or raise explicit bound")
            used_bytes = 0
            for day, following in windows:
                path, checkpoint = folder / f"{day}.csv", folder / f"{day}.json"
                previous = json.loads(checkpoint.read_text()) if checkpoint.exists() else None
                if (previous and path.exists() and previous["rows"] == expected[day]
                        and previous["sha256"] == sha256_file(path)):
                    info = previous
                    print(f"[history] verified checkpoint {day}: {info['rows']}", flush=True)
                else:
                    where = (f"created_date >= '{day}T00:00:00' AND "
                             f"created_date < '{following}T00:00:00'")
                    temporary = path.with_suffix(".partial")
                    seen, cursor, requests_log = set(), None, []
                    try:
                        with temporary.open("w", encoding="utf-8", newline="") as stream:
                            writer = csv.DictWriter(stream, fieldnames=fields)
                            writer.writeheader()
                            while True:
                                condition = where
                                if cursor:
                                    stamp, key = cursor
                                    condition += (f" AND (created_date > '{stamp}' OR "
                                                  f"(created_date = '{stamp}' AND unique_key > '{key}'))")
                                params = {"$select": ",".join(fields), "$where": condition,
                                          "$order": "created_date,unique_key", "$limit": str(page_size)}
                                page = query(params)
                                requests_log.append(params)
                                if len(page) > page_size:
                                    raise ValueError("source exceeded requested page size")
                                for row in page:
                                    key, stamp = row.get("unique_key", ""), row.get("created_date", "")
                                    parsed = datetime.fromisoformat(stamp)
                                    if (not key.isdigit() or parsed.tzinfo is not None
                                            or parsed.date().isoformat() != day):
                                        raise ValueError("invalid source key or event date")
                                    current = (stamp, key)
                                    if key in seen or (cursor and current <= cursor):
                                        raise ValueError("duplicate key or non-increasing source cursor")
                                    seen.add(key)
                                    cursor = current
                                    if len(seen) > expected[day]:
                                        raise ValueError("source grew during download")
                                    writer.writerow({f: row.get(f, "") for f in fields})
                                stream.flush()
                                if used_bytes + temporary.stat().st_size > max_bytes:
                                    raise ValueError("download exceeds max_bytes")
                                if len(page) < page_size:
                                    break
                        if len(seen) != expected[day]:
                            raise ValueError(f"daily count mismatch on {day}")
                        temporary.replace(path)
                        info = {"day": day, "rows": len(seen), "sha256": sha256_file(path),
                                "bytes": path.stat().st_size,
                                "acquired_at_utc": datetime.now(timezone.utc).isoformat(),
                                "requests": requests_log}
                        save_json(checkpoint, info)
                    finally:
                        temporary.unlink(missing_ok=True)
                    print(f"[history] downloaded {day}: {info['rows']}", flush=True)
                used_bytes += path.stat().st_size
                if used_bytes > max_bytes:
                    raise ValueError("checkpoints exceed max_bytes")
                manifest["days"].append(info)
                save_json(manifest_path, manifest)
            after = counts()
            manifest["source_daily_counts_after"] = after
            if expected != after:
                raise ValueError("source daily counts changed; rerun to reconcile checkpoints")
            monthly = Counter()
            for item in manifest["days"]:
                monthly[item["day"][:7]] += item["rows"]
            manifest.update(complete=True, rows_written=sum(monthly.values()),
                            bytes_written=used_bytes, monthly_rows=dict(monthly),
                            zero_complaint_days=[d for d, n in expected.items() if n == 0],
                            verified_at_utc=datetime.now(timezone.utc).isoformat())
            save_json(manifest_path, manifest)
        except Exception as error:
            manifest["failures"].append(str(error))
            save_json(manifest_path, manifest)
            raise
    return manifest
