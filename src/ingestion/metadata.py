"""Dataset metadata snapshot helpers. ASCII only."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import requests

REQUIRED_ATTRS = [
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


def fetch_json(url: str, timeout: int = 30) -> Any:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()


def summarize_columns(columns_meta: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for c in columns_meta:
        out.append(
            {
                "fieldName": c.get("fieldName"),
                "name": c.get("name"),
                "dataTypeName": c.get("dataTypeName"),
            }
        )
    return out


def save_metadata_snapshot(
    metadata_url: str, columns_url: str, out_path: str | Path, timeout: int = 30
) -> dict[str, Any]:
    meta = fetch_json(metadata_url, timeout=timeout)
    try:
        cols = fetch_json(columns_url, timeout=timeout)
    except Exception:
        cols = []
    snapshot = {
        "id": meta.get("id"),
        "name": meta.get("name"),
        "rowsUpdatedAt": meta.get("rowsUpdatedAt"),
        "viewLastModified": meta.get("viewLastModified"),
        "columns": summarize_columns(cols) if isinstance(cols, list) else [],
        "required_attrs_present": (
            [c.get("fieldName") for c in cols]
            if isinstance(cols, list)
            else []
        ),
    }
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2)
    return snapshot
