"""Synthetic (invented) cleaned-table records for Member 2 tests. ASCII only.

Not real data. Rows follow the cleaned-table schema in
docs/data_dictionary.md (subset of columns used by Member 2 code).
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pandas as pd

COLUMNS = [
    "unique_key",
    "created_at",
    "closed_at",
    "complaint_type",
    "agency",
    "status_snapshot",
    "borough",
    "incident_zip",
    "resolution_hours",
    "created_day",
    "created_week",
    "weekday",
    "hour",
    "data_snapshot_date",
    "data_quality_flags",
]

WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
SNAPSHOT = date(2024, 1, 20)


def record(key: str, created: datetime | None, closed: datetime | None, *,
           complaint: str = "Noise - Residential", agency: str = "NYPD",
           borough: str | None = "BROOKLYN", zip_code: str | None = "11201",
           status: str | None = None, snapshot: date = SNAPSHOT,
           extra_flags: list[str] | None = None) -> tuple:
    flags = list(extra_flags or [])
    hours = None
    if created is not None and closed is not None:
        h = (closed - created).total_seconds() / 3600.0
        if h < 0:
            flags.append("negative_duration")
        else:
            hours = h
    if closed is None:
        flags.append("unresolved")
    if zip_code is None:
        flags.append("invalid_zip")
    if borough is None:
        flags.append("missing_borough")
    day = created.date() if created is not None else None
    week = day - timedelta(days=day.weekday()) if day is not None else None
    return (
        key,
        created,
        closed,
        complaint,
        agency,
        status if status is not None else ("Closed" if closed is not None else "Open"),
        borough,
        zip_code,
        hours,
        day,
        week,
        WEEKDAYS[day.weekday()] if day is not None else None,
        created.hour if created is not None else None,
        snapshot,
        sorted(flags),
    )


def label_cases() -> list[tuple]:
    """Hand-labelled closure edge cases (expected label in the key)."""
    base = datetime(2024, 1, 2, 10, 0, 0)
    return [
        record("pos_2h", base, base + timedelta(hours=2)),
        record("pos_zero", base, base, complaint="Water System", agency="DEP"),
        record("pos_exact_168h", base, base + timedelta(hours=168)),
        record("neg_168h_plus_1s", base, base + timedelta(hours=168, seconds=1),
               complaint="HEAT/HOT WATER", agency="HPD", borough="BRONX", zip_code="10451"),
        record("neg_unresolved", base, None, complaint="HEAT/HOT WATER", agency="HPD",
               borough="BRONX", zip_code=None, status="In Progress"),
        record("neg_closed_status_no_date", base, None, status="Closed", borough=None),
        record("excl_negative", base, base - timedelta(hours=3), extra_flags=[]),
        record("excl_immature", datetime(2024, 1, 13, 9, 0, 0),
               datetime(2024, 1, 13, 10, 0, 0)),
        record("pos_last_mature_day", datetime(2024, 1, 12, 23, 0, 0),
               datetime(2024, 1, 13, 1, 0, 0), extra_flags=["invalid_coords"]),
    ]


EXPECTED_LABELS = {
    "pos_2h": ("eligible", 1),
    "pos_zero": ("eligible", 1),
    "pos_exact_168h": ("eligible", 1),
    "neg_168h_plus_1s": ("eligible", 0),
    "neg_unresolved": ("eligible", 0),
    "neg_closed_status_no_date": ("eligible", 0),
    "excl_negative": ("excluded_negative_duration", None),
    "excl_immature": ("excluded_immature", None),
    "pos_last_mature_day": ("eligible", 1),
}


def records_from_baskets(baskets: list[tuple[str | None, date, list[str]]]) -> list[tuple]:
    """One record per listed item (duplicates allowed) for each basket."""
    rows: list[tuple] = []
    k = 0
    for zip_code, day, items in baskets:
        for item in items:
            k += 1
            created = datetime.combine(day, datetime.min.time()) + timedelta(hours=8 + k % 10)
            rows.append(record(f"b{k:05d}", created, created + timedelta(hours=1),
                               complaint=item, zip_code=zip_code))
    return rows


def to_pandas(rows: list[tuple]) -> pd.DataFrame:
    pdf = pd.DataFrame(rows, columns=COLUMNS)
    pdf["created_at"] = pd.to_datetime(pdf["created_at"])
    pdf["closed_at"] = pd.to_datetime(pdf["closed_at"])
    return pdf


def spark_schema():
    from pyspark.sql.types import (
        ArrayType,
        DateType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    types = {
        "created_at": TimestampType(),
        "closed_at": TimestampType(),
        "resolution_hours": DoubleType(),
        "created_day": DateType(),
        "created_week": DateType(),
        "hour": IntegerType(),
        "data_snapshot_date": DateType(),
        "data_quality_flags": ArrayType(StringType()),
    }
    return StructType([StructField(c, types.get(c, StringType()), True) for c in COLUMNS])
