"""Member 3 priority aggregation: Spark vs pandas reference + CLI run. ASCII only.

Synthetic records only (explicitly not real data). The Spark aggregation that
feeds the priority score must equal the independent pandas reference, and the
CLI must publish contract-valid priority_scores / priority_backtest tables.
"""

from __future__ import annotations

import importlib.util
import json
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.prioritization import aggregate
from src.utils import contracts
from src.utils.spark import get_spark

REPO = Path(__file__).resolve().parents[2]
SCHEMA = StructType(
    [
        StructField("created_at", TimestampType(), True),
        StructField("closed_at", TimestampType(), True),
        StructField("created_day", DateType(), True),
        StructField("complaint_type", StringType(), True),
        StructField("borough", StringType(), True),
        StructField("incident_zip", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("resolution_hours", DoubleType(), True),
    ]
)


def _records(n_days: int = 10, seed: int = 7) -> list[dict]:
    rng = np.random.default_rng(seed)
    rows = []
    boroughs = ["BRONX", "QUEENS", None]
    types = ["Heat", "Noise", "Parking"]
    for day in range(n_days):
        for _ in range(40):
            created = datetime(2024, 1, 1) + timedelta(days=day, minutes=int(rng.integers(0, 1440)))
            r = rng.random()
            if r < 0.15:
                closed = None
            elif r < 0.18:
                closed = created - timedelta(hours=2)  # negative duration
            else:
                closed = created + timedelta(hours=float(rng.exponential(60)))
            hours = (closed - created).total_seconds() / 3600 if closed and closed >= created else None
            valid = rng.random() > 0.1
            rows.append(
                {
                    "created_at": created,
                    "closed_at": closed,
                    "created_day": created.date(),
                    "complaint_type": types[int(rng.integers(0, 3))],
                    "borough": boroughs[int(rng.integers(0, 3))],
                    "incident_zip": ["10001", "11201", None][int(rng.integers(0, 3))],
                    "latitude": 40.6 + rng.random() * 0.2 if valid else None,
                    "longitude": -73.95 + rng.random() * 0.1 if valid else None,
                    "resolution_hours": hours,
                }
            )
    return rows


@pytest.fixture(scope="module")
def spark():
    s = get_spark("member3-priority-test", master="local[2]")
    yield s


def _to_spark(spark, rows: list[dict]):
    names = [f.name for f in SCHEMA.fields]
    return spark.createDataFrame([tuple(r[n] for n in names) for r in rows], SCHEMA)


def _norm(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    out = df.copy()
    for k in keys:
        if k in ("day", "closed_day"):
            out[k] = pd.to_datetime(out[k]).dt.date
    return out.sort_values(keys).reset_index(drop=True)


@pytest.mark.parametrize("location_type", ["borough", "zip"])
def test_spark_aggregate_matches_pandas_reference(spark, tmp_path, location_type):
    rows = _records()
    recs = pd.DataFrame(rows)
    path = str(tmp_path / "clean")
    _to_spark(spark, rows).write.parquet(path)
    got = aggregate.aggregate_spark(spark, path, location_type, lag_cap_days=5)
    ref = aggregate.aggregate_pandas(recs, location_type, lag_cap_days=5)
    assert got["records"] == ref["records"] == len(recs)
    assert got["records_without_location"] == ref["records_without_location"]

    ck = ["day", "location", "complaint_type", "close_lag_days"]
    a = _norm(got["created"], ck)
    b = _norm(ref["created"], ck)
    a["close_lag_days"] = a["close_lag_days"].astype("Int64")
    b["close_lag_days"] = b["close_lag_days"].astype("Int64")
    pd.testing.assert_frame_equal(a[[*ck, "n"]], b[[*ck, "n"]], check_dtype=False)
    assert int(a["n"].sum()) == len(recs) - ref["records_without_location"]

    dk = ["closed_day", "location", "complaint_type", "hours_bin"]
    pd.testing.assert_frame_equal(
        _norm(got["delay"], dk)[[*dk, "n"]], _norm(ref["delay"], dk)[[*dk, "n"]],
        check_dtype=False,
    )
    ca = got["coords"].sort_values("location").reset_index(drop=True)
    cb = ref["coords"].sort_values("location").reset_index(drop=True)
    assert ca["location"].tolist() == cb["location"].tolist()
    np.testing.assert_allclose(ca["center_lat"], cb["center_lat"])
    assert ca["n_coords"].tolist() == cb["n_coords"].tolist()


def test_cli_publishes_valid_contract_tables(spark, tmp_path):
    clean = tmp_path / "data" / "processed" / "clean"
    _to_spark(spark, _records(n_days=12)).write.parquet(str(clean))
    spec = importlib.util.spec_from_file_location(
        "run_member3_priorities", REPO / "scripts" / "run_member3_priorities.py"
    )
    cli = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cli)
    cfg = tmp_path / "member3.yaml"
    cfg.write_text("member3:\n  backtest_k: [1, 2]\n  min_delay_records: 1\n")
    code = cli.main(
        ["--config", str(cfg), "--input", str(clean),
         "--root", str(tmp_path), "--window-days", "3", "--step-days", "3"]
    )
    assert code == 0
    for name in ("priority_scores", "priority_backtest"):
        df = pd.read_parquet(contracts.contract_path(tmp_path, name))
        assert contracts.validate(df, name) == []
        meta = json.loads(contracts.manifest_path(tmp_path, name).read_text())
        assert meta["is_fixture"] is False
        assert meta["weights"] == {"demand": 0.5, "backlog": 0.3, "delay": 0.2}
        assert meta["coverage_start"] == "2024-01-01"
        assert "not a proven staffing optimum" in meta["label"]
    scores = pd.read_parquet(contracts.contract_path(tmp_path, "priority_scores"))
    assert scores["rank"].tolist() == list(range(1, len(scores) + 1))
    assert scores["priority_score"].is_monotonic_decreasing
    for comp in ("demand", "backlog", "delay"):
        assert scores[f"{comp}_component"].between(0, 1).all()
    bt = pd.read_parquet(contracts.contract_path(tmp_path, "priority_backtest"))
    assert set(bt["method"]) == {"priority_score", "volume_only", "random"}
    assert set(bt["k"]) == {1, 2}
    assert bt["precision_at_k"].between(0, 1).all()
    assert (pd.to_datetime(bt["target_start"]) > pd.to_datetime(bt["window_end"])).all()
