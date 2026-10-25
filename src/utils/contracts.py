"""Shared output contracts for Members 1-3 (docs/data_contracts.md). ASCII only.

Each contract lists required columns and a coarse logical type. Producers
validate before writing; consumers (dashboard adapters) validate after
reading. Pure pandas: no Spark required.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

CONTRACT_VERSION = "1.0.0"

# Coarse logical types checked against pandas dtypes.
STRING = "string"
INT = "int"
FLOAT = "float"
DATE = "date"
LIST = "list"
BOOL = "bool"


@dataclass(frozen=True)
class TableContract:
    name: str
    path: str  # relative to repo root
    columns: dict[str, str]
    nullable: frozenset[str] = field(default_factory=frozenset)
    owner: str = ""


ART = "artifacts"

CONTRACTS: dict[str, TableContract] = {
    c.name: c
    for c in [
        # Member 2 (B): pattern mining.
        TableContract(
            "pattern_rules",
            f"{ART}/member2/mining/tables/pattern_rules",
            {
                "antecedent": LIST,
                "consequent": LIST,
                "support": FLOAT,
                "confidence": FLOAT,
                "lift": FLOAT,
                "rule_baskets": INT,
                "n_baskets": INT,
                "min_support": FLOAT,
                "min_confidence": FLOAT,
                "basket_definition": STRING,
                "coverage_start": DATE,
                "coverage_end": DATE,
                "model_version": STRING,
            },
            owner="member2",
        ),
        TableContract(
            "frequent_itemsets",
            f"{ART}/member2/mining/tables/frequent_itemsets",
            {
                "items": LIST,
                "item_count": INT,
                "freq": INT,
                "support": FLOAT,
                "n_baskets": INT,
                "model_version": STRING,
            },
            owner="member2",
        ),
        TableContract(
            "mining_threshold_experiments",
            f"{ART}/member2/mining/tables/threshold_experiments",
            {
                "min_support": FLOAT,
                "min_confidence": FLOAT,
                "n_baskets": INT,
                "n_itemsets": INT,
                "n_rules": INT,
                "median_lift": FLOAT,
                "max_lift": FLOAT,
                "runtime_seconds": FLOAT,
            },
            nullable=frozenset({"median_lift", "max_lift"}),
            owner="member2",
        ),
        # Member 2 (B): resolution / closure.
        TableContract(
            "resolution_summary",
            f"{ART}/member2/resolution/tables/resolution_summary",
            {
                "borough": STRING,
                "complaint_type": STRING,
                "total_count": INT,
                "eligible_count": INT,
                "closed_7d_count": INT,
                "closure_rate_7d": FLOAT,
                "median_resolution_hours": FLOAT,
                "p90_resolution_hours": FLOAT,
                "unresolved_count": INT,
                "coverage_start": DATE,
                "coverage_end": DATE,
            },
            nullable=frozenset(
                {
                    "borough",
                    "closure_rate_7d",
                    "median_resolution_hours",
                    "p90_resolution_hours",
                }
            ),
            owner="member2",
        ),
        TableContract(
            "closure_model_comparison",
            f"{ART}/member2/resolution/tables/model_comparison",
            {
                "model": STRING,
                "split": STRING,
                "n": INT,
                "positive_rate": FLOAT,
                "pr_auc": FLOAT,
                "roc_auc": FLOAT,
                "brier": FLOAT,
                "precision_at_05": FLOAT,
                "recall_at_05": FLOAT,
                "model_version": STRING,
            },
            nullable=frozenset({"roc_auc", "pr_auc"}),
            owner="member2",
        ),
        TableContract(
            "closure_cohort_predictions",
            f"{ART}/member2/resolution/predictions/closure_cohorts",
            {
                "borough": STRING,
                "complaint_type": STRING,
                "n_complaints": INT,
                "mean_closure_probability": FLOAT,
                "expected_open_after_7d": FLOAT,
                "observed_closure_rate_7d": FLOAT,
                "prediction_window_start": DATE,
                "prediction_window_end": DATE,
                "model_version": STRING,
            },
            nullable=frozenset({"borough", "observed_closure_rate_7d"}),
            owner="member2",
        ),
        # Member 1 (A): demand forecasts (pending; fixture until delivered).
        TableContract(
            "demand_forecast",
            f"{ART}/member1/forecasts/tables/demand_forecast",
            {
                "forecast_date": DATE,
                "borough": STRING,
                "complaint_type": STRING,
                "predicted_count": FLOAT,
                "training_cutoff": DATE,
                "model_version": STRING,
            },
            nullable=frozenset({"borough"}),
            owner="member1",
        ),
        TableContract(
            "demand_metrics",
            f"{ART}/member1/forecasts/tables/demand_metrics",
            {
                "model": STRING,
                "mae": FLOAT,
                "rmse": FLOAT,
                "wape": FLOAT,
                "eval_start": DATE,
                "eval_end": DATE,
                "model_version": STRING,
            },
            nullable=frozenset({"wape"}),
            owner="member1",
        ),
        # Member 3 (C): prioritization.
        TableContract(
            "priority_scores",
            f"{ART}/member3/priorities/tables/priority_scores",
            {
                "location_type": STRING,
                "location": STRING,
                "complaint_type": STRING,
                "demand_value": FLOAT,
                "backlog_value": FLOAT,
                "delay_value": FLOAT,
                "demand_component": FLOAT,
                "backlog_component": FLOAT,
                "delay_component": FLOAT,
                "priority_score": FLOAT,
                "rank": INT,
                "window_start": DATE,
                "window_end": DATE,
                "scoring_version": STRING,
            },
            nullable=frozenset({"delay_value"}),
            owner="member3",
        ),
        TableContract(
            "priority_backtest",
            f"{ART}/member3/priorities/tables/priority_backtest",
            {
                "window_start": DATE,
                "window_end": DATE,
                "target_start": DATE,
                "target_end": DATE,
                "method": STRING,
                "k": INT,
                "precision_at_k": FLOAT,
                "spearman": FLOAT,
                "n_units": INT,
            },
            nullable=frozenset({"spearman"}),
            owner="member3",
        ),
    ]
}


def _kind_ok(series: pd.Series, kind: str) -> bool:
    s = series.dropna()
    if s.empty:
        return True
    dt = series.dtype
    if kind == STRING:
        return pd.api.types.is_string_dtype(dt) or pd.api.types.is_object_dtype(dt)
    if kind == INT:
        return pd.api.types.is_integer_dtype(dt) or (
            pd.api.types.is_float_dtype(dt) and bool((s % 1 == 0).all())
        )
    if kind == FLOAT:
        return pd.api.types.is_numeric_dtype(dt) and not pd.api.types.is_bool_dtype(dt)
    if kind == BOOL:
        return pd.api.types.is_bool_dtype(dt)
    if kind == DATE:
        if pd.api.types.is_datetime64_any_dtype(dt):
            return True
        return bool(pd.to_datetime(s, errors="coerce").notna().all())
    if kind == LIST:
        return bool(s.map(lambda v: hasattr(v, "__len__") and not isinstance(v, str)).all())
    raise ValueError(f"unknown kind {kind}")


def validate(df: pd.DataFrame, contract: TableContract | str) -> list[str]:
    """Return a list of human-readable contract violations (empty = valid)."""
    c = CONTRACTS[contract] if isinstance(contract, str) else contract
    problems: list[str] = []
    missing = [col for col in c.columns if col not in df.columns]
    if missing:
        problems.append(f"{c.name}: missing columns {missing}")
    for col, kind in c.columns.items():
        if col not in df.columns:
            continue
        if not _kind_ok(df[col], kind):
            problems.append(f"{c.name}.{col}: expected {kind}, got {df[col].dtype}")
        if col not in c.nullable and df[col].isna().any():
            problems.append(f"{c.name}.{col}: nulls in non-nullable column")
    return problems


def contract_path(root: str | Path, name: str) -> Path:
    return Path(root) / CONTRACTS[name].path


def manifest_path(root: str | Path, name: str) -> Path:
    """Sibling JSON describing a table: <path>.meta.json."""
    p = contract_path(root, name)
    return p.with_name(p.name + ".meta.json")


# Required keys in every <table>.meta.json written by a producer.
META_KEYS = ("contract_version", "producer", "generated_at_utc", "is_fixture")


def write_table(df: pd.DataFrame, root: str | Path, name: str, meta: dict) -> Path:
    """Validate then write a contract table as <dir>/part-00000.parquet + meta.

    Raises ValueError on any contract violation so invalid outputs are never
    published. Spark producers may write the directory themselves but must
    still emit the sibling meta JSON.
    """
    import json
    from datetime import datetime, timezone

    problems = validate(df, name)
    if problems:
        raise ValueError("; ".join(problems))
    out = contract_path(root, name)
    out.mkdir(parents=True, exist_ok=True)
    for old in out.glob("*.parquet"):
        old.unlink()
    df.to_parquet(out / "part-00000.parquet", index=False)
    full = {
        "contract_version": CONTRACT_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),  # noqa: UP017
        "is_fixture": False,
        "rows": len(df),
        **meta,
    }
    missing = [k for k in META_KEYS if k not in full]
    if missing:
        raise ValueError(f"{name}: meta missing keys {missing}")
    with open(manifest_path(root, name), "w", encoding="utf-8") as f:
        json.dump(full, f, indent=2, default=str)
    return out
