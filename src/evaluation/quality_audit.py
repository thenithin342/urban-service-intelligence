"""Member 2 Phase 3 audit: quality-flag reconciliation and resolution states. ASCII only.

Every audit has a Spark implementation (used on the full table) and an
independent pandas reference (used for consistency checks on bounded
samples). Both return plain pandas frames with identical columns and a
deterministic row order so they can be compared exactly.

Flag semantics (docs/data_dictionary.md): a record carries an array of
quality flags. The 2,715-style "flagged records" figure counts records with
at least one flag; per-flag counts sum to more than that because flags
overlap. The *exclusive combination* table assigns every flagged record to
exactly one sorted flag set, so its counts sum to the flagged-record total.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

COMBO_SEP = " + "
NO_FLAGS = "(none)"

# Resolution-duration classes (hours); upper bounds inclusive.
DURATION_CLASSES = [
    ("no_closed_at", None),
    ("negative", None),
    ("zero", 0.0),
    ("le_1h", 1.0),
    ("le_24h", 24.0),
    ("le_7d", 168.0),
    ("le_30d", 720.0),
    ("gt_30d", None),
]


# ----------------------------------------------------------------------------
# Flag reconciliation
# ----------------------------------------------------------------------------


def flag_counts_spark(df: DataFrame) -> pd.DataFrame:
    rows = (
        df.select(F.explode("data_quality_flags").alias("flag"))
        .groupBy("flag")
        .count()
        .orderBy(F.col("count").desc(), "flag")
        .collect()
    )
    return pd.DataFrame(
        {"flag": [r["flag"] for r in rows], "rows": [int(r["count"]) for r in rows]}
    )


def flag_combinations_spark(df: DataFrame) -> pd.DataFrame:
    combo = F.when(
        F.size("data_quality_flags") > 0,
        F.concat_ws(COMBO_SEP, F.array_sort(F.array_distinct("data_quality_flags"))),
    ).otherwise(F.lit(NO_FLAGS))
    rows = (
        df.select(combo.alias("combination"),
                  F.size(F.array_distinct("data_quality_flags")).alias("n_flags"))
        .groupBy("combination", "n_flags")
        .count()
        .orderBy(F.col("count").desc(), "combination")
        .collect()
    )
    return pd.DataFrame(
        {
            "combination": [r["combination"] for r in rows],
            "n_flags": [int(r["n_flags"]) for r in rows],
            "rows": [int(r["count"]) for r in rows],
        }
    )


def _pairwise_from_combos(combos: pd.DataFrame) -> pd.DataFrame:
    """Rows carrying both flags of each pair, derived from exclusive combos."""
    counts: dict[tuple[str, str], int] = {}
    for combo, rows in zip(combos["combination"], combos["rows"]):
        if combo == NO_FLAGS:
            continue
        flags = combo.split(COMBO_SEP)
        for i, a in enumerate(flags):
            for b in flags[i + 1:]:
                counts[(a, b)] = counts.get((a, b), 0) + int(rows)
    out = pd.DataFrame(
        [(a, b, n) for (a, b), n in counts.items()], columns=["flag_a", "flag_b", "rows"]
    )
    if out.empty:
        return out.astype({"rows": "int64"})
    return out.sort_values(["rows", "flag_a", "flag_b"], ascending=[False, True, True],
                           ignore_index=True)


def flag_combinations_pandas(pdf: pd.DataFrame) -> pd.DataFrame:
    def key(flags: Any) -> tuple[str, int]:
        uniq = sorted(set(flags)) if flags is not None else []
        return (COMBO_SEP.join(uniq) if uniq else NO_FLAGS, len(uniq))

    keys = pdf["data_quality_flags"].map(key)
    frame = pd.DataFrame({"combination": keys.map(lambda k: k[0]),
                          "n_flags": keys.map(lambda k: k[1])})
    out = frame.groupby(["combination", "n_flags"]).size().reset_index(name="rows")
    out["rows"] = out["rows"].astype("int64")
    out["n_flags"] = out["n_flags"].astype("int64")
    return out.sort_values(["rows", "combination"], ascending=[False, True],
                           ignore_index=True)


def flag_counts_pandas(pdf: pd.DataFrame) -> pd.DataFrame:
    exploded = pdf["data_quality_flags"].explode().dropna()
    out = exploded.value_counts().rename_axis("flag").reset_index(name="rows")
    out["rows"] = out["rows"].astype("int64")
    return out.sort_values(["rows", "flag"], ascending=[False, True], ignore_index=True)


def reconcile_flags(per_flag: pd.DataFrame, combos: pd.DataFrame,
                    total_rows: int) -> dict[str, Any]:
    """Arithmetic reconciliation of per-flag, combination and row totals."""
    flagged = combos.loc[combos["combination"] != NO_FLAGS]
    flagged_rows = int(flagged["rows"].sum())
    clean_rows = int(combos.loc[combos["combination"] == NO_FLAGS, "rows"].sum())
    sum_per_flag = int(per_flag["rows"].sum())
    weighted = int((flagged["rows"] * flagged["n_flags"]).sum())
    return {
        "total_rows": int(total_rows),
        "flagged_rows": flagged_rows,
        "clean_rows": clean_rows,
        "sum_per_flag_counts": sum_per_flag,
        "excess_from_overlap": sum_per_flag - flagged_rows,
        "rows_by_n_flags": {
            int(k): int(v) for k, v in flagged.groupby("n_flags")["rows"].sum().items()
        },
        "checks": {
            "flagged_plus_clean_equals_total": flagged_rows + clean_rows == total_rows,
            "combos_weighted_by_size_equal_per_flag_sum": weighted == sum_per_flag,
        },
    }


def flag_reconciliation_spark(df: DataFrame) -> dict[str, Any]:
    total = df.count()
    per_flag = flag_counts_spark(df)
    combos = flag_combinations_spark(df)
    return {
        "per_flag": per_flag,
        "combinations": combos,
        "pairwise": _pairwise_from_combos(combos),
        "summary": reconcile_flags(per_flag, combos, total),
    }


def flag_reconciliation_pandas(pdf: pd.DataFrame) -> dict[str, Any]:
    per_flag = flag_counts_pandas(pdf)
    combos = flag_combinations_pandas(pdf)
    return {
        "per_flag": per_flag,
        "combinations": combos,
        "pairwise": _pairwise_from_combos(combos),
        "summary": reconcile_flags(per_flag, combos, len(pdf)),
    }


# ----------------------------------------------------------------------------
# Resolution / closure-state audit
# ----------------------------------------------------------------------------


def _duration_hours_spark() -> Any:
    return (F.col("closed_at").cast("long") - F.col("created_at").cast("long")) / 3600.0


def _duration_class_spark() -> Any:
    d = _duration_hours_spark()
    return (
        F.when(F.col("closed_at").isNull() | F.col("created_at").isNull(), "no_closed_at")
        .when(d < 0, "negative")
        .when(d == 0, "zero")
        .when(d <= 1, "le_1h")
        .when(d <= 24, "le_24h")
        .when(d <= 168, "le_7d")
        .when(d <= 720, "le_30d")
        .otherwise("gt_30d")
    )


def _duration_class_pandas(pdf: pd.DataFrame) -> pd.Series:
    d = (pdf["closed_at"] - pdf["created_at"]).dt.total_seconds() / 3600.0
    out = pd.Series("gt_30d", index=pdf.index, dtype=object)
    out[d <= 720] = "le_30d"
    out[d <= 168] = "le_7d"
    out[d <= 24] = "le_24h"
    out[d <= 1] = "le_1h"
    out[d == 0] = "zero"
    out[d < 0] = "negative"
    out[pdf["closed_at"].isna() | pdf["created_at"].isna()] = "no_closed_at"
    return out


def _order_classes(frame: pd.DataFrame, col: str) -> pd.DataFrame:
    order = {name: i for i, (name, _) in enumerate(DURATION_CLASSES)}
    frame = frame.assign(_o=frame[col].map(order)).sort_values(["_o"], ignore_index=True)
    return frame.drop(columns="_o")


def status_closure_spark(df: DataFrame) -> pd.DataFrame:
    rows = (
        df.groupBy(
            F.coalesce("status_snapshot", F.lit("NULL")).alias("status_snapshot"),
            F.col("closed_at").isNotNull().alias("has_closed_at"),
        )
        .count()
        .orderBy("status_snapshot", "has_closed_at")
        .collect()
    )
    return pd.DataFrame(
        {
            "status_snapshot": [r["status_snapshot"] for r in rows],
            "has_closed_at": [bool(r["has_closed_at"]) for r in rows],
            "rows": [int(r["count"]) for r in rows],
        }
    )


def status_closure_pandas(pdf: pd.DataFrame) -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "status_snapshot": pdf["status_snapshot"].fillna("NULL"),
            "has_closed_at": pdf["closed_at"].notna(),
        }
    )
    out = frame.groupby(["status_snapshot", "has_closed_at"]).size().reset_index(name="rows")
    out["rows"] = out["rows"].astype("int64")
    return out.sort_values(["status_snapshot", "has_closed_at"], ignore_index=True)


def duration_classes_spark(df: DataFrame) -> pd.DataFrame:
    rows = df.groupBy(_duration_class_spark().alias("duration_class")).count().collect()
    out = pd.DataFrame(
        {"duration_class": [r["duration_class"] for r in rows],
         "rows": [int(r["count"]) for r in rows]}
    )
    return _order_classes(out, "duration_class")


def duration_classes_pandas(pdf: pd.DataFrame) -> pd.DataFrame:
    out = _duration_class_pandas(pdf).value_counts().rename_axis(
        "duration_class").reset_index(name="rows")
    out["rows"] = out["rows"].astype("int64")
    return _order_classes(out, "duration_class")


QUANTILES = (0.25, 0.5, 0.75, 0.9, 0.99)


def resolution_quantiles_spark(df: DataFrame) -> dict[str, float | int | None]:
    """Exact (interpolated) quantiles of the cleaned ``resolution_hours``."""
    qs = ", ".join(str(q) for q in QUANTILES)
    row = df.filter(F.col("resolution_hours").isNotNull()).agg(
        F.count(F.lit(1)).alias("n"),
        F.mean("resolution_hours").alias("mean"),
        F.max("resolution_hours").alias("max"),
        F.expr(f"percentile(resolution_hours, array({qs}))").alias("q"),
    ).collect()[0]
    out: dict[str, float | int | None] = {"n": int(row["n"]), "mean": row["mean"],
                                          "max": row["max"]}
    for q, v in zip(QUANTILES, row["q"] or [None] * len(QUANTILES)):
        out[f"p{int(q * 100)}"] = v
    return out


def resolution_quantiles_pandas(pdf: pd.DataFrame) -> dict[str, float | int | None]:
    s = pdf["resolution_hours"].dropna()
    out: dict[str, float | int | None] = {
        "n": len(s),
        "mean": float(s.mean()) if len(s) else None,
        "max": float(s.max()) if len(s) else None,
    }
    for q in QUANTILES:
        out[f"p{int(q * 100)}"] = float(s.quantile(q)) if len(s) else None
    return out


def backlog_by_borough_spark(df: DataFrame) -> pd.DataFrame:
    rows = (
        df.groupBy(F.coalesce("borough", F.lit("UNKNOWN")).alias("borough"))
        .agg(
            F.count(F.lit(1)).alias("total_count"),
            F.sum(F.col("closed_at").isNull().cast("int")).alias("unresolved_count"),
            F.sum((F.coalesce("status_snapshot", F.lit("")) != "Closed").cast("int")).alias(
                "status_not_closed_count"),
        )
        .orderBy("borough")
        .collect()
    )
    out = pd.DataFrame([r.asDict() for r in rows])
    for c in ("total_count", "unresolved_count", "status_not_closed_count"):
        out[c] = out[c].astype("int64")
    out["unresolved_share"] = out["unresolved_count"] / out["total_count"]
    return out


def backlog_by_borough_pandas(pdf: pd.DataFrame) -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "borough": pdf["borough"].fillna("UNKNOWN"),
            "unresolved": pdf["closed_at"].isna().astype("int64"),
            "not_closed": (pdf["status_snapshot"].fillna("") != "Closed").astype("int64"),
        }
    )
    out = frame.groupby("borough").agg(
        total_count=("unresolved", "size"),
        unresolved_count=("unresolved", "sum"),
        status_not_closed_count=("not_closed", "sum"),
    ).reset_index().sort_values("borough", ignore_index=True)
    for c in ("total_count", "unresolved_count", "status_not_closed_count"):
        out[c] = out[c].astype("int64")
    out["unresolved_share"] = out["unresolved_count"] / out["total_count"]
    return out


def resolution_audit_spark(df: DataFrame) -> dict[str, Any]:
    snap = df.agg(F.min("data_snapshot_date").alias("lo"),
                  F.max("data_snapshot_date").alias("hi"),
                  F.max("closed_at").alias("closed_max")).collect()[0]
    after_snapshot = df.filter(
        F.to_date("closed_at") > F.col("data_snapshot_date")).count()
    return {
        "status_closure": status_closure_spark(df),
        "duration_classes": duration_classes_spark(df),
        "backlog_by_borough": backlog_by_borough_spark(df),
        "quantiles": resolution_quantiles_spark(df),
        "snapshot_min": str(snap["lo"]),
        "snapshot_max": str(snap["hi"]),
        "closed_at_max": str(snap["closed_max"]),
        "closed_after_snapshot_rows": int(after_snapshot),
    }


def resolution_audit_pandas(pdf: pd.DataFrame) -> dict[str, Any]:
    return {
        "status_closure": status_closure_pandas(pdf),
        "duration_classes": duration_classes_pandas(pdf),
        "backlog_by_borough": backlog_by_borough_pandas(pdf),
        "quantiles": resolution_quantiles_pandas(pdf),
    }


def frames_equal(a: pd.DataFrame, b: pd.DataFrame, float_tol: float = 1e-9) -> bool:
    """Exact equality for keys/ints, tolerance for floats (same row order)."""
    if list(a.columns) != list(b.columns) or len(a) != len(b):
        return False
    for col in a.columns:
        x, y = a[col].reset_index(drop=True), b[col].reset_index(drop=True)
        if pd.api.types.is_float_dtype(x) or pd.api.types.is_float_dtype(y):
            if not ((x - y).abs().fillna(0) <= float_tol).all():
                return False
            if not (x.isna() == y.isna()).all():
                return False
        else:
            xs = x.astype(object).where(x.notna(), "<NULL>").astype(str)
            ys = y.astype(object).where(y.notna(), "<NULL>").astype(str)
            if not (xs == ys).all():
                return False
    return True


def compare_spark_pandas(spark_out: dict[str, Any], pandas_out: dict[str, Any],
                         keys: list[str]) -> dict[str, bool]:
    """Per-table equality of Spark vs pandas audit outputs."""
    result: dict[str, bool] = {}
    for k in keys:
        a, b = spark_out[k], pandas_out[k]
        if isinstance(a, pd.DataFrame):
            result[k] = frames_equal(a, b)
        else:
            result[k] = all(
                (a[f] is None and b[f] is None)
                or (a[f] is not None and b[f] is not None and abs(float(a[f]) - float(b[f]))
                    <= 1e-6 * max(1.0, abs(float(b[f]))))
                for f in b
            )
    return result
