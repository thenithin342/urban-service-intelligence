"""Member 2 closure-within-7-days labels, summaries and models. ASCII only.

Label (docs/member2/semantics.md):
- prediction time = ``created_at``;
- ``closed_within_7d`` = 1 when ``closed_at`` is not null, ``closed_at >=
  created_at`` and ``closed_at - created_at <= horizon``; 0 otherwise;
- negative durations (``closed_at < created_at``) are excluded and counted;
- censoring: a record is label-eligible only when the snapshot leaves at
  least ``horizon`` full days of observation after its created day
  (``data_snapshot_date - created_day >= horizon_days + 1``); younger
  records are excluded (never labelled 0);
- missing ``created_at`` is excluded.

Durations use epoch-second differences, so they do not depend on the
session time zone. Calendar keys use the ETL's wall-clock ``created_day``.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

ELIGIBLE = "eligible"
EXCL_MISSING_CREATED = "excluded_missing_created"
EXCL_NEGATIVE = "excluded_negative_duration"
EXCL_IMMATURE = "excluded_immature"
LABEL_STATUSES = (ELIGIBLE, EXCL_MISSING_CREATED, EXCL_NEGATIVE, EXCL_IMMATURE)

# Columns that describe the outcome or the acquisition snapshot. They may be
# used to build labels and strictly-historical aggregates, never as features
# of the record being predicted.
FORBIDDEN_FEATURES = frozenset(
    {
        "closed_at",
        "closed_date",
        "status_snapshot",
        "status",
        "resolution_hours",
        "data_snapshot_date",
        "data_quality_flags",
        "closed_within_7d",
        "label_status",
        "duration_hours",
    }
)


def add_closure_label_spark(df: DataFrame, horizon_days: int = 7) -> DataFrame:
    """Append ``duration_hours``, ``label_status`` and ``closed_within_7d``."""
    dur = (F.col("closed_at").cast("long") - F.col("created_at").cast("long")) / 3600.0
    mature = F.datediff(F.col("data_snapshot_date"), F.col("created_day")) >= horizon_days + 1
    status = (
        F.when(F.col("created_at").isNull() | F.col("created_day").isNull(),
               EXCL_MISSING_CREATED)
        .when(F.col("closed_at").isNotNull() & (dur < 0), EXCL_NEGATIVE)
        .when(~mature, EXCL_IMMATURE)
        .otherwise(ELIGIBLE)
    )
    label = F.when(
        status == ELIGIBLE,
        (F.col("closed_at").isNotNull() & (dur >= 0) & (dur <= horizon_days * 24.0)).cast(
            "int"),
    )
    return (
        df.withColumn("duration_hours", dur)
        .withColumn("label_status", status)
        .withColumn("closed_within_7d", label)
    )


def add_closure_label_pandas(pdf: pd.DataFrame, horizon_days: int = 7) -> pd.DataFrame:
    """Independent pandas implementation of ``add_closure_label_spark``."""
    out = pdf.copy()
    dur = (out["closed_at"] - out["created_at"]).dt.total_seconds() / 3600.0
    created_day = pd.to_datetime(out["created_day"])
    snap = pd.to_datetime(out["data_snapshot_date"])
    mature = (snap - created_day).dt.days >= horizon_days + 1
    status = pd.Series(ELIGIBLE, index=out.index, dtype=object)
    status[~mature.fillna(False)] = EXCL_IMMATURE
    status[out["closed_at"].notna() & (dur < 0)] = EXCL_NEGATIVE
    status[out["created_at"].isna() | out["created_day"].isna()] = EXCL_MISSING_CREATED
    positive = out["closed_at"].notna() & (dur >= 0) & (dur <= horizon_days * 24.0)
    label = positive.astype("Int64").where(status == ELIGIBLE)
    out["duration_hours"] = dur
    out["label_status"] = status
    out["closed_within_7d"] = label
    return out


def label_summary_spark(labelled: DataFrame) -> pd.DataFrame:
    rows = (
        labelled.groupBy("label_status")
        .agg(F.count(F.lit(1)).alias("rows"),
             F.sum(F.coalesce("closed_within_7d", F.lit(0))).alias("positives"))
        .orderBy("label_status")
        .collect()
    )
    out = pd.DataFrame([r.asDict() for r in rows], columns=["label_status", "rows", "positives"])
    return out.astype({"rows": "int64", "positives": "int64"})


def label_summary_pandas(labelled: pd.DataFrame) -> pd.DataFrame:
    frame = pd.DataFrame({"label_status": labelled["label_status"],
                          "pos": labelled["closed_within_7d"].fillna(0).astype("int64")})
    out = frame.groupby("label_status").agg(rows=("pos", "size"), positives=("pos", "sum"))
    out = out.reset_index().sort_values("label_status", ignore_index=True)
    return out.astype({"rows": "int64", "positives": "int64"})


def resolution_summary_spark(labelled: DataFrame) -> DataFrame:
    """Contract ``resolution_summary``: borough x complaint_type closure stats.

    ``median``/``p90_resolution_hours`` are exact interpolated percentiles
    over the cleaned ``resolution_hours`` (valid closures only);
    ``unresolved_count`` counts records without ``closed_at`` at snapshot.
    """
    elig = F.col("label_status") == ELIGIBLE
    cov = labelled.agg(F.min("created_day").alias("lo"), F.max("created_day").alias("hi"))
    grouped = labelled.groupBy("borough", "complaint_type").agg(
        F.count(F.lit(1)).cast("long").alias("total_count"),
        F.sum(elig.cast("long")).alias("eligible_count"),
        F.sum(F.when(elig, F.col("closed_within_7d")).cast("long")).alias("closed_7d_count"),
        F.expr("percentile(resolution_hours, 0.5)").alias("median_resolution_hours"),
        F.expr("percentile(resolution_hours, 0.9)").alias("p90_resolution_hours"),
        F.sum(F.col("closed_at").isNull().cast("long")).alias("unresolved_count"),
    )
    return (
        grouped.crossJoin(cov)
        .select(
            "borough",
            "complaint_type",
            "total_count",
            "eligible_count",
            F.coalesce("closed_7d_count", F.lit(0)).cast("long").alias("closed_7d_count"),
            F.when(F.col("eligible_count") > 0,
                   F.coalesce("closed_7d_count", F.lit(0)) / F.col("eligible_count"))
            .alias("closure_rate_7d"),
            "median_resolution_hours",
            "p90_resolution_hours",
            "unresolved_count",
            F.col("lo").alias("coverage_start"),
            F.col("hi").alias("coverage_end"),
        )
        .orderBy(F.col("total_count").desc(), F.col("borough").asc_nulls_last(),
                 "complaint_type")
    )


def resolution_summary_pandas(labelled: pd.DataFrame) -> pd.DataFrame:
    """Independent pandas reference for ``resolution_summary_spark``."""
    elig = labelled["label_status"] == ELIGIBLE
    frame = pd.DataFrame(
        {
            "borough": labelled["borough"],
            "complaint_type": labelled["complaint_type"],
            "elig": elig.astype("int64"),
            "pos": labelled["closed_within_7d"].where(elig).fillna(0).astype("int64"),
            "rh": labelled["resolution_hours"],
            "unres": labelled["closed_at"].isna().astype("int64"),
        }
    )
    g = frame.groupby(["borough", "complaint_type"], dropna=False)
    out = g.agg(
        total_count=("elig", "size"),
        eligible_count=("elig", "sum"),
        closed_7d_count=("pos", "sum"),
        median_resolution_hours=("rh", "median"),
        p90_resolution_hours=("rh", lambda s: s.quantile(0.9)),
        unresolved_count=("unres", "sum"),
    ).reset_index()
    out["closure_rate_7d"] = (out["closed_7d_count"] / out["eligible_count"]).where(
        out["eligible_count"] > 0)
    days = pd.to_datetime(labelled["created_day"])
    out["coverage_start"] = days.min().date()
    out["coverage_end"] = days.max().date()
    out = out[
        ["borough", "complaint_type", "total_count", "eligible_count", "closed_7d_count",
         "closure_rate_7d", "median_resolution_hours", "p90_resolution_hours",
         "unresolved_count", "coverage_start", "coverage_end"]
    ]
    out = out.astype({c: "int64" for c in ("total_count", "eligible_count",
                                           "closed_7d_count", "unresolved_count")})
    out["_b"] = out["borough"].isna()
    out = out.sort_values(["total_count", "_b", "borough", "complaint_type"],
                          ascending=[False, True, True, True], ignore_index=True)
    return out.drop(columns="_b")


# ----------------------------------------------------------------------------
# Modeling (bounded pandas + scikit-learn)
# ----------------------------------------------------------------------------

MODEL_INPUT_COLUMNS = [
    "unique_key",
    "created_at",
    "complaint_type",
    "agency",
    "borough",
    "incident_zip",
    "hour",
    "weekday",
    "created_day",
    "closed_within_7d",
]

CATEGORICAL_FEATURES = ["complaint_type_top", "agency", "borough", "weekday"]
NUMERIC_FEATURES = [
    "hour_sin",
    "hour_cos",
    "zip_log_freq",
    "zip_missing",
    "asof_category_rate",
    "asof_category_log_n",
    "asof_agency_rate",
    "asof_agency_log_n",
]
FEATURE_COLUMNS = CATEGORICAL_FEATURES + NUMERIC_FEATURES
OTHER = "OTHER"
UNKNOWN = "UNKNOWN"


def assert_no_leakage(features: list[str]) -> None:
    bad = sorted(set(features) & FORBIDDEN_FEATURES)
    if bad:
        raise ValueError(f"forbidden (outcome/snapshot) columns used as features: {bad}")


def asof_rate(created_at: pd.Series, key: pd.Series, label: pd.Series,
              horizon_days: int, alpha: float, fallback: float) -> tuple[pd.Series, pd.Series]:
    """Strictly historical closure rate per key at each record's created_at.

    For record i only records j with the same key and
    ``created_at_j + horizon <= created_at_i`` contribute, so the full
    horizon outcome of j was observable at the prediction time of i. The
    rate is Beta-smoothed toward the as-of global rate (computed the same
    way), or toward ``fallback`` when no global history exists yet.
    Returns (smoothed_rate, history_count).
    """
    # Normalise the unit first: Spark/Arrow may hand back datetime64[us].
    t = pd.to_datetime(created_at).astype("datetime64[ns]").astype("int64").to_numpy()
    h = np.int64(horizon_days) * np.int64(86400) * np.int64(10**9)
    y = label.astype(float).to_numpy()
    k = key.fillna(UNKNOWN).astype(str).to_numpy()

    order = np.argsort(t, kind="mergesort")
    t_sorted, y_sorted = t[order], y[order]
    cum = np.concatenate([[0.0], np.cumsum(y_sorted)])
    g_n = np.searchsorted(t_sorted, t - h, side="right")
    g_rate = np.where(g_n > 0, cum[g_n] / np.maximum(g_n, 1), fallback)

    n_hist = np.zeros(len(t), dtype=np.int64)
    pos_hist = np.zeros(len(t), dtype=float)
    for idx in pd.Series(np.arange(len(k))).groupby(k, sort=False).indices.values():
        tk = t[idx]
        o = np.argsort(tk, kind="mergesort")
        ck = np.concatenate([[0.0], np.cumsum(y[idx][o])])
        nk = np.searchsorted(tk[o], tk - h, side="right")
        n_hist[idx] = nk
        pos_hist[idx] = ck[nk]
    rate = (pos_hist + alpha * g_rate) / (n_hist + alpha)
    return (pd.Series(rate, index=created_at.index),
            pd.Series(n_hist, index=created_at.index))


def add_features(frame: pd.DataFrame, horizon_days: int, alpha: float,
                 fallback: float) -> pd.DataFrame:
    """Creation-time features plus strictly historical (as-of) rates.

    ``frame`` must hold every eligible labelled record (all splits), so the
    as-of history of a test record includes earlier training records. The
    as-of rates only use records created at least ``horizon_days`` earlier.
    ``borough_cohort`` keeps the original (nullable) borough for cohorts.
    """
    out = frame.copy()
    out["borough_cohort"] = out["borough"]
    out["agency"] = out["agency"].fillna(UNKNOWN)
    out["borough"] = out["borough"].fillna(UNKNOWN)
    out["weekday"] = out["weekday"].fillna(UNKNOWN)
    hour = out["hour"].astype(float)
    out["hour_sin"] = np.sin(2 * np.pi * hour / 24.0)
    out["hour_cos"] = np.cos(2 * np.pi * hour / 24.0)
    out["zip_missing"] = out["incident_zip"].isna().astype(float)
    label = out["closed_within_7d"].astype(float)
    for name, key in (("category", out["complaint_type"]), ("agency", out["agency"])):
        rate, n = asof_rate(out["created_at"], key, label, horizon_days, alpha, fallback)
        out[f"asof_{name}_rate"] = rate
        out[f"asof_{name}_log_n"] = np.log1p(n.astype(float))
    return out


class FittedEncoders:
    """Training-window statistics applied unchanged to later splits."""

    def __init__(self, train: pd.DataFrame, top_categories: int) -> None:
        vc = train["complaint_type"].value_counts()
        ranked = sorted(vc.items(), key=lambda kv: (-kv[1], kv[0]))
        self.top = [k for k, _ in ranked[:top_categories]]
        days = max(pd.to_datetime(train["created_day"]).nunique(), 1)
        self.zip_rate = (train["incident_zip"].dropna().value_counts() / days).to_dict()

    def transform(self, frame: pd.DataFrame) -> pd.DataFrame:
        out = frame.copy()
        out["complaint_type_top"] = out["complaint_type"].where(
            out["complaint_type"].isin(set(self.top)), OTHER)
        out["zip_log_freq"] = np.log1p(
            out["incident_zip"].map(self.zip_rate).astype(float).fillna(0.0))
        return out


def chronological_split(frame: pd.DataFrame, cutoff: str, embargo_days: int,
                        validation_days: int, test_end: str | None = None
                        ) -> dict[str, pd.DataFrame]:
    """train: created_day < cutoff - embargo; test: cutoff <= created_day [<= test_end].

    ``fit`` / ``validation`` split train at its last ``validation_days`` days.
    """
    day = pd.to_datetime(frame["created_day"])
    c = pd.Timestamp(cutoff)
    train_end = c - pd.Timedelta(days=embargo_days)
    val_start = train_end - pd.Timedelta(days=validation_days)
    test_mask = day >= c
    if test_end is not None:
        test_mask &= day <= pd.Timestamp(test_end)
    return {
        "train": frame[day < train_end],
        "fit": frame[day < val_start],
        "validation": frame[(day >= val_start) & (day < train_end)],
        "test": frame[test_mask],
    }


class RateModel:
    """Baselines: constant prior, per-category training rate, as-of rate."""

    def __init__(self, kind: str, alpha: float = 20.0) -> None:
        self.kind, self.alpha = kind, alpha

    def fit(self, frame: pd.DataFrame) -> RateModel:
        y = frame["closed_within_7d"].astype(float)
        self.prior = float(y.mean())
        g = frame.assign(_y=y).groupby("complaint_type")["_y"].agg(["sum", "count"])
        self.rates = ((g["sum"] + self.alpha * self.prior)
                      / (g["count"] + self.alpha)).to_dict()
        return self

    def predict_proba(self, frame: pd.DataFrame) -> np.ndarray:
        if self.kind == "prior_rate":
            return np.full(len(frame), self.prior)
        if self.kind == "asof_category_rate":
            return frame["asof_category_rate"].to_numpy(dtype=float)
        return frame["complaint_type"].map(self.rates).astype(float).fillna(
            self.prior).to_numpy(float)


def _logistic(seed: int):
    from sklearn.compose import ColumnTransformer
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder, StandardScaler

    pre = ColumnTransformer([
        ("cat", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL_FEATURES),
        ("num", StandardScaler(), NUMERIC_FEATURES),
    ])
    return Pipeline([("pre", pre),
                     ("clf", LogisticRegression(C=1.0, max_iter=2000, random_state=seed))])


def _hgb(seed: int):
    from sklearn.compose import ColumnTransformer
    from sklearn.ensemble import HistGradientBoostingClassifier
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OrdinalEncoder

    pre = ColumnTransformer([
        ("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1,
                               encoded_missing_value=-1), CATEGORICAL_FEATURES),
        ("num", "passthrough", NUMERIC_FEATURES),
    ])
    clf = HistGradientBoostingClassifier(
        max_iter=300, learning_rate=0.08, max_leaf_nodes=31, min_samples_leaf=50,
        l2_regularization=1.0, early_stopping=False, random_state=seed,
        categorical_features=list(range(len(CATEGORICAL_FEATURES))))
    return Pipeline([("pre", pre), ("clf", clf)])


class SklearnModel:
    def __init__(self, kind: str, seed: int) -> None:
        self.kind, self.seed = kind, seed

    def fit(self, frame: pd.DataFrame) -> SklearnModel:
        assert_no_leakage(FEATURE_COLUMNS)
        self.pipe = (_logistic(self.seed) if self.kind == "logistic_regression"
                     else _hgb(self.seed))
        self.pipe.fit(frame[FEATURE_COLUMNS], frame["closed_within_7d"].astype(int))
        return self

    def predict_proba(self, frame: pd.DataFrame) -> np.ndarray:
        return self.pipe.predict_proba(frame[FEATURE_COLUMNS])[:, 1]


MODEL_NAMES = ["prior_rate", "category_rate", "asof_category_rate", "logistic_regression",
               "gradient_boosting"]
BASELINES = frozenset({"prior_rate", "category_rate", "asof_category_rate"})


def make_model(name: str, alpha: float, seed: int) -> RateModel | SklearnModel:
    if name in BASELINES:
        return RateModel(name, alpha)
    if name in ("logistic_regression", "gradient_boosting"):
        return SklearnModel(name, seed)
    raise ValueError(f"unknown model {name}")


def cohort_predictions(test: pd.DataFrame, p: np.ndarray, window_start: str,
                       window_end: str, model_version: str, min_n: int) -> pd.DataFrame:
    """Contract ``closure_cohort_predictions`` (borough x complaint_type).

    ``expected_open_after_7d`` = sum(1 - p); ``observed_*`` use the mature
    labels of the same records (all test records are label-eligible).
    Additive columns: ``observed_open_after_7d``, ``small_sample``.
    """
    frame = pd.DataFrame({
        "borough": test["borough_cohort"].to_numpy(),
        "complaint_type": test["complaint_type"].to_numpy(),
        "p": np.asarray(p, float),
        "y": test["closed_within_7d"].astype(float).to_numpy(),
    })
    frame["q"] = 1.0 - frame["p"]
    frame["open"] = 1.0 - frame["y"]
    g = frame.groupby(["borough", "complaint_type"], dropna=False)
    out = g.agg(n_complaints=("p", "size"), mean_closure_probability=("p", "mean"),
                expected_open_after_7d=("q", "sum"), observed_closure_rate_7d=("y", "mean"),
                observed_open_after_7d=("open", "sum")).reset_index()
    out["n_complaints"] = out["n_complaints"].astype("int64")
    out["prediction_window_start"] = pd.Timestamp(window_start).date()
    out["prediction_window_end"] = pd.Timestamp(window_end).date()
    out["model_version"] = model_version
    out["small_sample"] = out["n_complaints"] < min_n
    out["_b"] = out["borough"].isna()
    out = out.sort_values(["expected_open_after_7d", "_b", "borough", "complaint_type"],
                          ascending=[False, True, True, True], ignore_index=True)
    return out.drop(columns="_b")
