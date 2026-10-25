"""Member 2 closure-model unit tests: leakage audit, as-of features, split, metrics."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.evaluation import model_metrics as mm
from src.modeling import resolution as res


def _frame(n_days: int = 20, per_day: int = 30, seed: int = 0) -> pd.DataFrame:
    """Synthetic eligible records: category A closes ~90%, B ~30%."""
    rng = np.random.default_rng(seed)
    rows = []
    for d in range(n_days):
        day = pd.Timestamp("2024-01-01") + pd.Timedelta(days=d)
        for i in range(per_day):
            cat = "A" if i % 2 == 0 else "B"
            ts = day + pd.Timedelta(minutes=int(rng.integers(0, 1440)))
            rows.append({
                "unique_key": f"k{d:03d}{i:03d}",
                "created_at": ts,
                "complaint_type": cat,
                "agency": "NYPD" if cat == "A" else "HPD",
                "borough": None if i == 1 else ("BRONX" if i % 3 else "QUEENS"),
                "incident_zip": None if i == 5 else f"1040{i % 4}",
                "hour": ts.hour,
                "weekday": ts.day_name(),
                "created_day": ts.date(),
                "closed_within_7d": int(rng.random() < (0.9 if cat == "A" else 0.3)),
            })
    return pd.DataFrame(rows).sort_values(["created_at", "unique_key"], ignore_index=True)


def test_feature_list_has_no_forbidden_columns():
    assert not set(res.FEATURE_COLUMNS) & res.FORBIDDEN_FEATURES
    for col in ("closed_at", "status_snapshot", "resolution_hours", "data_snapshot_date"):
        assert col in res.FORBIDDEN_FEATURES
        with pytest.raises(ValueError):
            res.assert_no_leakage([*res.FEATURE_COLUMNS, col])
    assert not set(res.MODEL_INPUT_COLUMNS) & (res.FORBIDDEN_FEATURES - {"closed_within_7d"})


def test_asof_rate_uses_only_records_mature_before_prediction_time():
    t = pd.Series(pd.to_datetime(["2024-01-01 00:00:00", "2024-01-02 00:00:00",
                                  "2024-01-08 00:00:00", "2024-01-09 00:00:01"]))
    key = pd.Series(["A", "A", "A", "A"])
    y = pd.Series([1, 0, 1, 1])
    rate, n = res.asof_rate(t, key, y, horizon_days=7, alpha=0.0, fallback=0.5)
    # rec 2 (Jan 8 00:00) sees rec 0 (exactly 7 days earlier) only;
    # rec 3 (Jan 9 00:00:01) sees rec 0 and rec 1.
    assert n.tolist() == [0, 0, 1, 2]
    assert rate.iloc[2] == pytest.approx(1.0) and rate.iloc[3] == pytest.approx(0.5)
    assert np.isnan(rate.iloc[0]) or rate.iloc[0] == pytest.approx(0.5)


def test_asof_rate_unit_independent_of_timestamp_resolution():
    t = pd.Series(pd.to_datetime(["2024-01-01", "2024-01-09"]))
    key, y = pd.Series(["A", "A"]), pd.Series([1, 0])
    a = res.asof_rate(t, key, y, 7, 1.0, 0.5)
    b = res.asof_rate(t.astype("datetime64[us]"), key, y, 7, 1.0, 0.5)
    assert a[1].tolist() == b[1].tolist() == [0, 1]
    assert np.allclose(a[0], b[0])


def test_asof_features_ignore_future_labels():
    frame = _frame()
    base = res.add_features(frame, 7, 20.0, 0.6)
    cutoff = pd.Timestamp("2024-01-12")
    future = frame["created_at"] >= cutoff
    flipped = frame.copy()
    flipped.loc[future, "closed_within_7d"] = 1 - flipped.loc[future, "closed_within_7d"]
    alt = res.add_features(flipped, 7, 20.0, 0.6)
    # Records created before cutoff + 7 days cannot see any flipped label.
    safe = frame["created_at"] < cutoff + pd.Timedelta(days=7)
    for col in ("asof_category_rate", "asof_agency_rate", "asof_category_log_n"):
        assert np.allclose(base.loc[safe, col], alt.loc[safe, col])
    assert not np.allclose(base.loc[~safe, "asof_category_rate"],
                           alt.loc[~safe, "asof_category_rate"])


def test_chronological_split_with_embargo():
    frame = _frame()
    s = res.chronological_split(frame, "2024-01-15", embargo_days=7, validation_days=3)
    day = {k: pd.to_datetime(v["created_day"]) for k, v in s.items()}
    assert day["train"].max() < pd.Timestamp("2024-01-08")
    assert day["test"].min() == pd.Timestamp("2024-01-15")
    assert day["validation"].min() == pd.Timestamp("2024-01-05")
    assert len(s["fit"]) + len(s["validation"]) == len(s["train"])
    assert not set(s["train"]["unique_key"]) & set(s["test"]["unique_key"])


def test_models_fit_and_beat_prior_on_separable_synthetic_data():
    frame = res.add_features(_frame(), 7, 20.0, 0.6)
    s = res.chronological_split(frame, "2024-01-15", 0, 3)
    enc = res.FittedEncoders(s["train"], 10)
    tr, te = enc.transform(s["train"]), enc.transform(s["test"])
    briers = {}
    for name in res.MODEL_NAMES:
        p = res.make_model(name, 20.0, 42).fit(tr).predict_proba(te)
        assert np.all((p >= 0) & (p <= 1)) and len(p) == len(te)
        briers[name] = mm.binary_metrics(te["closed_within_7d"], p)["brier"]
    for name in ("category_rate", "asof_category_rate", "logistic_regression",
                 "gradient_boosting"):
        assert briers[name] < briers["prior_rate"]


def test_cohort_predictions_expected_open_is_sum_of_one_minus_p():
    frame = res.add_features(_frame(n_days=3), 7, 20.0, 0.6)
    p = np.linspace(0.1, 0.9, len(frame))
    out = res.cohort_predictions(frame, p, "2024-01-01", "2024-01-03", "v", 30)
    from src.utils import contracts

    assert contracts.validate(out, "closure_cohort_predictions") == []
    assert out["n_complaints"].sum() == len(frame)
    assert out["expected_open_after_7d"].sum() == pytest.approx((1 - p).sum())
    assert out["borough"].isna().any()  # null borough kept as its own cohort
    assert (out["small_sample"] == (out["n_complaints"] < 30)).all()


def test_binary_metrics_hand_computed():
    y = np.array([1, 1, 0, 0])
    p = np.array([0.9, 0.4, 0.6, 0.1])
    m = mm.binary_metrics(y, p)
    assert m["brier"] == pytest.approx((0.01 + 0.36 + 0.36 + 0.01) / 4)
    assert m["precision_at_05"] == pytest.approx(0.5)
    assert m["recall_at_05"] == pytest.approx(0.5)
    assert m["roc_auc"] == pytest.approx(0.75)
    single = mm.binary_metrics(np.ones(3), np.full(3, 0.8))
    assert single["pr_auc"] is None and single["roc_auc"] is None


def test_calibration_table_and_group_metrics():
    y = np.array([0, 1, 1, 1, 0, 1])
    p = np.array([0.05, 0.15, 0.95, 0.99, 0.5, 1.0])
    cal = mm.calibration_table(y, p, n_bins=10)
    assert cal["n"].sum() == len(y) and len(cal) == 10
    assert cal.loc[9, "n"] == 3 and cal.loc[9, "observed_rate"] == pytest.approx(1.0)
    frame = pd.DataFrame({"g": ["a", "a", "b", "b", "b", None], "y": y, "p": p,
                          "b0": np.full(6, 0.5)})
    out = mm.group_metrics(frame, "g", "y", "p", "b0", min_n=3)
    assert out["n"].sum() == 6 and out["small_sample"].tolist() == [False, True, True]
