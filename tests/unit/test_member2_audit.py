"""Member 2 Phase 3 unit tests: labels, flag reconciliation, config (no Spark)."""

from __future__ import annotations

import pandas as pd
import pytest

from src.evaluation import quality_audit as qa
from src.evaluation.member2_config import member2_config
from src.modeling import resolution as res
from tests.member2_synthetic import EXPECTED_LABELS, label_cases, to_pandas


def test_closure_label_edge_cases_pandas():
    out = res.add_closure_label_pandas(to_pandas(label_cases()))
    got = {
        k: (s, None if pd.isna(v) else int(v))
        for k, s, v in zip(out["unique_key"], out["label_status"], out["closed_within_7d"])
    }
    assert got == EXPECTED_LABELS


def test_label_summary_counts():
    out = res.label_summary_pandas(res.add_closure_label_pandas(to_pandas(label_cases())))
    by = out.set_index("label_status")
    assert by.loc["eligible", "rows"] == 7 and by.loc["eligible", "positives"] == 4
    assert by.loc["excluded_negative_duration", "rows"] == 1
    assert by.loc["excluded_immature", "rows"] == 1


def test_flag_reconciliation_sums():
    rec = qa.flag_reconciliation_pandas(to_pandas(label_cases()))
    s = rec["summary"]
    assert s["checks"]["flagged_plus_clean_equals_total"]
    assert s["checks"]["combos_weighted_by_size_equal_per_flag_sum"]
    per = dict(zip(rec["per_flag"]["flag"], rec["per_flag"]["rows"]))
    # unresolved x2, negative_duration x1, invalid_zip x1, missing_borough x1,
    # invalid_coords x1 -> 6 flag occurrences over 4 flagged rows.
    assert per == {"unresolved": 2, "negative_duration": 1, "invalid_zip": 1,
                   "missing_borough": 1, "invalid_coords": 1}
    assert s["flagged_rows"] == 4 and s["sum_per_flag_counts"] == 6
    assert s["excess_from_overlap"] == 2 and s["clean_rows"] == 5
    combos = dict(zip(rec["combinations"]["combination"], rec["combinations"]["rows"]))
    assert combos == {"(none)": 5, "invalid_zip + unresolved": 1,
                      "missing_borough + unresolved": 1, "negative_duration": 1,
                      "invalid_coords": 1}
    pair = rec["pairwise"]
    assert sorted(zip(pair["flag_a"], pair["flag_b"], pair["rows"])) == [
        ("invalid_zip", "unresolved", 1), ("missing_borough", "unresolved", 1)]


def test_duration_classes_cover_all_rows():
    pdf = to_pandas(label_cases())
    out = qa.duration_classes_pandas(pdf)
    assert int(out["rows"].sum()) == len(pdf)
    by = dict(zip(out["duration_class"], out["rows"]))
    assert by["no_closed_at"] == 2 and by["negative"] == 1 and by["zero"] == 1


def test_resolution_summary_pandas_counts():
    lab = res.add_closure_label_pandas(to_pandas(label_cases()))
    out = res.resolution_summary_pandas(lab)
    assert int(out["total_count"].sum()) == len(lab)
    assert int(out["eligible_count"].sum()) == 7
    assert int(out["closed_7d_count"].sum()) == 4
    assert int(out["unresolved_count"].sum()) == 2
    assert (out["closure_rate_7d"].dropna().between(0, 1)).all()


def test_frames_equal_handles_null_keys_and_float_tolerance():
    a = pd.DataFrame({"k": [None, "x"], "v": [1.0, 2.0]})
    b = pd.DataFrame({"k": [float("nan"), "x"], "v": [1.0 + 1e-12, 2.0]})
    assert qa.frames_equal(a, b)
    assert not qa.frames_equal(a, b.assign(v=[1.5, 2.0]))


def test_member2_config_defaults_and_validation():
    cfg = member2_config({"etl_output": "data/processed/x"})
    assert cfg["input"] == "data/processed/x"
    assert cfg["support_grid"] == sorted(cfg["support_grid"])
    with pytest.raises(ValueError):
        member2_config({"member2": {"min_support": 0}})
    with pytest.raises(ValueError):
        member2_config({"member2": {"not_a_key": 1}})
    with pytest.raises(ValueError):
        member2_config({"member2": {"basket_granularity": "month"}})
