"""Independent arithmetic and leakage checks used by the final audit."""

import json
import math
from datetime import date, timedelta

import pytest

from src.evaluation.member1_audit import (
    audit_member1,
    check_benchmark_coverage,
    check_future_rows,
    check_prediction_rows,
    independent_metrics,
)


def test_independent_metrics_and_zero_denominator():
    result = independent_metrics([{"actual_count": 10, "predicted_count": 10},
                                  {"actual_count": 10, "predicted_count": 14}])
    assert result["mae"] == 2 and result["rmse"] == math.sqrt(8) and result["wape"] == 0.2
    assert independent_metrics([{"actual_count": 0, "predicted_count": 1}])["wape"] is None


def test_audit_rejects_training_after_origin():
    row = {"model": "x", "split": "test", "forecast_origin": date(2024, 1, 1),
           "fit_cutoff": date(2024, 1, 2), "target_date": date(2024, 1, 2), "borough": "QUEENS",
           "horizon_days": 1, "actual_count": 10, "predicted_count": 10.0}
    with pytest.raises(ValueError, match="time boundary"):
        check_prediction_rows([row], {(date(2024, 1, 2), "QUEENS"): 10})


def test_audit_rejects_changed_seed_before_loading_data(tmp_path):
    out = tmp_path / "out"
    out.mkdir()
    (out / "model_manifest.json").write_text(json.dumps({"seed": 42}))
    (out / "final_audit.json").write_text('{"status": "PASS"}')
    with pytest.raises(ValueError, match="seed changed"):
        audit_member1(None, {"output_dir": "out", "seed": 43}, {}, tmp_path)
    assert not (out / "final_audit.json").exists()


def test_audit_requires_all_expected_predictions():
    with pytest.raises(ValueError, match="coverage is incomplete"):
        check_prediction_rows([], {}, {("seasonal_naive", "test", date(2024, 2, 1), "QUEENS", 1)})


def test_audit_keys_include_category():
    origin, target = date(2024, 1, 31), date(2024, 2, 1)
    rows = [{"model": "fixture", "split": "test", "forecast_origin": origin, "fit_cutoff": origin,
             "target_date": target, "borough": "QUEENS", "complaint_type": category,
             "horizon_days": 1, "actual_count": n, "predicted_count": float(n)}
            for category, n in (("A", 10), ("B", 20))]
    counts = {(target, "QUEENS", r["complaint_type"]): r["actual_count"] for r in rows}
    expected = {("fixture", "test", origin, "QUEENS", c, 1) for c in ("A", "B")}
    check_prediction_rows(rows, counts, expected)


def test_future_baseline_and_geography_are_checked_independently():
    origin = date(2024, 2, 1)
    daily = {(origin - timedelta(days=i), "QUEENS"): i for i in range(28)}
    rows = [{"borough": "QUEENS", "horizon_days": h, "forecast_origin": origin,
             "fit_cutoff": origin, "target_date": origin + timedelta(days=h),
             "model": "seasonal_naive", "actual_count": None, "predicted_count": float(7 - h)}
            for h in range(1, 8)]
    check_future_rows(rows, daily, "seasonal_naive", str(origin))
    rows[0]["predicted_count"] += 1
    with pytest.raises(ValueError, match="baseline differs"):
        check_future_rows(rows, daily, "seasonal_naive", str(origin))
    rows[0]["borough"] = "unrelated"
    with pytest.raises(ValueError, match="geography/horizon"):
        check_future_rows(rows, daily, "seasonal_naive", str(origin))


@pytest.mark.parametrize("broken", ["trials", "summary", "inputs"])
def test_benchmark_audit_rejects_missing_results(broken):
    cfg = {"scales": [2], "partitions": [1, 2], "repetitions": 3}
    settings = [("pandas", 0), ("spark", 1), ("spark", 2)]
    bench = {"inputs": {"2": {}},
             "summary": [{"engine": e, "rows": 2, "partitions": p} for e, p in settings],
             "trials": [{"engine": e, "rows": 2, "partitions": p, "repeat": r}
                        for e, p in settings for r in range(3)]}
    check_benchmark_coverage(bench, cfg)
    if broken == "inputs":
        bench[broken].clear()
    else:
        bench[broken].pop()
    with pytest.raises(ValueError, match="incomplete"):
        check_benchmark_coverage(bench, cfg)
