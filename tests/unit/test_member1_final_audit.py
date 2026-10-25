"""Independent arithmetic and leakage checks used by the final audit."""

import json
import math
from datetime import date

import pytest

from src.evaluation.member1_audit import (
    audit_member1,
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
    with pytest.raises(ValueError, match="seed changed"):
        audit_member1(None, {"output_dir": "out", "seed": 43}, {}, tmp_path)
