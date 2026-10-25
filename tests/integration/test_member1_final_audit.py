"""Cross-check the independent audit against real Spark metric aggregation."""

from datetime import date

from src.evaluation.member1_audit import check_prediction_rows, independent_metrics
from src.modeling.demand import error_metrics
from src.utils.spark import get_spark


def test_audit_and_spark_metrics_match():
    spark = get_spark("final-audit-test")
    try:
        frame = spark.createDataFrame([
            ("fixture", "test", date(2024, 1, 31), date(2024, 1, 31), date(2024, 2, 1), "QUEENS", 1, 10, 8.0),
            ("fixture", "test", date(2024, 1, 31), date(2024, 1, 31), date(2024, 2, 2), "QUEENS", 2, 12, 14.0)],
            "model string, split string, forecast_origin date, fit_cutoff date, target_date date, borough string, horizon_days int, actual_count bigint, predicted_count double")
        rows = [r.asDict() for r in frame.collect()]
        check_prediction_rows(rows, {(date(2024, 2, 1), "QUEENS"): 10, (date(2024, 2, 2), "QUEENS"): 12})
        expected = independent_metrics(rows)
        measured = error_metrics(frame, ["model"]).first()
        for name, value in expected.items():
            assert abs(measured[name] - value) < 1e-9
    finally:
        spark.stop()
