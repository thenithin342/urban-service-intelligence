"""Small synthetic input-prefix checks; benchmark findings use real records."""

import csv
import json

import pytest

from src.evaluation.benchmark import benchmark_speedups, prepare_inputs, read_counts
from src.ingestion.downloader import DEFAULT_FIELDS, sha256_file


def test_speedup_ratio_direction():
    report = {"summary": [{"engine": e, "rows": n, "partitions": p, "median_seconds": seconds}
                           for e, n, p, seconds in (("pandas", 10, 0, 1), ("spark", 10, 2, 2),
                                                    ("pandas", 20, 0, 4), ("spark", 20, 2, 2))]}
    assert benchmark_speedups(report) == [{"rows": 10, "pandas_seconds_over_spark_seconds": 0.5},
                                         {"rows": 20, "pandas_seconds_over_spark_seconds": 2.0}]


def test_missing_benchmark_part_files_are_not_empty_results(tmp_path):
    with pytest.raises(ValueError, match=r"missing.*part"):
        read_counts(tmp_path)


def test_benchmark_prefixes_are_exact_and_do_not_synthesize_rows(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    path = source / "2024-01-01.csv"
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=DEFAULT_FIELDS)
        writer.writeheader()
        for key in ("1", "2", "3"):
            writer.writerow({"unique_key": key})
    (source / "manifest.json").write_text(json.dumps({"complete": True, "rows_written": 3,
        "days": [{"day": "2024-01-01", "sha256": sha256_file(path)}]}))
    cfg = {"source_dir": "source", "output_dir": "out", "scales": [2, 3]}
    values = prepare_inputs(cfg, tmp_path)
    for n, info in values.items():
        with open(info["path"], newline="", encoding="utf-8") as stream:
            assert [r["unique_key"] for r in csv.DictReader(stream)] == [str(i) for i in range(1, n + 1)]
    with pytest.raises(ValueError, match="scales"):
        prepare_inputs({**cfg, "scales": [4]}, tmp_path)
