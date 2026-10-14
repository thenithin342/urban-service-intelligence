"""Spark session helper. ASCII only.

Uses JVM-safe defaults for Windows local execution. The caller (or the
environment) must set PYSPARK_PYTHON / PYSPARK_DRIVER_PYTHON consistently.
On this machine full Python-worker ops require Python 3.10 (see
docs/decisions.md); JVM-only ops work on 3.13.
"""

from __future__ import annotations

import os


def spark_env_note() -> dict[str, str]:
    return {
        "PYSPARK_PYTHON": os.environ.get("PYSPARK_PYTHON", ""),
        "PYSPARK_DRIVER_PYTHON": os.environ.get("PYSPARK_DRIVER_PYTHON", ""),
    }


def get_spark(app_name: str = "urban-service", master: str = "local[2]"):
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
    )
    return builder.getOrCreate()
