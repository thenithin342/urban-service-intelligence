"""Spark session helper. ASCII only.

Standard environment is the project .venv (Python 3.10 + PySpark 3.5.3).
Driver and worker interpreters are forced to the running interpreter
unless PYSPARK_PYTHON / PYSPARK_DRIVER_PYTHON are already set, so workers
always match the driver (see docs/decisions.md).
"""

from __future__ import annotations

import os
import sys


def ensure_worker_env() -> dict[str, str]:
    """Pin worker/driver python to current interpreter if unset.

    Also defaults HADOOP_HOME/PATH to a local winutils install
    (C:\\hadoop, one-time setup, see README) so Parquet writes work
    on Windows without manual env configuration in every shell.
    """
    exe = sys.executable
    os.environ.setdefault("PYSPARK_PYTHON", exe)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", exe)
    default_home = r"C:\hadoop"
    if os.path.isfile(os.path.join(default_home, "bin", "winutils.exe")):
        os.environ.setdefault("HADOOP_HOME", default_home)
        bindir = os.path.join(default_home, "bin")
        path = os.environ.get("PATH", "")
        if bindir.lower() not in path.lower():
            os.environ["PATH"] = bindir + os.pathsep + path
    return spark_env_note()


def spark_env_note() -> dict[str, str]:
    return {
        "PYSPARK_PYTHON": os.environ.get("PYSPARK_PYTHON", ""),
        "PYSPARK_DRIVER_PYTHON": os.environ.get("PYSPARK_DRIVER_PYTHON", ""),
    }


def get_spark(app_name: str = "urban-service", master: str = "local[2]"):
    ensure_worker_env()
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
    )
    return builder.getOrCreate()
