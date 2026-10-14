"""Real Spark Python-worker transformations (NOT JVM-only). ASCII only.

Requires the standard env (.venv Python 3.10 + PySpark 3.5.3) where
Python workers function. Fails loudly on interpreters with broken
worker transport (e.g. system Python 3.13 on Windows).
"""

from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.utils.spark import ensure_worker_env, get_spark


def _session():
    ensure_worker_env()
    return get_spark("worker-test", master="local[2]")


def test_create_dataframe_python_worker():
    spark = _session()
    try:
        df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "v"])
        assert df.count() == 3
    finally:
        spark.stop()


def test_udf_python_worker():
    spark = _session()
    try:
        df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
        plus = udf(lambda x: x + 100, IntegerType())
        got = df.withColumn("w", plus(F.col("id"))).groupBy().sum("w").collect()[0][0]
        assert got == 306
    finally:
        spark.stop()


def test_shuffle_groupby_python_worker():
    spark = _session()
    try:
        schema = StructType(
            [StructField("k", StringType(), True), StructField("n", IntegerType(), True)]
        )
        rows = [("a", 1), ("b", 2), ("a", 3), ("b", 4), ("c", 5)]
        df = spark.createDataFrame(rows, schema=schema)
        got = {r["k"]: r["total"] for r in df.groupBy("k").sum("n").withColumnRenamed("sum(n)", "total").collect()}
        assert got == {"a": 4, "b": 6, "c": 5}
    finally:
        spark.stop()


def test_rdd_map_python_worker():
    spark = _session()
    try:
        assert spark.sparkContext.parallelize([1, 2, 3, 4]).map(lambda x: x * 2).sum() == 20
    finally:
        spark.stop()
