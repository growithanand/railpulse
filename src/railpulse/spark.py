"""Create local Spark sessions with the Delta Lake extensions enabled."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def create_local_spark_session(
    app_name: str,
    *,
    master: str = "local[2]",
    warehouse_dir: str | Path | None = None,
) -> SparkSession:
    """Return a small local Spark session configured for path-backed Delta tables.

    The caller remains responsible for selecting a supported Java runtime. The function does not
    modify ``JAVA_HOME`` or install dependencies. Databricks jobs should pass their managed
    ``SparkSession`` directly to ingestion functions instead of calling this local helper.
    """

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    builder = (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.default.parallelism", "2")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    if warehouse_dir is not None:
        builder = builder.config(
            "spark.sql.warehouse.dir",
            Path(warehouse_dir).expanduser().resolve().as_uri(),
        )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
