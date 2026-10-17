from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

from railpulse.catalog import CatalogNamespace
from railpulse.features.failure_horizon_storage import FAILURE_HORIZONS_TABLE
from railpulse.features.failure_horizons import (
    STATUS_MISSING_PREDICTION,
    STATUS_NEGATIVE,
    STATUS_POSITIVE,
)
from railpulse.jobs.gold_failure_horizon_processing import (
    GOLD_FAILURE_HORIZON_PROCESSING_JOB_CONTRACT_VERSION,
    run_managed_gold_failure_horizon_processing,
)


def _namespace() -> CatalogNamespace:
    return CatalogNamespace(
        catalog="spark_catalog",
        bronze_schema="railpulse_managed_bronze_horizon_job_test",
        silver_schema="railpulse_managed_silver_horizon_job_test",
        gold_schema="railpulse_managed_gold_horizon_job_test",
    )


@pytest.mark.spark
def test_managed_failure_horizon_job_reconciles_catalog_sources_and_rerun(
    spark: SparkSession,
) -> None:
    namespace = _namespace()
    silver_schema = namespace.schema("silver")
    gold_schema = namespace.schema("gold")
    cycle_table = namespace.table("gold", "loaded_cycles")
    telemetry_table = namespace.table("silver", "telemetry_accepted")
    failure_table = namespace.table("silver", "failure_events_accepted")
    horizon_table = namespace.table("gold", FAILURE_HORIZONS_TABLE)

    cycle_schema = StructType(
        [
            StructField("loaded_cycle_id", StringType(), nullable=False),
            StructField("loaded_cycle_stop_timestamp", TimestampNTZType(), nullable=True),
            StructField("is_right_censored", BooleanType(), nullable=False),
        ]
    )
    cycles = spark.createDataFrame(
        [
            ("positive", datetime(2020, 1, 1, 9, 0), False),
            ("negative", datetime(2020, 1, 2, 9, 0), False),
            ("missing-prediction", None, True),
        ],
        schema=cycle_schema,
    )
    telemetry_schema = StructType(
        [
            StructField("event_timestamp", TimestampNTZType(), nullable=False),
            StructField("dataset_version", StringType(), nullable=False),
            StructField("source_sha256", StringType(), nullable=False),
            StructField("ingestion_batch_id", StringType(), nullable=False),
        ]
    )
    telemetry = spark.createDataFrame(
        [
            (datetime(2020, 1, 1), "dataset-v1", "telemetry-sha", "telemetry-batch"),
            (datetime(2020, 1, 3), "dataset-v1", "telemetry-sha", "telemetry-batch"),
        ],
        schema=telemetry_schema,
    )
    failure_schema = StructType(
        [
            StructField("record_id", StringType(), nullable=False),
            StructField("source_row", LongType(), nullable=False),
            StructField("failure_start", TimestampNTZType(), nullable=False),
            StructField("failure_end", TimestampNTZType(), nullable=False),
            StructField("dataset_version", StringType(), nullable=False),
            StructField("source_sha256", StringType(), nullable=False),
            StructField("ingestion_batch_id", StringType(), nullable=False),
        ]
    )
    failures = spark.createDataFrame(
        [
            (
                "failure-1",
                1,
                datetime(2020, 1, 1, 10, 0),
                datetime(2020, 1, 1, 11, 0),
                "dataset-v1",
                "failure-sha",
                "failure-batch",
            )
        ],
        schema=failure_schema,
    )

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {silver_schema}").collect()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {gold_schema}").collect()
    try:
        cycles.write.format("delta").mode("error").saveAsTable(cycle_table)
        telemetry.write.format("delta").mode("error").saveAsTable(telemetry_table)
        failures.write.format("delta").mode("error").saveAsTable(failure_table)

        first = run_managed_gold_failure_horizon_processing(spark, namespace)
        repeated = run_managed_gold_failure_horizon_processing(spark, namespace)

        assert first.contract_version == GOLD_FAILURE_HORIZON_PROCESSING_JOB_CONTRACT_VERSION
        assert first.label_observation_end == "2020-01-03 00:00:00"
        assert first.telemetry_lineage.dataset_version == "dataset-v1"
        assert first.failure_lineage.source_sha256 == "failure-sha"
        assert (
            first.write.source_horizon_count,
            first.write.inserted_horizon_count,
            first.write.unchanged_horizon_count,
            first.write.target_horizon_count_before,
            first.write.target_horizon_count_after,
        ) == (3, 3, 0, 0, 3)
        assert (
            repeated.write.inserted_horizon_count,
            repeated.write.unchanged_horizon_count,
            repeated.write.target_horizon_count_before,
            repeated.write.target_horizon_count_after,
        ) == (0, 3, 3, 3)

        rows = {
            row.loaded_cycle_id: row.failure_horizon_status
            for row in spark.table(horizon_table).collect()
        }
        assert rows == {
            "positive": STATUS_POSITIVE,
            "negative": STATUS_NEGATIVE,
            "missing-prediction": STATUS_MISSING_PREDICTION,
        }
    finally:
        spark.sql(f"DROP DATABASE IF EXISTS {gold_schema} CASCADE").collect()
        spark.sql(f"DROP DATABASE IF EXISTS {silver_schema} CASCADE").collect()
