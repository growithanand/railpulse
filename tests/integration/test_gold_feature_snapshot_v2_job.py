from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

from railpulse.catalog import CatalogNamespace
from railpulse.features.cycle_operating_context import (
    STATUS_AVAILABLE as CONTEXT_AVAILABLE,
)
from railpulse.features.cycle_operating_context import (
    STATUS_MISSING_PREVIOUS,
)
from railpulse.features.feature_eligibility import STATUS_ELIGIBLE, STATUS_INELIGIBLE
from railpulse.features.feature_snapshot_v2_schema import FEATURE_SNAPSHOTS_V2_TABLE
from railpulse.jobs.gold_feature_snapshot_v2_processing import (
    GOLD_FEATURE_SNAPSHOT_V2_PROCESSING_JOB_CONTRACT_VERSION,
    run_managed_gold_feature_snapshot_v2_processing,
)


def _namespace() -> CatalogNamespace:
    return CatalogNamespace(
        catalog="spark_catalog",
        bronze_schema="railpulse_managed_bronze_feature_v2_job_test",
        silver_schema="railpulse_managed_silver_feature_v2_job_test",
        gold_schema="railpulse_managed_gold_feature_v2_job_test",
    )


def _cycles(spark: SparkSession):
    schema = StructType(
        [
            StructField("loaded_cycle_id", StringType(), nullable=False),
            StructField("loaded_cycle_start_type", StringType(), nullable=False),
            StructField("loaded_cycle_start_timestamp", TimestampNTZType(), nullable=False),
            StructField("loaded_cycle_stop_timestamp", TimestampNTZType(), nullable=False),
            StructField("observed_duration_seconds", LongType(), nullable=False),
            StructField("is_right_censored", BooleanType(), nullable=False),
        ]
    )
    start = datetime(2020, 1, 1)
    return spark.createDataFrame(
        [
            ("cycle-1", "observed", start, start + timedelta(seconds=2), 2, False),
            (
                "cycle-2",
                "observed",
                start + timedelta(seconds=4),
                start + timedelta(seconds=6),
                2,
                False,
            ),
        ],
        schema=schema,
    )


def _telemetry(spark: SparkSession):
    schema = StructType(
        [
            StructField("event_timestamp", TimestampNTZType(), nullable=False),
            StructField("motor_current", DoubleType(), nullable=False),
            StructField("interval_seconds", LongType(), nullable=False),
            StructField("is_forward_gap", BooleanType(), nullable=False),
            StructField("dataset_version", StringType(), nullable=False),
            StructField("source_sha256", StringType(), nullable=False),
            StructField("ingestion_batch_id", StringType(), nullable=False),
        ]
    )
    start = datetime(2020, 1, 1)
    return spark.createDataFrame(
        [
            (
                start + timedelta(seconds=offset),
                float(10 + offset),
                0 if offset == 0 else 1,
                False,
                "dataset-v1",
                "telemetry-sha",
                "telemetry-batch",
            )
            for offset in range(7)
        ],
        schema=schema,
    )


@pytest.mark.spark
def test_managed_feature_snapshot_v2_job_builds_catalog_table_and_reconciles_rerun(
    spark: SparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    namespace = _namespace()
    silver_schema = namespace.schema("silver")
    gold_schema = namespace.schema("gold")
    cycle_table = namespace.table("gold", "loaded_cycles")
    telemetry_table = namespace.table("silver", "telemetry_accepted")
    snapshot_table = namespace.table("gold", FEATURE_SNAPSHOTS_V2_TABLE)
    cycles = _cycles(spark)
    telemetry = _telemetry(spark)

    def reject_persist(*args: object, **kwargs: object) -> None:
        raise AssertionError("managed feature processing must not persist DataFrames")

    monkeypatch.setattr(type(telemetry), "persist", reject_persist)

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {silver_schema}").collect()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {gold_schema}").collect()
    try:
        cycles.write.format("delta").mode("error").saveAsTable(cycle_table)
        telemetry.write.format("delta").mode("error").saveAsTable(telemetry_table)

        first = run_managed_gold_feature_snapshot_v2_processing(spark, namespace)
        repeated = run_managed_gold_feature_snapshot_v2_processing(spark, namespace)

        assert first.contract_version == GOLD_FEATURE_SNAPSHOT_V2_PROCESSING_JOB_CONTRACT_VERSION
        assert first.cycle_source_table == cycle_table
        assert first.telemetry_source_table == telemetry_table
        assert first.telemetry_lineage.dataset_version == "dataset-v1"
        assert first.telemetry_lineage.source_sha256 == "telemetry-sha"
        assert first.accepted_telemetry_record_count == 7
        assert first.cycle_count == 2
        assert {row.status: row.cycle_count for row in first.eligibility_status_counts} == {
            STATUS_ELIGIBLE: 2,
            STATUS_INELIGIBLE: 0,
        }
        assert sum(row.cycle_count for row in first.eligibility_reason_counts) == 0
        assert (
            first.write.source_snapshot_count,
            first.write.inserted_snapshot_count,
            first.write.unchanged_snapshot_count,
            first.write.target_snapshot_count_before,
            first.write.target_snapshot_count_after,
        ) == (2, 2, 0, 0, 2)
        assert (
            repeated.write.inserted_snapshot_count,
            repeated.write.unchanged_snapshot_count,
            repeated.write.target_snapshot_count_before,
            repeated.write.target_snapshot_count_after,
        ) == (0, 2, 2, 2)

        rows = {
            row.loaded_cycle_id: row.cycle_context_status
            for row in spark.table(snapshot_table).collect()
        }
        assert rows == {
            "cycle-1": STATUS_MISSING_PREVIOUS,
            "cycle-2": CONTEXT_AVAILABLE,
        }
    finally:
        spark.sql(f"DROP DATABASE IF EXISTS {gold_schema} CASCADE").collect()
        spark.sql(f"DROP DATABASE IF EXISTS {silver_schema} CASCADE").collect()
