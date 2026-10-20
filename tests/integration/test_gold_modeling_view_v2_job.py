from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampNTZType

from railpulse.catalog import CatalogNamespace
from railpulse.evaluation.modeling_view_schema import STATUS_EXCLUDED, STATUS_TRAINABLE
from railpulse.evaluation.modeling_view_v2_schema import (
    MODELING_VIEW_V2_TABLE,
    REASON_CYCLE_CONTEXT_UNAVAILABLE,
)
from railpulse.features.failure_horizon_storage import FAILURE_HORIZONS_TABLE
from railpulse.features.failure_horizons import assign_cycle_failure_horizons
from railpulse.features.feature_snapshot_v2 import build_feature_snapshot_v2
from railpulse.features.feature_snapshot_v2_schema import FEATURE_SNAPSHOTS_V2_TABLE
from railpulse.jobs.gold_modeling_view_v2_processing import (
    GOLD_MODELING_VIEW_V2_PROCESSING_JOB_CONTRACT_VERSION,
    run_managed_gold_modeling_view_v2_processing,
)

_STOPS = {
    "negative": datetime(2020, 1, 1, 7, 0),
    "positive": datetime(2020, 1, 1, 9, 0),
    "missing": None,
}


def _namespace() -> CatalogNamespace:
    return CatalogNamespace(
        catalog="spark_catalog",
        bronze_schema="railpulse_managed_bronze_modeling_v2_job_test",
        silver_schema="railpulse_managed_silver_modeling_v2_job_test",
        gold_schema="railpulse_managed_gold_modeling_v2_job_test",
    )


def _failure_events(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [
            (
                "failure-1",
                1,
                datetime(2020, 1, 1, 10, 0),
                datetime(2020, 1, 1, 11, 0),
                "fixture-v1",
                "failure-sha",
                "failure-document-sha",
                "failure-batch",
            )
        ],
        StructType(
            [
                StructField("record_id", StringType(), nullable=False),
                StructField("source_row", LongType(), nullable=False),
                StructField("failure_start", TimestampNTZType(), nullable=False),
                StructField("failure_end", TimestampNTZType(), nullable=False),
                StructField("dataset_version", StringType(), nullable=False),
                StructField("source_sha256", StringType(), nullable=False),
                StructField("source_document_sha256", StringType(), nullable=False),
                StructField("ingestion_batch_id", StringType(), nullable=False),
            ]
        ),
    )


def _horizons(spark: SparkSession, failures: DataFrame) -> DataFrame:
    cycles = spark.createDataFrame(
        [(cycle_id, stop, stop is None) for cycle_id, stop in _STOPS.items()],
        "loaded_cycle_id string, loaded_cycle_stop_timestamp timestamp_ntz, "
        "is_right_censored boolean",
    )
    return assign_cycle_failure_horizons(
        cycles,
        failures,
        horizon_seconds=7_200,
        observation_end=datetime(2020, 1, 1, 12, 0),
    )


def _snapshots(spark: SparkSession) -> DataFrame:
    feature_rows = []
    context_rows = []
    for cycle_id, stop in _STOPS.items():
        window_start = None if stop is None else stop - timedelta(minutes=15)
        available = stop is not None
        feature_rows.append(
            (
                cycle_id,
                "motor-current-15m-v1",
                900,
                window_start,
                "available" if available else "missing_prediction_boundary",
                91 if available else 0,
                window_start,
                stop,
                3.0 if available else None,
                4.0 if available else None,
                5.0 if available else None,
                "motor-current-15m-eligibility-v1",
                20,
                10 if available else None,
                "eligible" if available else "ineligible",
                [] if available else ["feature_missing_prediction_boundary"],
            )
        )
        context_rows.append(
            (
                cycle_id,
                "loaded-cycle-operating-context-v1",
                "available" if available else "missing_prediction_boundary",
                120 if available else None,
                f"previous-{cycle_id}" if available else None,
                110 if available else None,
                800 if available else None,
            )
        )
    features = spark.createDataFrame(
        feature_rows,
        "loaded_cycle_id string, motor_current_15m_feature_version string, "
        "motor_current_15m_window_seconds long, motor_current_15m_window_start timestamp_ntz, "
        "motor_current_15m_status string, motor_current_15m_observation_count long, "
        "motor_current_15m_first_observation_timestamp timestamp_ntz, "
        "motor_current_15m_last_observation_timestamp timestamp_ntz, "
        "motor_current_15m_minimum_amperes double, motor_current_15m_mean_amperes double, "
        "motor_current_15m_maximum_amperes double, motor_current_eligibility_version string, "
        "motor_current_eligibility_minimum_excluded_gap_seconds long, "
        "motor_current_maximum_window_gap_seconds long, motor_current_eligibility_status string, "
        "motor_current_eligibility_reasons array<string>",
    )
    context = spark.createDataFrame(
        context_rows,
        "loaded_cycle_id string, cycle_context_feature_version string, "
        "cycle_context_status string, cycle_context_current_duration_seconds long, "
        "cycle_context_previous_cycle_id string, cycle_context_previous_duration_seconds long, "
        "cycle_context_previous_idle_seconds long",
    )
    return build_feature_snapshot_v2(
        features,
        context,
        dataset_version="fixture-v1",
        telemetry_source_sha256="telemetry-sha",
        telemetry_ingestion_batch_id="telemetry-batch",
    )


@pytest.mark.spark
def test_managed_modeling_view_v2_job_builds_catalog_table_and_reconciles_rerun(
    spark: SparkSession,
) -> None:
    namespace = _namespace()
    silver_schema = namespace.schema("silver")
    gold_schema = namespace.schema("gold")
    feature_table = namespace.table("gold", FEATURE_SNAPSHOTS_V2_TABLE)
    horizon_table = namespace.table("gold", FAILURE_HORIZONS_TABLE)
    failure_table = namespace.table("silver", "failure_events_accepted")
    modeling_table = namespace.table("gold", MODELING_VIEW_V2_TABLE)
    failures = _failure_events(spark)
    horizons = _horizons(spark, failures)
    snapshots = _snapshots(spark)

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {silver_schema}").collect()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {gold_schema}").collect()
    try:
        snapshots.write.format("delta").mode("error").saveAsTable(feature_table)
        horizons.write.format("delta").mode("error").saveAsTable(horizon_table)
        failures.write.format("delta").mode("error").saveAsTable(failure_table)

        first = run_managed_gold_modeling_view_v2_processing(spark, namespace)
        repeated = run_managed_gold_modeling_view_v2_processing(spark, namespace)

        assert first.contract_version == GOLD_MODELING_VIEW_V2_PROCESSING_JOB_CONTRACT_VERSION
        assert first.feature_source_table == feature_table
        assert first.horizon_source_table == horizon_table
        assert first.failure_source_table == failure_table
        assert first.feature_lineage.source_sha256 == "telemetry-sha"
        assert first.failure_lineage.source_document_sha256 == "failure-document-sha"
        assert first.accepted_failure_event_count == 1
        assert first.matched_trainable_failure_event_count == 1
        assert first.summary.cycle_count == 3
        assert {row.status: row.cycle_count for row in first.summary.status_counts} == {
            STATUS_TRAINABLE: 2,
            STATUS_EXCLUDED: 1,
        }
        assert (
            first.write.source_row_count,
            first.write.inserted_row_count,
            first.write.unchanged_row_count,
            first.write.target_row_count_before,
            first.write.target_row_count_after,
        ) == (3, 3, 0, 0, 3)
        assert (
            repeated.write.inserted_row_count,
            repeated.write.unchanged_row_count,
            repeated.write.target_row_count_before,
            repeated.write.target_row_count_after,
        ) == (0, 3, 3, 3)

        rows = {row.loaded_cycle_id: row for row in spark.table(modeling_table).collect()}
        assert rows["positive"].modeling_row_status == STATUS_TRAINABLE
        assert rows["negative"].modeling_row_status == STATUS_TRAINABLE
        assert rows["missing"].modeling_row_status == STATUS_EXCLUDED
        assert REASON_CYCLE_CONTEXT_UNAVAILABLE in rows["missing"].modeling_exclusion_reasons
    finally:
        spark.sql(f"DROP DATABASE IF EXISTS {gold_schema} CASCADE").collect()
        spark.sql(f"DROP DATABASE IF EXISTS {silver_schema} CASCADE").collect()
