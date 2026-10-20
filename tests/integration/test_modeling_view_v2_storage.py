from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampNTZType

from railpulse.catalog import CatalogNamespace
from railpulse.evaluation.modeling_view_v2 import build_modeling_view_v2
from railpulse.evaluation.modeling_view_v2_storage import (
    CATALOG_MODELING_VIEW_V2_CONTRACT_VERSION,
    ModelingViewV2PersistenceError,
    catalog_modeling_view_v2_table,
    persist_catalog_modeling_view_v2,
)
from railpulse.features.failure_horizons import assign_cycle_failure_horizons
from railpulse.features.feature_snapshot_v2 import build_feature_snapshot_v2

_STOPS = {
    "negative": datetime(2020, 1, 1, 7, 0),
    "positive": datetime(2020, 1, 1, 9, 0),
    "missing": None,
}


def _namespace() -> CatalogNamespace:
    return CatalogNamespace(
        catalog="spark_catalog",
        bronze_schema="railpulse_managed_bronze_test",
        silver_schema="railpulse_managed_silver_test",
        gold_schema="railpulse_managed_gold_modeling_v2_test",
    )


def _horizons(spark: SparkSession) -> DataFrame:
    cycles = spark.createDataFrame(
        [(cycle_id, stop, stop is None) for cycle_id, stop in _STOPS.items()],
        "loaded_cycle_id string, loaded_cycle_stop_timestamp timestamp_ntz, "
        "is_right_censored boolean",
    )
    failures = spark.createDataFrame(
        [("failure-1", 1, datetime(2020, 1, 1, 10, 0), datetime(2020, 1, 1, 11, 0))],
        StructType(
            [
                StructField("record_id", StringType(), nullable=False),
                StructField("source_row", LongType(), nullable=False),
                StructField("failure_start", TimestampNTZType(), nullable=False),
                StructField("failure_end", TimestampNTZType(), nullable=False),
            ]
        ),
    )
    return assign_cycle_failure_horizons(
        cycles,
        failures,
        horizon_seconds=7_200,
        observation_end=datetime(2020, 1, 1, 12, 0),
    )


def _features(spark: SparkSession) -> DataFrame:
    rows = []
    for cycle_id, stop in _STOPS.items():
        window_start = None if stop is None else stop - timedelta(minutes=15)
        available = stop is not None
        rows.append(
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
    return spark.createDataFrame(
        rows,
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


def _context(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [
            (
                cycle_id,
                "loaded-cycle-operating-context-v1",
                "available" if stop is not None else "missing_prediction_boundary",
                120 if stop is not None else None,
                f"previous-{cycle_id}" if stop is not None else None,
                110 if stop is not None else None,
                800 if stop is not None else None,
            )
            for cycle_id, stop in _STOPS.items()
        ],
        "loaded_cycle_id string, cycle_context_feature_version string, "
        "cycle_context_status string, cycle_context_current_duration_seconds long, "
        "cycle_context_previous_cycle_id string, cycle_context_previous_duration_seconds long, "
        "cycle_context_previous_idle_seconds long",
    )


def _modeling_rows(spark: SparkSession) -> DataFrame:
    snapshots = build_feature_snapshot_v2(
        _features(spark),
        _context(spark),
        dataset_version="fixture-v1",
        telemetry_source_sha256="telemetry-sha",
        telemetry_ingestion_batch_id="telemetry-batch",
    )
    return build_modeling_view_v2(
        snapshots,
        _horizons(spark),
        failure_dataset_version="fixture-v1",
        failure_source_sha256="failure-sha",
        failure_ingestion_batch_id="failure-batch",
    )


@pytest.mark.spark
def test_managed_modeling_view_v2_merge_is_allowlisted_immutable_and_idempotent(
    spark: SparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    namespace = _namespace()
    qualified_schema = namespace.schema("gold")
    qualified_table = namespace.table("gold", "modeling_view_v2")
    modeling_rows = _modeling_rows(spark)

    def reject_persist(*args: object, **kwargs: object) -> None:
        raise AssertionError("managed modelling-view writes must not persist DataFrames")

    monkeypatch.setattr(type(modeling_rows), "persist", reject_persist)

    with pytest.raises(ModelingViewV2PersistenceError, match="Unsupported managed"):
        catalog_modeling_view_v2_table(namespace, "unexpected_table")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {qualified_schema}").collect()
    try:
        first = persist_catalog_modeling_view_v2(modeling_rows, namespace)
        repeated = persist_catalog_modeling_view_v2(modeling_rows, namespace)

        target = spark.table(qualified_table)
        assert first.contract_version == CATALOG_MODELING_VIEW_V2_CONTRACT_VERSION
        assert first.table_name == qualified_table
        assert first.key_field == "loaded_cycle_id"
        assert (
            first.source_row_count,
            first.inserted_row_count,
            first.unchanged_row_count,
            first.target_row_count_before,
            first.target_row_count_after,
        ) == (3, 3, 0, 0, 3)
        assert (
            repeated.source_row_count,
            repeated.inserted_row_count,
            repeated.unchanged_row_count,
            repeated.target_row_count_before,
            repeated.target_row_count_after,
        ) == (3, 0, 3, 3, 3)
        assert {row.loaded_cycle_id for row in target.collect()} == set(_STOPS)

        different_lineage = modeling_rows.withColumn(
            "loaded_cycle_id",
            F.concat(F.lit("new-"), F.col("loaded_cycle_id")),
        ).withColumn("failure_ingestion_batch_id", F.lit("different-failure-batch"))
        with pytest.raises(ModelingViewV2PersistenceError, match="source lineage differs"):
            persist_catalog_modeling_view_v2(different_lineage, namespace)

        changed = modeling_rows.where(F.col("loaded_cycle_id") == "positive").withColumn(
            "cycle_context_previous_idle_seconds",
            F.lit(801).cast("long"),
        )
        with pytest.raises(ModelingViewV2PersistenceError, match="immutable target row"):
            persist_catalog_modeling_view_v2(changed, namespace)

        unchanged = spark.table(qualified_table).where(F.col("loaded_cycle_id") == "positive")
        assert unchanged.first().cycle_context_previous_idle_seconds == 800
    finally:
        spark.sql(f"DROP DATABASE IF EXISTS {qualified_schema} CASCADE").collect()


@pytest.mark.spark
def test_managed_modeling_view_v2_merge_rejects_invalid_source_rows(
    spark: SparkSession,
) -> None:
    namespace = _namespace()
    modeling_rows = _modeling_rows(spark)

    with pytest.raises(ModelingViewV2PersistenceError, match="missing managed Gold"):
        persist_catalog_modeling_view_v2(modeling_rows.drop("modeling_row_status"), namespace)

    with pytest.raises(ModelingViewV2PersistenceError, match="contains no modelling rows"):
        persist_catalog_modeling_view_v2(modeling_rows.limit(0), namespace)

    duplicate = modeling_rows.unionByName(modeling_rows.limit(1))
    with pytest.raises(ModelingViewV2PersistenceError, match="contains duplicate"):
        persist_catalog_modeling_view_v2(duplicate, namespace)

    invalid_state = modeling_rows.withColumn(
        "modeling_row_status",
        F.when(F.col("loaded_cycle_id") == "missing", F.lit("trainable")).otherwise(
            F.col("modeling_row_status")
        ),
    )
    with pytest.raises(ModelingViewV2PersistenceError, match="inconsistent modelling-row"):
        persist_catalog_modeling_view_v2(invalid_state, namespace)

    mixed_lineage = modeling_rows.withColumn(
        "failure_ingestion_batch_id",
        F.when(F.col("loaded_cycle_id") == "positive", F.lit("different-batch")).otherwise(
            F.col("failure_ingestion_batch_id")
        ),
    )
    with pytest.raises(ModelingViewV2PersistenceError, match=r"exactly one.*lineage"):
        persist_catalog_modeling_view_v2(mixed_lineage, namespace)

    invalid_boundary = modeling_rows.withColumn(
        "motor_current_15m_window_start",
        F.when(
            F.col("loaded_cycle_id") == "positive",
            F.col("motor_current_15m_window_start") + F.expr("INTERVAL 1 MINUTE"),
        ).otherwise(F.col("motor_current_15m_window_start")),
    )
    with pytest.raises(ModelingViewV2PersistenceError, match="crosses its prediction boundary"):
        persist_catalog_modeling_view_v2(invalid_boundary, namespace)
