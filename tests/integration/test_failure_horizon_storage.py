from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

from railpulse.catalog import CatalogNamespace
from railpulse.features.failure_horizon_storage import (
    CATALOG_FAILURE_HORIZON_CONTRACT_VERSION,
    FAILURE_HORIZONS_TABLE,
    FailureHorizonPersistenceError,
    catalog_failure_horizon_table,
    persist_catalog_failure_horizons,
)
from railpulse.features.failure_horizons import assign_cycle_failure_horizons


def _namespace() -> CatalogNamespace:
    return CatalogNamespace(
        catalog="spark_catalog",
        bronze_schema="railpulse_managed_bronze_test",
        silver_schema="railpulse_managed_silver_test",
        gold_schema="railpulse_managed_gold_horizon_test",
    )


def _horizons(spark: SparkSession) -> DataFrame:
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
    failure_schema = StructType(
        [
            StructField("record_id", StringType(), nullable=False),
            StructField("source_row", LongType(), nullable=False),
            StructField("failure_start", TimestampNTZType(), nullable=False),
            StructField("failure_end", TimestampNTZType(), nullable=False),
        ]
    )
    failures = spark.createDataFrame(
        [("failure-1", 1, datetime(2020, 1, 1, 10, 0), datetime(2020, 1, 1, 11, 0))],
        schema=failure_schema,
    )
    return assign_cycle_failure_horizons(
        cycles,
        failures,
        horizon_seconds=7_200,
        observation_end=datetime(2020, 1, 3),
    )


@pytest.mark.spark
def test_managed_failure_horizon_merge_is_allowlisted_immutable_and_serverless_safe(
    spark: SparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    namespace = _namespace()
    qualified_schema = namespace.schema("gold")
    qualified_table = namespace.table("gold", FAILURE_HORIZONS_TABLE)
    horizons = _horizons(spark)

    def reject_persist(*args: object, **kwargs: object) -> None:
        raise AssertionError("managed failure-horizon writes must not persist DataFrames")

    monkeypatch.setattr(type(horizons), "persist", reject_persist)

    with pytest.raises(FailureHorizonPersistenceError, match="Unsupported managed"):
        catalog_failure_horizon_table(namespace, "unexpected_table")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {qualified_schema}").collect()
    try:
        first = persist_catalog_failure_horizons(horizons, namespace)
        repeated = persist_catalog_failure_horizons(horizons, namespace)

        target = spark.table(qualified_table)
        assert first.contract_version == CATALOG_FAILURE_HORIZON_CONTRACT_VERSION
        assert first.table_name == qualified_table
        assert first.key_field == "loaded_cycle_id"
        assert (
            first.source_horizon_count,
            first.inserted_horizon_count,
            first.unchanged_horizon_count,
            first.target_horizon_count_before,
            first.target_horizon_count_after,
        ) == (3, 3, 0, 0, 3)
        assert (
            repeated.source_horizon_count,
            repeated.inserted_horizon_count,
            repeated.unchanged_horizon_count,
            repeated.target_horizon_count_before,
            repeated.target_horizon_count_after,
        ) == (3, 0, 3, 3, 3)
        assert {row.loaded_cycle_id for row in target.collect()} == {
            "positive",
            "negative",
            "missing-prediction",
        }

        different_snapshot = horizons.withColumn(
            "loaded_cycle_id",
            F.concat(F.lit("new-"), F.col("loaded_cycle_id")),
        ).withColumn(
            "label_observation_end",
            F.col("label_observation_end") + F.expr("INTERVAL 1 DAY"),
        )
        with pytest.raises(FailureHorizonPersistenceError, match="observation boundary differs"):
            persist_catalog_failure_horizons(different_snapshot, namespace)

        changed = horizons.withColumn(
            "matched_failure_end",
            F.when(
                F.col("loaded_cycle_id") == "positive",
                F.col("matched_failure_end") + F.expr("INTERVAL 1 MINUTE"),
            ).otherwise(F.col("matched_failure_end")),
        )
        with pytest.raises(FailureHorizonPersistenceError, match="immutable target row"):
            persist_catalog_failure_horizons(changed, namespace)

        unchanged = spark.table(qualified_table).where(F.col("loaded_cycle_id") == "positive")
        assert unchanged.first().matched_failure_end == datetime(2020, 1, 1, 11, 0)
    finally:
        spark.sql(f"DROP DATABASE IF EXISTS {qualified_schema} CASCADE").collect()


@pytest.mark.spark
def test_managed_failure_horizon_merge_rejects_invalid_source_rows(
    spark: SparkSession,
) -> None:
    namespace = _namespace()
    horizons = _horizons(spark)

    with pytest.raises(FailureHorizonPersistenceError, match="missing managed Gold"):
        persist_catalog_failure_horizons(horizons.drop("failure_horizon_status"), namespace)

    duplicate = horizons.unionByName(horizons.limit(1))
    with pytest.raises(FailureHorizonPersistenceError, match="contains duplicate"):
        persist_catalog_failure_horizons(duplicate, namespace)

    invalid_target = horizons.withColumn(
        "failure_within_horizon",
        F.when(F.col("loaded_cycle_id") == "negative", F.lit(True)).otherwise(
            F.col("failure_within_horizon")
        ),
    )
    with pytest.raises(FailureHorizonPersistenceError, match="inconsistent horizon semantics"):
        persist_catalog_failure_horizons(invalid_target, namespace)

    inconsistent_boundary = horizons.withColumn(
        "prediction_timestamp",
        F.when(
            F.col("loaded_cycle_id") == "positive",
            F.col("prediction_timestamp") + F.expr("INTERVAL 1 MINUTE"),
        ).otherwise(F.col("prediction_timestamp")),
    )
    with pytest.raises(FailureHorizonPersistenceError, match="inconsistent horizon semantics"):
        persist_catalog_failure_horizons(inconsistent_boundary, namespace)
