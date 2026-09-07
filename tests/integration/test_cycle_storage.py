from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    ByteType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

from railpulse.config import RailPulseConfig, SchemaNames, StoragePaths
from railpulse.features.cycle_storage import (
    LOADED_CYCLES_TABLE,
    GoldCyclePersistenceError,
    gold_table_path,
    persist_loaded_cycles,
)
from railpulse.features.cycles import (
    aggregate_loaded_cycles,
    annotate_loaded_cycle_boundaries,
    assign_loaded_cycle_segments,
)


def _test_config(tmp_path: Path) -> RailPulseConfig:
    return RailPulseConfig(
        name="railpulse-test",
        environment="test",
        dataset_version="fixture-v1",
        project_root=tmp_path,
        paths=StoragePaths(
            raw_data=tmp_path / "raw",
            processed_data=tmp_path / "processed",
            delta=tmp_path / "delta",
            checkpoints=tmp_path / "checkpoints",
            artifacts=tmp_path / "artifacts",
        ),
        schemas=SchemaNames(
            catalog="railpulse_test",
            bronze="bronze",
            silver="silver",
            gold="gold",
        ),
    )


def _telemetry(spark: SparkSession) -> DataFrame:
    start = datetime(2020, 2, 1)
    schema = StructType(
        [
            StructField("record_id", StringType(), nullable=False),
            StructField("source_index", LongType(), nullable=False),
            StructField("event_timestamp", TimestampNTZType(), nullable=False),
            StructField("previous_source_index", LongType(), nullable=True),
            StructField("dv_eletric", ByteType(), nullable=False),
            StructField("is_forward_gap", BooleanType(), nullable=False),
            StructField(
                "rejection_reasons",
                ArrayType(StringType(), containsNull=False),
                nullable=False,
            ),
        ]
    )
    rows = [
        ("inactive", 0, start, None, 0, False, []),
        ("first-start", 10, start + timedelta(seconds=10), 0, 1, False, []),
        ("first-active", 20, start + timedelta(seconds=20), 10, 1, False, []),
        ("first-stop", 30, start + timedelta(seconds=30), 20, 0, False, []),
        ("second-start", 40, start + timedelta(seconds=40), 30, 1, False, []),
    ]
    return spark.createDataFrame(rows, schema=schema)


def _cycle_snapshot(telemetry: DataFrame, maximum_source_index: int) -> DataFrame:
    source = telemetry.where(telemetry.source_index <= maximum_source_index)
    return aggregate_loaded_cycles(
        assign_loaded_cycle_segments(annotate_loaded_cycle_boundaries(source))
    )


@pytest.mark.spark
def test_gold_cycle_merge_closes_open_cycle_without_changing_its_id(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    config = _test_config(tmp_path)
    telemetry = _telemetry(spark)
    open_snapshot = _cycle_snapshot(telemetry, 10)
    extended_open_snapshot = _cycle_snapshot(telemetry, 20)
    later_snapshot = _cycle_snapshot(telemetry, 40)
    open_cycle_id = open_snapshot.first().loaded_cycle_id

    first = persist_loaded_cycles(open_snapshot, config)
    repeated_open = persist_loaded_cycles(open_snapshot, config)
    extended_open = persist_loaded_cycles(extended_open_snapshot, config)
    open_target = (
        spark.read.format("delta").load(str(gold_table_path(config, LOADED_CYCLES_TABLE))).first()
    )
    closed_and_extended = persist_loaded_cycles(later_snapshot, config)
    repeated_later = persist_loaded_cycles(later_snapshot, config)

    target = spark.read.format("delta").load(str(gold_table_path(config, LOADED_CYCLES_TABLE)))
    rows = {row.loaded_cycle_start_record_id: row for row in target.collect()}
    closed = rows["first-start"]

    assert first.table_name == "gold.loaded_cycles"
    assert (
        first.source_cycle_count,
        first.inserted_cycle_count,
        first.updated_cycle_count,
        first.unchanged_cycle_count,
    ) == (1, 1, 0, 0)
    assert (
        repeated_open.inserted_cycle_count,
        repeated_open.updated_cycle_count,
        repeated_open.unchanged_cycle_count,
    ) == (0, 0, 1)
    assert (
        extended_open.inserted_cycle_count,
        extended_open.updated_cycle_count,
        extended_open.unchanged_cycle_count,
    ) == (0, 1, 0)
    assert open_target.loaded_observation_count == 2
    assert open_target.is_right_censored is True
    assert (
        closed_and_extended.source_cycle_count,
        closed_and_extended.inserted_cycle_count,
        closed_and_extended.updated_cycle_count,
        closed_and_extended.unchanged_cycle_count,
    ) == (2, 1, 1, 0)
    assert (
        repeated_later.inserted_cycle_count,
        repeated_later.updated_cycle_count,
        repeated_later.unchanged_cycle_count,
    ) == (0, 0, 2)
    assert repeated_later.target_cycle_count_before == 2
    assert repeated_later.target_cycle_count_after == 2
    assert target.count() == 2
    assert closed.loaded_cycle_id == open_cycle_id
    assert closed.loaded_cycle_stop_record_id == "first-stop"
    assert closed.loaded_cycle_stop_timestamp == datetime(2020, 2, 1, 0, 0, 30)
    assert closed.loaded_observation_count == 2
    assert closed.is_right_censored is False
    assert closed.observed_duration_seconds == 20
    assert rows["second-start"].is_right_censored is True


@pytest.mark.spark
def test_gold_cycle_merge_rejects_reopening_a_settled_cycle(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    config = _test_config(tmp_path)
    telemetry = _telemetry(spark)
    closed_snapshot = _cycle_snapshot(telemetry, 30)
    earlier_open_snapshot = _cycle_snapshot(telemetry, 20)
    persist_loaded_cycles(closed_snapshot, config)

    with pytest.raises(GoldCyclePersistenceError, match="cannot change or reopen"):
        persist_loaded_cycles(earlier_open_snapshot, config)

    row = spark.read.format("delta").load(str(gold_table_path(config, LOADED_CYCLES_TABLE))).first()
    assert row.loaded_cycle_stop_record_id == "first-stop"
    assert row.is_right_censored is False


@pytest.mark.spark
def test_gold_cycle_merge_rejects_observation_count_regression(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    config = _test_config(tmp_path)
    telemetry = _telemetry(spark)
    longer_open_snapshot = _cycle_snapshot(telemetry, 20)
    shorter_open_snapshot = _cycle_snapshot(telemetry, 10)
    persist_loaded_cycles(longer_open_snapshot, config)

    with pytest.raises(GoldCyclePersistenceError, match="regresses its loaded observation count"):
        persist_loaded_cycles(shorter_open_snapshot, config)

    row = spark.read.format("delta").load(str(gold_table_path(config, LOADED_CYCLES_TABLE))).first()
    assert row.loaded_observation_count == 2
    assert row.is_right_censored is True
