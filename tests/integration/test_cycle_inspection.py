from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from railpulse.features.cycle_inspection import (
    CYCLE_INSPECTION_VERSION,
    CycleInspectionError,
    inspect_loaded_cycles,
    load_cycle_inspection_query,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _inspection_frame(spark: SparkSession):
    schema = StructType(
        [
            StructField("loaded_cycle_start_type", StringType(), nullable=False),
            StructField("is_right_censored", BooleanType(), nullable=False),
            StructField("loaded_observation_count", LongType(), nullable=False),
            StructField("observed_duration_seconds", LongType(), nullable=True),
        ]
    )
    rows = [
        ("observed", False, 2, 20),
        ("observed", False, 4, 40),
        ("observed", True, 2, None),
        ("left_censored", False, 6, 60),
        ("left_censored", True, 3, None),
    ]
    return spark.createDataFrame(rows, schema=schema)


@pytest.mark.spark
def test_cycle_inspection_query_reconciles_censoring_groups_and_duration_tails(
    spark: SparkSession,
) -> None:
    inspection = inspect_loaded_cycles(
        _inspection_frame(spark),
        load_cycle_inspection_query(PROJECT_ROOT),
    )
    groups = {
        (group.loaded_cycle_start_type, group.is_right_censored): group
        for group in inspection.groups
    }

    assert inspection.inspection_version == CYCLE_INSPECTION_VERSION
    assert inspection.table_name == "gold.loaded_cycles"
    assert inspection.cycle_count == 5
    assert inspection.loaded_observation_count == 17
    assert inspection.duration_cycle_count == 3
    assert len(groups) == 4

    observed = groups[("observed", False)]
    assert observed.cycle_count == 2
    assert observed.loaded_observation_count == 6
    assert observed.duration_cycle_count == 2
    assert observed.minimum_observed_duration_seconds == 20
    assert observed.median_observed_duration_seconds == 20
    assert observed.p95_observed_duration_seconds == 40
    assert observed.maximum_observed_duration_seconds == 40

    for key in (("observed", True), ("left_censored", True)):
        censored = groups[key]
        assert censored.duration_cycle_count == 0
        assert censored.minimum_observed_duration_seconds is None
        assert censored.median_observed_duration_seconds is None
        assert censored.p95_observed_duration_seconds is None
        assert censored.maximum_observed_duration_seconds is None


@pytest.mark.spark
def test_cycle_inspection_rejects_missing_source_columns(spark: SparkSession) -> None:
    cycles = _inspection_frame(spark).drop("observed_duration_seconds")

    with pytest.raises(CycleInspectionError, match="missing inspection columns"):
        inspect_loaded_cycles(cycles, load_cycle_inspection_query(PROJECT_ROOT))
