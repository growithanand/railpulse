from __future__ import annotations

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    ByteType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from railpulse.features.cycles import CycleBoundaryError, annotate_loaded_cycle_boundaries

BOUNDARY_FIELDS = (
    "record_id",
    "previous_dv_eletric",
    "is_loaded_operation",
    "is_cycle_start",
    "is_cycle_stop",
    "is_left_censored_cycle",
)


def _cycle_frame(spark: SparkSession) -> DataFrame:
    schema = StructType(
        [
            StructField("record_id", StringType(), nullable=False),
            StructField("source_index", LongType(), nullable=False),
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
        ("initial-active", 0, None, 1, False, []),
        ("continuing-active", 10, 0, 1, False, []),
        ("observed-stop", 20, 10, 0, False, []),
        ("continuing-inactive", 30, 20, 0, False, []),
        ("observed-start", 40, 30, 1, False, []),
        ("active-after-gap", 50, 40, 1, True, []),
        ("stop-after-censored-segment", 60, 50, 0, False, []),
    ]
    return spark.createDataFrame(rows, schema=schema)


@pytest.mark.spark
def test_loaded_cycle_boundaries_preserve_censoring_and_do_not_use_future_rows(
    spark: SparkSession,
) -> None:
    source = _cycle_frame(spark)
    annotated = annotate_loaded_cycle_boundaries(source)
    rows = {row.record_id: row for row in annotated.collect()}

    assert rows["initial-active"].previous_dv_eletric is None
    assert rows["initial-active"].is_loaded_operation is True
    assert rows["initial-active"].is_cycle_start is False
    assert rows["initial-active"].is_left_censored_cycle is True
    assert rows["continuing-active"].is_cycle_start is False
    assert rows["continuing-active"].is_left_censored_cycle is False
    assert rows["observed-stop"].is_cycle_stop is True
    assert rows["continuing-inactive"].is_cycle_start is False
    assert rows["continuing-inactive"].is_cycle_stop is False
    assert rows["observed-start"].is_cycle_start is True
    assert rows["observed-start"].is_left_censored_cycle is False
    assert rows["active-after-gap"].is_cycle_start is False
    assert rows["active-after-gap"].is_left_censored_cycle is True
    assert rows["stop-after-censored-segment"].is_cycle_stop is True

    prefix = source.where(F.col("source_index") <= 40)
    prefix_rows = (
        annotate_loaded_cycle_boundaries(prefix)
        .select(*BOUNDARY_FIELDS)
        .orderBy("record_id")
        .collect()
    )
    full_prefix_rows = (
        annotated.where(F.col("source_index") <= 40)
        .select(*BOUNDARY_FIELDS)
        .orderBy("record_id")
        .collect()
    )
    assert prefix_rows == full_prefix_rows


@pytest.mark.spark
def test_loaded_cycle_boundaries_reject_missing_or_conflicting_columns(
    spark: SparkSession,
) -> None:
    source = _cycle_frame(spark)

    with pytest.raises(CycleBoundaryError, match="missing cycle-boundary columns"):
        annotate_loaded_cycle_boundaries(source.drop("previous_source_index"))

    conflicting = source.withColumn("is_cycle_start", F.lit(False))
    with pytest.raises(CycleBoundaryError, match="already contains cycle-boundary columns"):
        annotate_loaded_cycle_boundaries(conflicting)
