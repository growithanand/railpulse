from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from railpulse.features.feature_eligibility import (
    MOTOR_CURRENT_ELIGIBILITY_VERSION,
    MOTOR_CURRENT_MINIMUM_EXCLUDED_GAP_SECONDS,
    REASON_MATERIAL_WINDOW_GAP,
    REASON_MISSING_OR_INVALID_CONTEXT,
    REASON_MISSING_PREDICTION,
    REASON_MISSING_PREDICTION_OBSERVATION,
    STATUS_ELIGIBLE,
    STATUS_INELIGIBLE,
    FeatureEligibilityError,
    add_motor_current_eligibility,
)
from railpulse.validation.silver_telemetry import MATERIAL_GAP_SECONDS


def _features(spark: SparkSession):
    schema = StructType(
        [
            StructField("loaded_cycle_id", StringType(), nullable=False),
            StructField("motor_current_15m_status", StringType(), nullable=False),
            StructField("failure_horizon_status", StringType(), nullable=True),
        ]
    )
    return spark.createDataFrame(
        [
            ("clean", "available", "positive"),
            ("leading-small", "available", "positive"),
            ("internal-small", "available", "negative"),
            ("internal-threshold", "available", "positive"),
            ("leading-threshold", "available", "negative"),
            ("both-gaps", "available", "negative"),
            ("missing-boundary", "missing_prediction_boundary", None),
            ("missing-observation", "missing_prediction_observation", None),
            ("missing-context", "available", "negative"),
        ],
        schema=schema,
    )


def _coverage_context(spark: SparkSession):
    schema = StructType(
        [
            StructField("loaded_cycle_id", StringType(), nullable=False),
            StructField("leading_unobserved_seconds", LongType(), nullable=False),
            StructField("first_observation_follows_forward_gap", BooleanType(), nullable=False),
            StructField("maximum_internal_forward_gap_seconds", LongType(), nullable=True),
        ]
    )
    return spark.createDataFrame(
        [
            ("clean", 8, False, None),
            ("leading-small", 8, True, None),
            ("internal-small", 8, False, 19),
            ("internal-threshold", 8, False, 20),
            ("leading-threshold", 20, True, None),
            ("both-gaps", 20, True, 120),
        ],
        schema=schema,
    )


@pytest.mark.spark
def test_motor_current_eligibility_uses_only_availability_and_gap_context(
    spark: SparkSession,
) -> None:
    result = add_motor_current_eligibility(_features(spark), _coverage_context(spark))
    rows = {row.loaded_cycle_id: row for row in result.collect()}

    for cycle_id in ("clean", "leading-small", "internal-small"):
        assert rows[cycle_id].motor_current_eligibility_status == STATUS_ELIGIBLE
        assert rows[cycle_id].motor_current_eligibility_reasons == []
    assert rows["clean"].motor_current_maximum_window_gap_seconds == 0
    assert rows["leading-small"].motor_current_maximum_window_gap_seconds == 8
    assert rows["internal-small"].motor_current_maximum_window_gap_seconds == 19

    for cycle_id, maximum_gap in (
        ("internal-threshold", 20),
        ("leading-threshold", 20),
        ("both-gaps", 120),
    ):
        assert rows[cycle_id].motor_current_eligibility_status == STATUS_INELIGIBLE
        assert rows[cycle_id].motor_current_eligibility_reasons == [REASON_MATERIAL_WINDOW_GAP]
        assert rows[cycle_id].motor_current_maximum_window_gap_seconds == maximum_gap

    assert rows["missing-boundary"].motor_current_eligibility_reasons == [REASON_MISSING_PREDICTION]
    assert rows["missing-observation"].motor_current_eligibility_reasons == [
        REASON_MISSING_PREDICTION_OBSERVATION
    ]
    assert rows["missing-context"].motor_current_eligibility_reasons == [
        REASON_MISSING_OR_INVALID_CONTEXT
    ]
    for cycle_id in ("missing-boundary", "missing-observation", "missing-context"):
        assert rows[cycle_id].motor_current_eligibility_status == STATUS_INELIGIBLE
        assert rows[cycle_id].motor_current_maximum_window_gap_seconds is None

    assert {row.motor_current_eligibility_version for row in rows.values()} == {
        MOTOR_CURRENT_ELIGIBILITY_VERSION
    }
    assert {
        row.motor_current_eligibility_minimum_excluded_gap_seconds for row in rows.values()
    } == {20}
    assert MOTOR_CURRENT_MINIMUM_EXCLUDED_GAP_SECONDS == MATERIAL_GAP_SECONDS


@pytest.mark.spark
def test_motor_current_eligibility_is_independent_of_horizon_labels(
    spark: SparkSession,
) -> None:
    features = _features(spark)
    original = add_motor_current_eligibility(features, _coverage_context(spark))
    changed_labels = features.withColumn(
        "failure_horizon_status",
        F.when(F.col("failure_horizon_status") == "positive", F.lit("negative")).otherwise(
            F.lit("positive")
        ),
    )
    relabeled = add_motor_current_eligibility(changed_labels, _coverage_context(spark))
    selected_columns = (
        "loaded_cycle_id",
        "motor_current_maximum_window_gap_seconds",
        "motor_current_eligibility_status",
        "motor_current_eligibility_reasons",
    )

    assert (
        original.select(*selected_columns).exceptAll(relabeled.select(*selected_columns)).count()
        == 0
    )
    assert (
        relabeled.select(*selected_columns).exceptAll(original.select(*selected_columns)).count()
        == 0
    )


@pytest.mark.spark
def test_motor_current_eligibility_rejects_missing_or_conflicting_columns(
    spark: SparkSession,
) -> None:
    with pytest.raises(FeatureEligibilityError, match="Coverage context is missing"):
        add_motor_current_eligibility(
            _features(spark),
            _coverage_context(spark).drop("maximum_internal_forward_gap_seconds"),
        )

    conflicting = _features(spark).withColumn("motor_current_eligibility_status", F.lit("old"))
    with pytest.raises(FeatureEligibilityError, match="already contain eligibility columns"):
        add_motor_current_eligibility(conflicting, _coverage_context(spark))
