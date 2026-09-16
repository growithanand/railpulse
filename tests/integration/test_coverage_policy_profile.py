from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from railpulse.features.coverage_policy_profile import (
    COVERAGE_POLICY_PROFILE_VERSION,
    CoveragePolicyProfileError,
    collect_coverage_policy_counts,
)


def _available(spark: SparkSession):
    schema = StructType(
        [
            StructField("loaded_cycle_id", StringType(), nullable=False),
            StructField("motor_current_15m_observation_count", LongType(), nullable=False),
            StructField("observation_span_seconds", LongType(), nullable=False),
            StructField("leading_unobserved_seconds", LongType(), nullable=False),
            StructField("first_observation_follows_forward_gap", BooleanType(), nullable=False),
            StructField("maximum_internal_forward_gap_seconds", LongType(), nullable=True),
        ]
    )
    return spark.createDataFrame(
        [
            ("clean-positive", 91, 892, 8, False, None),
            ("tail-positive", 74, 892, 8, False, None),
            ("material-gap-positive", 91, 892, 8, False, 20),
            ("large-gap-negative", 91, 892, 8, False, 150),
            ("tail-and-gap-negative", 74, 890, 130, True, None),
            ("clean-negative", 91, 892, 8, False, None),
            ("clean-null", 91, 892, 8, False, None),
        ],
        schema=schema,
    )


def _labels(spark: SparkSession):
    schema = StructType(
        [
            StructField("loaded_cycle_id", StringType(), nullable=False),
            StructField("failure_horizon_status", StringType(), nullable=False),
        ]
    )
    return spark.createDataFrame(
        [
            ("clean-positive", "positive"),
            ("tail-positive", "positive"),
            ("material-gap-positive", "positive"),
            ("large-gap-negative", "negative"),
            ("tail-and-gap-negative", "negative"),
            ("clean-negative", "negative"),
            ("clean-null", "horizon_censored"),
            ("feature-unavailable", "missing_prediction_boundary"),
        ],
        schema=schema,
    )


@pytest.mark.spark
def test_coverage_policy_counts_reconcile_features_and_labels(spark: SparkSession) -> None:
    comparison = collect_coverage_policy_counts(
        _available(spark),
        _labels(spark),
        p05_observation_count=75,
        p05_observation_span_seconds=891,
    )
    policies = {policy.policy_id: policy for policy in comparison.policies}

    assert COVERAGE_POLICY_PROFILE_VERSION == "motor-current-coverage-policy-profile-v1"
    assert comparison.available_feature_cycle_count == 7
    assert comparison.p05_observation_count == 75
    assert comparison.p05_observation_span_seconds == 891
    assert comparison.available_positive_cycle_count == 3
    assert comparison.available_negative_cycle_count == 3
    assert comparison.available_null_label_cycle_count == 1

    baseline = policies["available_baseline"]
    assert baseline.retained_feature_cycle_count == 7
    assert baseline.excluded_feature_cycle_count == 0

    strict_tail = policies["exclude_strict_tail"]
    assert strict_tail.retained_feature_cycle_count == 5
    assert strict_tail.excluded_feature_cycle_count == 2
    assert strict_tail.retained_positive_cycle_count == 2
    assert strict_tail.retained_negative_cycle_count == 2
    assert strict_tail.retained_null_label_cycle_count == 1
    assert strict_tail.excluded_positive_cycle_count == 1
    assert strict_tail.excluded_negative_cycle_count == 1
    assert strict_tail.excluded_null_label_cycle_count == 0

    material_gap = policies["exclude_material_gap_20s"]
    assert material_gap.retained_feature_cycle_count == 4
    assert material_gap.excluded_feature_cycle_count == 3
    assert material_gap.retained_positive_cycle_count == 2
    assert material_gap.retained_negative_cycle_count == 1
    assert material_gap.retained_null_label_cycle_count == 1

    combined_material = policies["exclude_strict_tail_or_material_gap_20s"]
    assert combined_material.retained_feature_cycle_count == 3
    assert combined_material.excluded_feature_cycle_count == 4
    assert combined_material.retained_positive_cycle_count == 1
    assert combined_material.retained_negative_cycle_count == 1
    assert combined_material.retained_null_label_cycle_count == 1

    combined_large = policies["exclude_strict_tail_or_gap_120s"]
    assert combined_large.retained_feature_cycle_count == 4
    assert combined_large.excluded_feature_cycle_count == 3
    assert combined_large.retained_positive_cycle_count == 2
    assert combined_large.retained_negative_cycle_count == 1
    assert combined_large.retained_null_label_cycle_count == 1


@pytest.mark.spark
def test_coverage_policy_counts_reject_incomplete_or_duplicate_labels(
    spark: SparkSession,
) -> None:
    labels = _labels(spark)
    missing = labels.where("loaded_cycle_id <> 'clean-positive'")
    with pytest.raises(CoveragePolicyProfileError, match="requires one failure horizon"):
        collect_coverage_policy_counts(
            _available(spark),
            missing,
            p05_observation_count=75,
            p05_observation_span_seconds=891,
        )

    duplicate = labels.unionByName(labels.where("loaded_cycle_id = 'clean-positive'"))
    with pytest.raises(CoveragePolicyProfileError, match="unique non-null cycle IDs"):
        collect_coverage_policy_counts(
            _available(spark),
            duplicate,
            p05_observation_count=75,
            p05_observation_span_seconds=891,
        )
