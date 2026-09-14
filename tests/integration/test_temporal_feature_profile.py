from __future__ import annotations

from datetime import datetime

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

from railpulse.features.temporal_feature_profile import (
    MOTOR_CURRENT_FEATURE_PROFILE_VERSION,
    MotorCurrentFeatureProfileError,
    _collect_count_only_internal_gaps,
    _collect_gap_tail_comparison,
    _collect_one_sided_tail_examples,
    _collect_strict_tail_overlap,
    _with_internal_gap_context,
    collect_motor_current_feature_profile,
)
from railpulse.features.temporal_features import MOTOR_CURRENT_FEATURE_VERSION
from railpulse.validation.silver_storage import TELEMETRY_VALIDATION_VERSION


def _cycles(spark: SparkSession):
    schema = StructType(
        [
            StructField("loaded_cycle_id", StringType(), nullable=False),
            StructField("loaded_cycle_stop_timestamp", TimestampNTZType(), nullable=True),
        ]
    )
    return spark.createDataFrame(
        [
            ("available-three", datetime(2020, 1, 1, 10, 0)),
            ("available-one", datetime(2020, 1, 1, 10, 20)),
            ("missing-observation", datetime(2020, 1, 1, 11, 0)),
            ("missing-boundary", None),
        ],
        schema=schema,
    )


def _telemetry(spark: SparkSession):
    schema = StructType(
        [
            StructField("event_timestamp", TimestampNTZType(), nullable=False),
            StructField("motor_current", DoubleType(), nullable=False),
            StructField("interval_seconds", LongType(), nullable=True),
            StructField("is_forward_gap", BooleanType(), nullable=False),
            StructField("dataset_version", StringType(), nullable=False),
            StructField("source_sha256", StringType(), nullable=False),
            StructField("ingestion_batch_id", StringType(), nullable=False),
        ]
    )
    return spark.createDataFrame(
        [
            (
                datetime(2020, 1, 1, 9, 45),
                9.0,
                None,
                False,
                "fixture-v1",
                "source-sha",
                "batch-id",
            ),
            (
                datetime(2020, 1, 1, 9, 59, 40),
                2.0,
                880,
                True,
                "fixture-v1",
                "source-sha",
                "batch-id",
            ),
            (
                datetime(2020, 1, 1, 9, 59, 50),
                4.0,
                10,
                False,
                "fixture-v1",
                "source-sha",
                "batch-id",
            ),
            (
                datetime(2020, 1, 1, 10, 0),
                6.0,
                1,
                False,
                "fixture-v1",
                "source-sha",
                "batch-id",
            ),
            (
                datetime(2020, 1, 1, 10, 5, 1),
                8.0,
                301,
                True,
                "fixture-v1",
                "source-sha",
                "batch-id",
            ),
            (
                datetime(2020, 1, 1, 10, 20),
                7.0,
                899,
                True,
                "fixture-v1",
                "source-sha",
                "batch-id",
            ),
        ],
        schema=schema,
    )


@pytest.mark.spark
def test_motor_current_feature_profile_reconciles_status_and_support(
    spark: SparkSession,
) -> None:
    profile = collect_motor_current_feature_profile(_cycles(spark), _telemetry(spark))
    statuses = {item.status: item.cycle_count for item in profile.status_counts}
    support = profile.available_window_support
    tail = profile.low_support_tail
    span_tail = profile.low_span_tail
    overlap = profile.strict_tail_overlap
    one_sided = profile.one_sided_tail_examples
    internal_gaps = profile.count_only_internal_gaps
    comparison = profile.gap_tail_comparison

    assert profile.profile_version == MOTOR_CURRENT_FEATURE_PROFILE_VERSION
    assert profile.feature_version == MOTOR_CURRENT_FEATURE_VERSION
    assert profile.window_seconds == 900
    assert profile.telemetry_validation_version == TELEMETRY_VALIDATION_VERSION
    assert profile.dataset_version == "fixture-v1"
    assert profile.telemetry_source_sha256 == "source-sha"
    assert profile.telemetry_ingestion_batch_id == "batch-id"
    assert profile.accepted_telemetry_record_count == 6
    assert profile.cycle_count == 4
    assert statuses == {
        "available": 2,
        "missing_prediction_boundary": 1,
        "missing_prediction_observation": 1,
    }
    assert support.available_cycle_count == 2
    assert support.minimum_observation_count == 2
    assert support.p05_observation_count == 2
    assert support.median_observation_count == 2
    assert support.p95_observation_count == 3
    assert support.maximum_observation_count == 3
    assert support.minimum_observation_span_seconds == 20
    assert support.p05_observation_span_seconds == 20
    assert support.median_observation_span_seconds == 20
    assert support.p95_observation_span_seconds == 899
    assert support.maximum_observation_span_seconds == 899
    assert tail.p05_observation_count == 2
    assert tail.cycle_count_below_p05 == 0
    assert tail.cycle_count_equal_to_p05 == 1
    assert tail.cycle_count_at_or_below_p05 == 1
    assert tail.example_limit == 10
    assert len(tail.examples) == 1
    example = tail.examples[0]
    assert example.loaded_cycle_id == "available-one"
    assert example.prediction_timestamp == "2020-01-01 10:20:00"
    assert example.observation_count == 2
    assert example.first_observation_timestamp == "2020-01-01 10:05:01"
    assert example.observation_span_seconds == 899
    assert example.leading_unobserved_seconds == 1
    assert example.first_observation_follows_forward_gap is True
    assert example.preceding_interval_seconds == 301

    assert span_tail.p05_observation_span_seconds == 20
    assert span_tail.cycle_count_below_p05 == 0
    assert span_tail.cycle_count_equal_to_p05 == 1
    assert span_tail.cycle_count_at_or_below_p05 == 1
    assert span_tail.example_limit == 10
    assert len(span_tail.examples) == 1
    span_example = span_tail.examples[0]
    assert span_example.loaded_cycle_id == "available-three"
    assert span_example.prediction_timestamp == "2020-01-01 10:00:00"
    assert span_example.observation_count == 3
    assert span_example.first_observation_timestamp == "2020-01-01 09:59:40"
    assert span_example.observation_span_seconds == 20
    assert span_example.leading_unobserved_seconds == 880
    assert span_example.first_observation_follows_forward_gap is True
    assert span_example.preceding_interval_seconds == 880

    assert overlap.p05_observation_count == 2
    assert overlap.p05_observation_span_seconds == 20
    assert overlap.cycle_count_below_both == 0
    assert overlap.cycle_count_below_count_only == 0
    assert overlap.cycle_count_below_span_only == 0
    assert overlap.cycle_count_below_either == 0
    assert one_sided.example_limit == 10
    assert one_sided.count_only_examples == ()
    assert one_sided.span_only_examples == ()
    assert internal_gaps.count_only_cycle_count == 0
    assert internal_gaps.cycle_count_with_internal_forward_gap == 0
    assert internal_gaps.cycle_count_without_internal_forward_gap == 0
    assert internal_gaps.internal_forward_gap_count == 0
    assert internal_gaps.maximum_internal_forward_gap_seconds is None
    assert comparison.available_cycle_count == 2
    assert comparison.strict_tail_union_cycle_count == 0
    assert comparison.gap_intersecting_cycle_count == 2
    assert comparison.cycle_count_in_tail_and_gap == 0
    assert comparison.cycle_count_in_tail_only == 0
    assert comparison.cycle_count_in_gap_only == 2
    assert comparison.cycle_count_in_neither == 0
    assert comparison.cycle_count_with_leading_forward_gap == 2
    assert comparison.cycle_count_with_internal_forward_gap == 1


@pytest.mark.spark
def test_strict_tail_overlap_and_examples_distinguish_membership(
    spark: SparkSession,
) -> None:
    available = spark.createDataFrame(
        [
            (
                "both",
                datetime(2020, 1, 1, 10, 0),
                74,
                datetime(2020, 1, 1, 9, 45, 10),
                890,
                10,
                True,
                1000,
            ),
            (
                "count-only-a",
                datetime(2020, 1, 1, 10, 20),
                74,
                datetime(2020, 1, 1, 10, 5, 9),
                891,
                9,
                False,
                10,
            ),
            (
                "count-only-b",
                datetime(2020, 1, 1, 12, 30),
                73,
                datetime(2020, 1, 1, 12, 15, 1),
                899,
                1,
                True,
                500,
            ),
            (
                "span-only-a",
                datetime(2020, 1, 1, 10, 40),
                75,
                datetime(2020, 1, 1, 10, 25, 10),
                890,
                10,
                True,
                901,
            ),
            (
                "span-only-b",
                datetime(2020, 1, 1, 10, 50),
                91,
                datetime(2020, 1, 1, 10, 35, 11),
                889,
                11,
                True,
                902,
            ),
            (
                "gap-only",
                datetime(2020, 1, 1, 11, 0),
                75,
                datetime(2020, 1, 1, 10, 45, 9),
                891,
                9,
                True,
                1000,
            ),
            (
                "neither",
                datetime(2020, 1, 1, 11, 20),
                91,
                datetime(2020, 1, 1, 11, 5, 1),
                899,
                1,
                False,
                10,
            ),
        ],
        schema=StructType(
            [
                StructField("loaded_cycle_id", StringType(), nullable=False),
                StructField("prediction_timestamp", TimestampNTZType(), nullable=False),
                StructField(
                    "motor_current_15m_observation_count",
                    LongType(),
                    nullable=False,
                ),
                StructField(
                    "motor_current_15m_first_observation_timestamp",
                    TimestampNTZType(),
                    nullable=False,
                ),
                StructField("observation_span_seconds", LongType(), nullable=False),
                StructField("leading_unobserved_seconds", LongType(), nullable=False),
                StructField(
                    "first_observation_follows_forward_gap",
                    BooleanType(),
                    nullable=False,
                ),
                StructField("preceding_interval_seconds", LongType(), nullable=True),
            ]
        ),
    )

    overlap = _collect_strict_tail_overlap(
        available,
        p05_observation_count=75,
        p05_observation_span_seconds=891,
    )

    assert overlap.p05_observation_count == 75
    assert overlap.p05_observation_span_seconds == 891
    assert overlap.cycle_count_below_both == 1
    assert overlap.cycle_count_below_count_only == 2
    assert overlap.cycle_count_below_span_only == 2
    assert overlap.cycle_count_below_either == 5

    examples = _collect_one_sided_tail_examples(
        available,
        p05_observation_count=75,
        p05_observation_span_seconds=891,
    )

    assert examples.example_limit == 10
    assert [item.loaded_cycle_id for item in examples.count_only_examples] == [
        "count-only-b",
        "count-only-a",
    ]
    assert examples.count_only_examples[0].observation_count == 73
    assert examples.count_only_examples[0].observation_span_seconds == 899
    assert examples.count_only_examples[0].leading_unobserved_seconds == 1
    assert examples.count_only_examples[0].first_observation_follows_forward_gap is True
    assert [item.loaded_cycle_id for item in examples.span_only_examples] == [
        "span-only-b",
        "span-only-a",
    ]
    assert examples.span_only_examples[0].observation_count == 91
    assert examples.span_only_examples[0].observation_span_seconds == 889
    assert examples.span_only_examples[0].leading_unobserved_seconds == 11
    assert examples.span_only_examples[0].first_observation_follows_forward_gap is True
    assert examples.span_only_examples[0].preceding_interval_seconds == 902

    gap_telemetry = spark.createDataFrame(
        [
            (datetime(2020, 1, 1, 10, 25, 10), True, 901),
            (datetime(2020, 1, 1, 12, 15, 1), True, 500),
            (datetime(2020, 1, 1, 12, 20), True, 300),
            (datetime(2020, 1, 1, 12, 25), True, 400),
        ],
        schema=StructType(
            [
                StructField("event_timestamp", TimestampNTZType(), nullable=False),
                StructField("is_forward_gap", BooleanType(), nullable=False),
                StructField("interval_seconds", LongType(), nullable=False),
            ]
        ),
    )
    gap_context = _with_internal_gap_context(available, gap_telemetry)
    internal_gaps = _collect_count_only_internal_gaps(
        gap_context,
        p05_observation_count=75,
        p05_observation_span_seconds=891,
    )

    assert internal_gaps.count_only_cycle_count == 2
    assert internal_gaps.cycle_count_with_internal_forward_gap == 1
    assert internal_gaps.cycle_count_without_internal_forward_gap == 1
    assert internal_gaps.internal_forward_gap_count == 2
    assert internal_gaps.maximum_internal_forward_gap_seconds == 400

    comparison = _collect_gap_tail_comparison(
        gap_context,
        p05_observation_count=75,
        p05_observation_span_seconds=891,
    )

    assert comparison.available_cycle_count == 7
    assert comparison.strict_tail_union_cycle_count == 5
    assert comparison.gap_intersecting_cycle_count == 5
    assert comparison.cycle_count_in_tail_and_gap == 4
    assert comparison.cycle_count_in_tail_only == 1
    assert comparison.cycle_count_in_gap_only == 1
    assert comparison.cycle_count_in_neither == 1
    assert comparison.cycle_count_with_leading_forward_gap == 5
    assert comparison.cycle_count_with_internal_forward_gap == 1


@pytest.mark.spark
def test_motor_current_feature_profile_rejects_missing_lineage_and_duplicate_cycles(
    spark: SparkSession,
) -> None:
    with pytest.raises(MotorCurrentFeatureProfileError, match="missing feature-profile columns"):
        collect_motor_current_feature_profile(
            _cycles(spark),
            _telemetry(spark).drop("source_sha256"),
        )

    duplicate_cycles = _cycles(spark).unionByName(_cycles(spark).limit(1))
    with pytest.raises(MotorCurrentFeatureProfileError, match="unique non-null cycle IDs"):
        collect_motor_current_feature_profile(duplicate_cycles, _telemetry(spark))
