from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from railpulse.evaluation.modeling_view_profile import (
    MODELING_VIEW_PROFILE_VERSION,
    ModelingViewProfileError,
    collect_modeling_view_summary,
)


def _failure_rows(spark: SparkSession):
    return tuple(
        spark.createDataFrame(
            [
                ("failure-1", 1, datetime(2020, 1, 2), datetime(2020, 1, 2, 1)),
                ("failure-2", 2, datetime(2020, 1, 4), datetime(2020, 1, 4, 1)),
            ],
            "record_id string, source_row long, failure_start timestamp_ntz, "
            "failure_end timestamp_ntz",
        )
        .orderBy("source_row")
        .collect()
    )


def _view(spark: SparkSession):
    return spark.createDataFrame(
        [
            (
                "positive",
                "motor-current-failure-modeling-view-v1",
                "trainable",
                [],
                "positive",
                True,
                datetime(2020, 1, 1, 12),
                "failure-1",
            ),
            (
                "negative",
                "motor-current-failure-modeling-view-v1",
                "trainable",
                [],
                "negative",
                False,
                datetime(2020, 1, 3, 12),
                None,
            ),
            (
                "gap",
                "motor-current-failure-modeling-view-v1",
                "excluded",
                ["feature_ineligible"],
                "negative",
                False,
                datetime(2020, 1, 3, 13),
                None,
            ),
            (
                "missing",
                "motor-current-failure-modeling-view-v1",
                "excluded",
                ["feature_ineligible", "missing_prediction_boundary"],
                "missing_prediction_boundary",
                None,
                None,
                None,
            ),
        ],
        "loaded_cycle_id string, modeling_view_version string, modeling_row_status string, "
        "modeling_exclusion_reasons array<string>, failure_horizon_status string, "
        "failure_within_horizon boolean, prediction_timestamp timestamp_ntz, "
        "matched_failure_record_id string",
    )


@pytest.mark.spark
def test_modeling_view_profile_reconciles_overlapping_reasons_and_event_coverage(
    spark: SparkSession,
) -> None:
    summary = collect_modeling_view_summary(_view(spark), _failure_rows(spark))
    statuses = {item.status: item.cycle_count for item in summary.status_counts}
    reasons = {item.reason: item.cycle_count for item in summary.exclusion_reason_counts}
    labels = {item.status: item.cycle_count for item in summary.trainable_label_counts}
    events = {item.failure_record_id: item for item in summary.failure_event_counts}

    assert MODELING_VIEW_PROFILE_VERSION
    assert summary.cycle_count == 4
    assert summary.earliest_trainable_prediction_timestamp == "2020-01-01 12:00:00"
    assert summary.latest_trainable_prediction_timestamp == "2020-01-03 12:00:00"
    assert statuses == {"trainable": 2, "excluded": 2}
    assert reasons["feature_ineligible"] == 2
    assert reasons["missing_prediction_boundary"] == 1
    assert sum(reasons.values()) == 3
    assert labels == {"positive": 1, "negative": 1}
    assert events["failure-1"].trainable_positive_cycle_count == 1
    assert events["failure-2"].trainable_positive_cycle_count == 0


@pytest.mark.spark
def test_modeling_view_profile_rejects_unexplained_exclusion_and_unknown_event(
    spark: SparkSession,
) -> None:
    unexplained = _view(spark).withColumn(
        "modeling_exclusion_reasons",
        F.when(F.col("loaded_cycle_id") == "gap", F.array()).otherwise(
            F.col("modeling_exclusion_reasons")
        ),
    )
    with pytest.raises(ModelingViewProfileError, match="row status semantics"):
        collect_modeling_view_summary(unexplained, _failure_rows(spark))

    unknown_event = _view(spark).withColumn(
        "matched_failure_record_id",
        F.when(F.col("loaded_cycle_id") == "positive", F.lit("unknown")).otherwise(
            F.col("matched_failure_record_id")
        ),
    )
    with pytest.raises(ModelingViewProfileError, match="unknown accepted failure event"):
        collect_modeling_view_summary(unknown_event, _failure_rows(spark))
