from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from railpulse.evaluation.chronological_split_profile import (
    CHRONOLOGICAL_SPLIT_PROFILE_VERSION,
    ChronologicalSplitProfileError,
    build_full_source_chronological_split_profile,
    collect_chronological_split_comparison,
)
from railpulse.evaluation.modeling_view_profile import FullSourceModelingViewInputs


def _view(spark: SparkSession):
    return spark.createDataFrame(
        [
            ("feb-negative", "trainable", "negative", datetime(2020, 2, 1), None),
            ("apr-positive", "trainable", "positive", datetime(2020, 4, 30), "failure-1"),
            ("may-positive", "trainable", "positive", datetime(2020, 5, 31), "failure-2"),
            ("jun-negative", "trainable", "negative", datetime(2020, 6, 30), None),
            ("jul-positive", "trainable", "positive", datetime(2020, 7, 15), "failure-3"),
            ("aug-negative", "trainable", "negative", datetime(2020, 8, 31), None),
            ("excluded", "excluded", "negative", datetime(2020, 7, 2), None),
        ],
        "loaded_cycle_id string, modeling_row_status string, failure_horizon_status string, "
        "prediction_timestamp timestamp_ntz, matched_failure_record_id string",
    ).withColumn("modeling_view_version", F.lit("motor-current-failure-modeling-view-v1"))


def _failures(spark: SparkSession):
    return tuple(
        spark.createDataFrame(
            [("failure-1",), ("failure-2",), ("failure-3",), ("failure-4",)],
            "record_id string",
        ).collect()
    )


@pytest.mark.spark
def test_split_profile_compares_labels_time_and_events_for_every_candidate(
    spark: SparkSession,
) -> None:
    comparison = collect_chronological_split_comparison(_view(spark), _failures(spark))
    candidates = {candidate.candidate_id: candidate for candidate in comparison.candidates}

    assert comparison.profile_version == CHRONOLOGICAL_SPLIT_PROFILE_VERSION
    assert comparison.trainable_row_count == 6
    assert comparison.accepted_failure_event_count == 4
    assert len(candidates) == 3

    candidate = candidates["validation-2020-06_test-2020-07"]
    periods = {period.partition: period for period in candidate.periods}
    assert (periods["train"].row_count, periods["train"].positive_count) == (3, 2)
    assert periods["train"].represented_failure_record_ids == ("failure-1", "failure-2")
    assert (periods["validation"].row_count, periods["validation"].negative_count) == (1, 1)
    assert (periods["test"].row_count, periods["test"].positive_count) == (2, 1)
    assert periods["test"].represented_failure_event_count == 1
    assert periods["test"].earliest_prediction_timestamp == "2020-07-15 00:00:00"
    assert periods["test"].latest_prediction_timestamp == "2020-08-31 00:00:00"
    assert periods["test"].prediction_span_seconds == 47 * 24 * 60 * 60

    full_profile = build_full_source_chronological_split_profile(
        FullSourceModelingViewInputs(
            view=_view(spark),
            failure_rows=_failures(spark),
            label_observation_end="2020-09-01 03:59:50",
            dataset_version="fixture-v1",
            telemetry_source_sha256="telemetry-sha",
            telemetry_ingestion_batch_id="telemetry-batch",
            failure_source_sha256="failure-sha",
            failure_source_document_sha256="failure-document-sha",
            failure_ingestion_batch_id="failure-batch",
            accepted_telemetry_record_count=100,
        )
    )
    assert full_profile.dataset_version == "fixture-v1"
    assert full_profile.accepted_telemetry_record_count == 100
    assert full_profile.comparison.trainable_row_count == 6


@pytest.mark.spark
def test_split_profile_rejects_duplicate_cycles_and_unknown_failure_events(
    spark: SparkSession,
) -> None:
    view = _view(spark)
    duplicated = view.unionByName(view.where(F.col("loaded_cycle_id") == "feb-negative"))
    with pytest.raises(ChronologicalSplitProfileError, match="unique cycle IDs"):
        collect_chronological_split_comparison(duplicated, _failures(spark))

    unknown = view.withColumn(
        "matched_failure_record_id",
        F.when(F.col("loaded_cycle_id") == "apr-positive", F.lit("unknown")).otherwise(
            F.col("matched_failure_record_id")
        ),
    )
    with pytest.raises(ChronologicalSplitProfileError, match="unknown accepted failure event"):
        collect_chronological_split_comparison(unknown, _failures(spark))
