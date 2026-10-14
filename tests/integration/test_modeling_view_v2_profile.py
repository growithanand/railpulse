from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from railpulse.evaluation.modeling_view_profile import (
    FullSourceModelingViewInputs,
    ModelingViewProfileError,
    collect_modeling_view_summary,
)
from railpulse.evaluation.modeling_view_v2_profile import (
    MODELING_VIEW_V2_PROFILE_VERSION,
    build_full_source_modeling_view_v2_profile,
)
from railpulse.evaluation.modeling_view_v2_schema import (
    MODELING_V2_EXCLUSION_REASON_ORDER,
    MODELING_VIEW_V2_VERSION,
)
from railpulse.features.cycle_operating_context import CYCLE_OPERATING_CONTEXT_VERSION
from railpulse.features.feature_snapshot_v2_schema import FEATURE_SNAPSHOT_V2_VERSION


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
                MODELING_VIEW_V2_VERSION,
                "trainable",
                [],
                "positive",
                True,
                datetime(2020, 1, 1, 12),
                "failure-1",
            ),
            (
                "negative",
                MODELING_VIEW_V2_VERSION,
                "trainable",
                [],
                "negative",
                False,
                datetime(2020, 1, 3, 12),
                None,
            ),
            (
                "context-gap",
                MODELING_VIEW_V2_VERSION,
                "excluded",
                ["cycle_context_unavailable"],
                "negative",
                False,
                datetime(2020, 1, 3, 13),
                None,
            ),
            (
                "missing",
                MODELING_VIEW_V2_VERSION,
                "excluded",
                [
                    "feature_ineligible",
                    "cycle_context_unavailable",
                    "missing_prediction_boundary",
                ],
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


def _inputs(spark: SparkSession) -> FullSourceModelingViewInputs:
    return FullSourceModelingViewInputs(
        view=_view(spark),
        failure_rows=_failure_rows(spark),
        label_observation_end="2020-01-05 00:00:00",
        dataset_version="fixture-v1",
        telemetry_source_sha256="telemetry-sha",
        telemetry_ingestion_batch_id="telemetry-batch",
        failure_source_sha256="failure-sha",
        failure_source_document_sha256="failure-document-sha",
        failure_ingestion_batch_id="failure-batch",
        accepted_telemetry_record_count=100,
    )


@pytest.mark.spark
def test_modeling_view_v2_profile_reconciles_context_exclusions_and_lineage(
    spark: SparkSession,
) -> None:
    profile = build_full_source_modeling_view_v2_profile(_inputs(spark))
    statuses = {item.status: item.cycle_count for item in profile.summary.status_counts}
    reasons = {item.reason: item.cycle_count for item in profile.summary.exclusion_reason_counts}
    labels = {item.status: item.cycle_count for item in profile.summary.trainable_label_counts}

    assert profile.profile_version == MODELING_VIEW_V2_PROFILE_VERSION
    assert profile.modeling_view_version == MODELING_VIEW_V2_VERSION
    assert profile.feature_snapshot_version == FEATURE_SNAPSHOT_V2_VERSION
    assert profile.cycle_context_feature_version == CYCLE_OPERATING_CONTEXT_VERSION
    assert profile.dataset_version == "fixture-v1"
    assert profile.accepted_telemetry_record_count == 100
    assert profile.accepted_failure_event_count == 2
    assert profile.matched_trainable_failure_event_count == 1
    assert statuses == {"trainable": 2, "excluded": 2}
    assert reasons["cycle_context_unavailable"] == 2
    assert labels == {"positive": 1, "negative": 1}


@pytest.mark.spark
def test_modeling_view_v2_profile_rejects_wrong_version_and_unknown_reason(
    spark: SparkSession,
) -> None:
    view = _view(spark)
    kwargs = {
        "modeling_view_version": MODELING_VIEW_V2_VERSION,
        "exclusion_reason_order": MODELING_V2_EXCLUSION_REASON_ORDER,
    }

    wrong_version = view.withColumn("modeling_view_version", F.lit("unsupported"))
    with pytest.raises(ModelingViewProfileError, match="row status semantics"):
        collect_modeling_view_summary(wrong_version, _failure_rows(spark), **kwargs)

    unknown_reason = view.withColumn(
        "modeling_exclusion_reasons",
        F.when(
            F.col("loaded_cycle_id") == "context-gap",
            F.array(F.lit("unknown")),
        ).otherwise(F.col("modeling_exclusion_reasons")),
    )
    with pytest.raises(ModelingViewProfileError, match="unexpected exclusion reasons"):
        collect_modeling_view_summary(unknown_reason, _failure_rows(spark), **kwargs)
