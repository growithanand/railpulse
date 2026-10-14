"""Profile the full-source cycle-context modelling view without writing data."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

from pyspark.sql import SparkSession

from railpulse.config import RailPulseConfig, load_config
from railpulse.evaluation.modeling_view_profile import (
    FullSourceModelingViewInputs,
    ModelingViewSummary,
    build_full_source_modeling_view_sources,
    collect_modeling_view_summary,
)
from railpulse.evaluation.modeling_view_v2 import build_modeling_view_v2
from railpulse.evaluation.modeling_view_v2_schema import (
    MODELING_V2_EXCLUSION_REASON_ORDER,
    MODELING_VIEW_V2_VERSION,
)
from railpulse.features.cycle_operating_context import CYCLE_OPERATING_CONTEXT_VERSION
from railpulse.features.cycle_storage import gold_table_path
from railpulse.features.failure_horizons import (
    DEFAULT_FAILURE_HORIZON_SECONDS,
    FAILURE_HORIZON_VERSION,
)
from railpulse.features.feature_snapshot_v2_schema import (
    FEATURE_SNAPSHOT_V2_VERSION,
    FEATURE_SNAPSHOTS_V2_TABLE,
)
from railpulse.spark import create_local_spark_session

MODELING_VIEW_V2_PROFILE_VERSION = "motor-current-cycle-context-failure-modeling-view-profile-v2"


@dataclass(frozen=True)
class FullSourceModelingViewV2Profile:
    """Source-bound profile of the complete cycle-context modelling view."""

    profile_version: str
    modeling_view_version: str
    feature_snapshot_version: str
    cycle_context_feature_version: str
    failure_horizon_version: str
    failure_horizon_seconds: int
    label_observation_end: str
    dataset_version: str
    telemetry_source_sha256: str
    telemetry_ingestion_batch_id: str
    failure_source_sha256: str
    failure_source_document_sha256: str
    failure_ingestion_batch_id: str
    accepted_telemetry_record_count: int
    accepted_failure_event_count: int
    matched_trainable_failure_event_count: int
    summary: ModelingViewSummary


def build_full_source_modeling_view_v2_inputs(
    spark: SparkSession,
    config: RailPulseConfig,
) -> FullSourceModelingViewInputs:
    """Rebuild labels and join the immutable version 2 snapshot without writing."""

    sources = build_full_source_modeling_view_sources(spark, config)
    snapshots = spark.read.format("delta").load(
        str(gold_table_path(config, FEATURE_SNAPSHOTS_V2_TABLE))
    )
    view = build_modeling_view_v2(
        snapshots,
        sources.horizons,
        failure_dataset_version=sources.dataset_version,
        failure_source_sha256=sources.failure_source_sha256,
        failure_ingestion_batch_id=sources.failure_ingestion_batch_id,
    )
    return FullSourceModelingViewInputs(
        view=view,
        failure_rows=sources.failure_rows,
        label_observation_end=sources.label_observation_end,
        dataset_version=sources.dataset_version,
        telemetry_source_sha256=sources.telemetry_source_sha256,
        telemetry_ingestion_batch_id=sources.telemetry_ingestion_batch_id,
        failure_source_sha256=sources.failure_source_sha256,
        failure_source_document_sha256=sources.failure_source_document_sha256,
        failure_ingestion_batch_id=sources.failure_ingestion_batch_id,
        accepted_telemetry_record_count=sources.accepted_telemetry_record_count,
    )


def build_full_source_modeling_view_v2_profile(
    inputs: FullSourceModelingViewInputs,
) -> FullSourceModelingViewV2Profile:
    """Attach version 2 contracts and lineage to a reconciled view summary."""

    summary = collect_modeling_view_summary(
        inputs.view,
        inputs.failure_rows,
        modeling_view_version=MODELING_VIEW_V2_VERSION,
        exclusion_reason_order=MODELING_V2_EXCLUSION_REASON_ORDER,
    )
    matched_event_count = sum(
        event.trainable_positive_cycle_count > 0 for event in summary.failure_event_counts
    )
    return FullSourceModelingViewV2Profile(
        profile_version=MODELING_VIEW_V2_PROFILE_VERSION,
        modeling_view_version=MODELING_VIEW_V2_VERSION,
        feature_snapshot_version=FEATURE_SNAPSHOT_V2_VERSION,
        cycle_context_feature_version=CYCLE_OPERATING_CONTEXT_VERSION,
        failure_horizon_version=FAILURE_HORIZON_VERSION,
        failure_horizon_seconds=DEFAULT_FAILURE_HORIZON_SECONDS,
        label_observation_end=inputs.label_observation_end,
        dataset_version=inputs.dataset_version,
        telemetry_source_sha256=inputs.telemetry_source_sha256,
        telemetry_ingestion_batch_id=inputs.telemetry_ingestion_batch_id,
        failure_source_sha256=inputs.failure_source_sha256,
        failure_source_document_sha256=inputs.failure_source_document_sha256,
        failure_ingestion_batch_id=inputs.failure_ingestion_batch_id,
        accepted_telemetry_record_count=inputs.accepted_telemetry_record_count,
        accepted_failure_event_count=len(inputs.failure_rows),
        matched_trainable_failure_event_count=matched_event_count,
        summary=summary,
    )


def profile_full_source_modeling_view_v2(
    spark: SparkSession,
    config: RailPulseConfig,
) -> FullSourceModelingViewV2Profile:
    """Build and profile the full-source version 2 modelling view without writing."""

    inputs = build_full_source_modeling_view_v2_inputs(spark, config)
    return build_full_source_modeling_view_v2_profile(inputs)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--project-root", type=Path, default=None)
    parser.add_argument("--master", default="local[4]")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the read-only full-source version 2 profile and print deterministic JSON."""

    args = _parse_args(argv)
    config = load_config(args.config, project_root=args.project_root)
    spark = create_local_spark_session("railpulse-modeling-view-v2-profile", master=args.master)
    try:
        profile = profile_full_source_modeling_view_v2(spark, config)
    finally:
        spark.stop()
    print(json.dumps(asdict(profile), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
