"""Build and persist full-source Gold feature snapshots version 2."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from railpulse.config import RailPulseConfig, load_config
from railpulse.features.cycle_operating_context import (
    CYCLE_OPERATING_CONTEXT_COLUMNS,
    add_cycle_operating_context,
)
from railpulse.features.cycle_operating_context_profile import (
    FullSourceCycleOperatingContextProfile,
    collect_cycle_operating_context_profile,
)
from railpulse.features.cycle_storage import LOADED_CYCLES_TABLE, gold_table_path
from railpulse.features.feature_eligibility import MOTOR_CURRENT_ELIGIBILITY_COLUMNS
from railpulse.features.feature_snapshot_schema import (
    FEATURE_SNAPSHOT_COLUMNS,
    FEATURE_SNAPSHOT_KEY_COLUMN,
    FEATURE_SNAPSHOT_VERSION,
    FEATURE_SNAPSHOTS_TABLE,
)
from railpulse.features.feature_snapshot_v2 import (
    FeatureSnapshotV2Error,
    build_feature_snapshot_v2,
)
from railpulse.features.feature_snapshot_v2_schema import FEATURE_SNAPSHOT_V2_VERSION
from railpulse.features.feature_snapshot_v2_storage import (
    FeatureSnapshotV2WriteResult,
    persist_feature_snapshots_v2,
)
from railpulse.features.temporal_features import MOTOR_CURRENT_FEATURE_COLUMNS
from railpulse.spark import create_local_spark_session

FEATURE_SNAPSHOT_V2_BUILD_VERSION = "motor-current-15m-cycle-context-snapshot-build-v2"


class FeatureSnapshotV2BuildError(RuntimeError):
    """Raised when a full-source feature-snapshot v2 build cannot be reconciled."""


@dataclass(frozen=True)
class FullSourceFeatureSnapshotV2BuildResult:
    """Cycle-context, v1-preservation, and Delta-write evidence for one build."""

    build_version: str
    source_snapshot_version: str
    snapshot_version: str
    dataset_version: str
    telemetry_source_sha256: str
    telemetry_ingestion_batch_id: str
    context_profile: FullSourceCycleOperatingContextProfile
    source_snapshot_count: int
    source_snapshot_count_after: int
    write: FeatureSnapshotV2WriteResult


def _collect_source_lineage(
    snapshots: DataFrame,
    config: RailPulseConfig,
) -> tuple[int, str, str, str]:
    missing_columns = sorted(set(FEATURE_SNAPSHOT_COLUMNS) - set(snapshots.columns))
    if missing_columns:
        raise FeatureSnapshotV2BuildError(
            "Gold feature snapshots v1 are missing contract columns: " + ", ".join(missing_columns)
        )

    summary = snapshots.agg(
        F.count(F.lit(1)).alias("snapshot_count"),
        F.countDistinct(FEATURE_SNAPSHOT_KEY_COLUMN).alias("distinct_snapshot_count"),
    ).first()
    snapshot_count = int(summary.snapshot_count)
    if snapshot_count <= 0:
        raise FeatureSnapshotV2BuildError("Feature snapshot v2 requires Gold v1 snapshots")
    if int(summary.distinct_snapshot_count) != snapshot_count:
        raise FeatureSnapshotV2BuildError("Gold feature snapshots v1 require unique cycle IDs")

    invalid_version = snapshots.where(
        ~F.col("feature_snapshot_version").eqNullSafe(F.lit(FEATURE_SNAPSHOT_VERSION))
    ).limit(1)
    if invalid_version.count():
        raise FeatureSnapshotV2BuildError(
            "Gold feature snapshots v1 contain an unsupported version"
        )

    lineage_rows = (
        snapshots.select(
            "dataset_version",
            "telemetry_source_sha256",
            "telemetry_ingestion_batch_id",
        )
        .distinct()
        .limit(2)
        .collect()
    )
    if len(lineage_rows) != 1:
        raise FeatureSnapshotV2BuildError("Gold feature snapshots v1 require one source lineage")
    lineage = lineage_rows[0]
    values = (
        lineage.dataset_version,
        lineage.telemetry_source_sha256,
        lineage.telemetry_ingestion_batch_id,
    )
    if any(not isinstance(value, str) or not value.strip() for value in values):
        raise FeatureSnapshotV2BuildError("Gold feature snapshots v1 contain incomplete lineage")
    if lineage.dataset_version != config.dataset_version:
        raise FeatureSnapshotV2BuildError(
            "Gold feature snapshots v1 do not match the configured dataset version"
        )
    return snapshot_count, values[0], values[1], values[2]


def _assert_source_snapshot_unchanged(
    before: DataFrame,
    spark: SparkSession,
    config: RailPulseConfig,
    expected_count: int,
) -> int:
    after = spark.read.format("delta").load(str(gold_table_path(config, FEATURE_SNAPSHOTS_TABLE)))
    missing_columns = sorted(set(FEATURE_SNAPSHOT_COLUMNS) - set(after.columns))
    if missing_columns:
        raise FeatureSnapshotV2BuildError(
            "Gold feature snapshots v1 changed schema during the v2 build"
        )
    after = after.select(*FEATURE_SNAPSHOT_COLUMNS)
    after_count = after.count()
    changed = before.exceptAll(after).limit(1).count() or after.exceptAll(before).limit(1).count()
    if after_count != expected_count or changed:
        raise FeatureSnapshotV2BuildError("Gold feature snapshots v1 changed during the v2 build")
    return after_count


def build_full_source_feature_snapshots_v2(
    spark: SparkSession,
    config: RailPulseConfig,
) -> FullSourceFeatureSnapshotV2BuildResult:
    """Expand immutable v1 snapshots with cycle context and persist version 2."""

    source_path = str(gold_table_path(config, FEATURE_SNAPSHOTS_TABLE))
    cycle_path = str(gold_table_path(config, LOADED_CYCLES_TABLE))
    source = (
        spark.read.format("delta")
        .load(source_path)
        .select(*FEATURE_SNAPSHOT_COLUMNS)
        .persist(StorageLevel.DISK_ONLY)
    )
    context = None
    try:
        source_count, dataset_version, source_sha256, ingestion_batch_id = _collect_source_lineage(
            source, config
        )
        cycles = spark.read.format("delta").load(cycle_path)
        context_profile = collect_cycle_operating_context_profile(
            cycles,
            dataset_version=dataset_version,
        )
        if context_profile.cycle_count != source_count:
            raise FeatureSnapshotV2BuildError(
                "Gold cycles and feature snapshots v1 require equal populations: "
                f"cycles={context_profile.cycle_count}, snapshots={source_count}"
            )

        context = (
            add_cycle_operating_context(cycles)
            .select(FEATURE_SNAPSHOT_KEY_COLUMN, *CYCLE_OPERATING_CONTEXT_COLUMNS)
            .persist(StorageLevel.DISK_ONLY)
        )
        features = source.select(
            FEATURE_SNAPSHOT_KEY_COLUMN,
            *MOTOR_CURRENT_FEATURE_COLUMNS,
            *MOTOR_CURRENT_ELIGIBILITY_COLUMNS,
        )
        try:
            snapshots = build_feature_snapshot_v2(
                features,
                context,
                dataset_version=dataset_version,
                telemetry_source_sha256=source_sha256,
                telemetry_ingestion_batch_id=ingestion_batch_id,
            )
        except FeatureSnapshotV2Error as exc:
            raise FeatureSnapshotV2BuildError(str(exc)) from exc

        write = persist_feature_snapshots_v2(snapshots, config)
        if write.source_snapshot_count != source_count:
            raise FeatureSnapshotV2BuildError(
                "Feature snapshot v1 and v2 write counts do not reconcile: "
                f"v1={source_count}, v2={write.source_snapshot_count}"
            )
        source_count_after = _assert_source_snapshot_unchanged(
            source,
            spark,
            config,
            source_count,
        )
        return FullSourceFeatureSnapshotV2BuildResult(
            build_version=FEATURE_SNAPSHOT_V2_BUILD_VERSION,
            source_snapshot_version=FEATURE_SNAPSHOT_VERSION,
            snapshot_version=FEATURE_SNAPSHOT_V2_VERSION,
            dataset_version=dataset_version,
            telemetry_source_sha256=source_sha256,
            telemetry_ingestion_batch_id=ingestion_batch_id,
            context_profile=context_profile,
            source_snapshot_count=source_count,
            source_snapshot_count_after=source_count_after,
            write=write,
        )
    finally:
        if context is not None:
            context.unpersist()
        source.unpersist()


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--project-root", type=Path, default=None)
    parser.add_argument("--master", default="local[4]")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the full-source snapshot v2 build and print reconciled JSON."""

    args = _parse_args(argv)
    config = load_config(args.config, project_root=args.project_root)
    spark = create_local_spark_session("railpulse-feature-snapshot-v2-build", master=args.master)
    try:
        result = build_full_source_feature_snapshots_v2(spark, config)
    finally:
        spark.stop()
    print(json.dumps(asdict(result), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
