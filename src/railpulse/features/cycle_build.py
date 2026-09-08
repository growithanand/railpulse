"""Build and persist the full-source Gold loaded-cycle table."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from railpulse.config import RailPulseConfig, load_config
from railpulse.features.cycle_profile import (
    FullSourceCycleProfile,
    collect_full_source_cycle_profile,
)
from railpulse.features.cycle_storage import GoldCycleWriteResult, persist_loaded_cycles
from railpulse.features.cycles import (
    aggregate_loaded_cycles,
    annotate_loaded_cycle_boundaries,
    assign_loaded_cycle_segments,
)
from railpulse.ingestion.bronze import TELEMETRY_TABLE, bronze_table_path
from railpulse.spark import create_local_spark_session
from railpulse.validation.silver_telemetry import (
    REJECTION_REASONS_FIELD,
    TelemetryQualitySplit,
    split_telemetry_by_quality,
)

CYCLE_BUILD_VERSION = "loaded-cycle-build-v1"


class CycleBuildError(RuntimeError):
    """Raised when a full-source Gold cycle build cannot be reconciled."""


@dataclass(frozen=True)
class FullSourceCycleBuildResult:
    """Reconciled profile and Gold write evidence for one source snapshot."""

    build_version: str
    profile: FullSourceCycleProfile
    write: GoldCycleWriteResult


def build_full_source_cycles(
    spark: SparkSession,
    config: RailPulseConfig,
) -> FullSourceCycleBuildResult:
    """Rebuild validated telemetry from Bronze and merge its cycles into Gold."""

    bronze = spark.read.format("delta").load(str(bronze_table_path(config, TELEMETRY_TABLE)))
    split = split_telemetry_by_quality(bronze)
    validated = split.all_records.persist(StorageLevel.DISK_ONLY)
    cycles = None
    try:
        reason_count = F.size(F.col(REJECTION_REASONS_FIELD))
        cached_split = TelemetryQualitySplit(
            all_records=validated,
            accepted=validated.where(reason_count == 0),
            quarantined=validated.where(reason_count > 0),
        )
        boundaries = annotate_loaded_cycle_boundaries(cached_split.accepted)
        segments = assign_loaded_cycle_segments(boundaries)
        cycles = aggregate_loaded_cycles(segments).persist(StorageLevel.DISK_ONLY)
        profile = collect_full_source_cycle_profile(cached_split, cycles, config)
        write = persist_loaded_cycles(cycles, config)
        if write.source_cycle_count != profile.loaded_cycles.cycle_count:
            raise CycleBuildError(
                "Profile and Gold write cycle counts do not reconcile: "
                f"profile={profile.loaded_cycles.cycle_count}, "
                f"write={write.source_cycle_count}"
            )
        return FullSourceCycleBuildResult(
            build_version=CYCLE_BUILD_VERSION,
            profile=profile,
            write=write,
        )
    finally:
        if cycles is not None:
            cycles.unpersist()
        validated.unpersist()


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--project-root", type=Path, default=None)
    parser.add_argument("--master", default="local[4]")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the full-source Gold cycle build and print reconciled JSON."""

    args = _parse_args(argv)
    config = load_config(args.config, project_root=args.project_root)
    spark = create_local_spark_session("railpulse-cycle-build", master=args.master)
    try:
        result = build_full_source_cycles(spark, config)
    finally:
        spark.stop()
    print(json.dumps(asdict(result), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
