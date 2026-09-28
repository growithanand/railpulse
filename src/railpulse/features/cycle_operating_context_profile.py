"""Profile full-source cycle operating-context coverage without loading labels."""

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
    CYCLE_OPERATING_CONTEXT_VERSION,
    STATUS_AVAILABLE,
    STATUS_INCOMPLETE_CURRENT,
    STATUS_INCOMPLETE_PREVIOUS,
    STATUS_INVALID_PREVIOUS_INTERVAL,
    STATUS_LEFT_CENSORED_CURRENT,
    STATUS_MISSING_PREDICTION,
    STATUS_MISSING_PREVIOUS,
    add_cycle_operating_context,
)
from railpulse.features.cycle_storage import LOADED_CYCLES_TABLE, gold_table_path
from railpulse.features.cycles import LOADED_CYCLE_ID_VERSION
from railpulse.spark import create_local_spark_session

CYCLE_OPERATING_CONTEXT_PROFILE_VERSION = "loaded-cycle-operating-context-profile-v1"
CYCLE_OPERATING_CONTEXT_STATUS_ORDER = (
    STATUS_AVAILABLE,
    STATUS_MISSING_PREDICTION,
    STATUS_LEFT_CENSORED_CURRENT,
    STATUS_INCOMPLETE_CURRENT,
    STATUS_MISSING_PREVIOUS,
    STATUS_INCOMPLETE_PREVIOUS,
    STATUS_INVALID_PREVIOUS_INTERVAL,
)
_DISTRIBUTION_COLUMNS = (
    ("current_duration_seconds", "cycle_context_current_duration_seconds"),
    ("previous_duration_seconds", "cycle_context_previous_duration_seconds"),
    ("previous_idle_seconds", "cycle_context_previous_idle_seconds"),
)


class CycleOperatingContextProfileError(RuntimeError):
    """Raised when cycle operating-context coverage cannot be reconciled."""


@dataclass(frozen=True)
class CycleOperatingContextStatusCount:
    """Cycle count for one explicit operating-context status."""

    status: str
    cycle_count: int


@dataclass(frozen=True)
class CycleOperatingContextDistribution:
    """Observed distribution for one duration component."""

    component: str
    non_null_cycle_count: int
    minimum_seconds: int
    p05_seconds: int
    median_seconds: int
    p95_seconds: int
    maximum_seconds: int


@dataclass(frozen=True)
class FullSourceCycleOperatingContextProfile:
    """Configured source identity and reconciled operating-context evidence."""

    profile_version: str
    feature_version: str
    cycle_id_version: str
    dataset_version: str
    cycle_count: int
    status_counts: tuple[CycleOperatingContextStatusCount, ...]
    distributions: tuple[CycleOperatingContextDistribution, ...]


def _validate_feature_semantics(features: DataFrame) -> None:
    status = F.col("cycle_context_status")
    current_duration = F.col("cycle_context_current_duration_seconds")
    previous_cycle_id = F.col("cycle_context_previous_cycle_id")
    previous_duration = F.col("cycle_context_previous_duration_seconds")
    previous_idle = F.col("cycle_context_previous_idle_seconds")
    invalid = features.where(
        ~F.col("cycle_context_feature_version").eqNullSafe(F.lit(CYCLE_OPERATING_CONTEXT_VERSION))
        | (current_duration.isNotNull() & (current_duration <= 0))
        | (previous_duration.isNotNull() & (previous_duration <= 0))
        | (previous_idle.isNotNull() & (previous_idle < 0))
        | (
            (status == STATUS_AVAILABLE)
            & (
                current_duration.isNull()
                | previous_cycle_id.isNull()
                | previous_duration.isNull()
                | previous_idle.isNull()
            )
        )
        | (
            status.isin(
                STATUS_MISSING_PREDICTION,
                STATUS_LEFT_CENSORED_CURRENT,
                STATUS_INCOMPLETE_CURRENT,
            )
            & current_duration.isNotNull()
        )
        | (
            (status == STATUS_MISSING_PREVIOUS)
            & (
                previous_cycle_id.isNotNull()
                | previous_duration.isNotNull()
                | previous_idle.isNotNull()
            )
        )
        | ((status == STATUS_INCOMPLETE_PREVIOUS) & previous_cycle_id.isNull())
        | (
            (status == STATUS_INVALID_PREVIOUS_INTERVAL)
            & (previous_cycle_id.isNull() | previous_idle.isNotNull())
        )
    ).limit(1)
    if invalid.count():
        raise CycleOperatingContextProfileError(
            "Cycle operating-context feature semantics do not reconcile"
        )


def _collect_distributions(
    features: DataFrame,
) -> tuple[CycleOperatingContextDistribution, ...]:
    aggregations = []
    for component, column in _DISTRIBUTION_COLUMNS:
        aggregations.extend(
            (
                F.count(column).alias(f"{component}_count"),
                F.min(column).alias(f"{component}_minimum"),
                F.percentile_approx(column, [0.05, 0.5, 0.95], 10_000).alias(
                    f"{component}_percentiles"
                ),
                F.max(column).alias(f"{component}_maximum"),
            )
        )
    summary = features.agg(*aggregations).first()
    distributions = []
    for component, _column in _DISTRIBUTION_COLUMNS:
        count = int(summary[f"{component}_count"])
        percentiles = summary[f"{component}_percentiles"]
        if count <= 0 or percentiles is None:
            raise CycleOperatingContextProfileError(
                f"Cycle operating-context profile requires {component} values"
            )
        distributions.append(
            CycleOperatingContextDistribution(
                component=component,
                non_null_cycle_count=count,
                minimum_seconds=int(summary[f"{component}_minimum"]),
                p05_seconds=int(percentiles[0]),
                median_seconds=int(percentiles[1]),
                p95_seconds=int(percentiles[2]),
                maximum_seconds=int(summary[f"{component}_maximum"]),
            )
        )
    return tuple(distributions)


def collect_cycle_operating_context_profile(
    cycles: DataFrame,
    *,
    dataset_version: str,
) -> FullSourceCycleOperatingContextProfile:
    """Build and reconcile a read-only cycle operating-context profile."""

    cycle_summary = cycles.agg(
        F.count(F.lit(1)).alias("cycle_count"),
        F.countDistinct("loaded_cycle_id").alias("distinct_cycle_count"),
    ).first()
    cycle_count = int(cycle_summary.cycle_count)
    if cycle_count <= 0:
        raise CycleOperatingContextProfileError(
            "Cycle operating-context profiling requires Gold cycles"
        )
    if int(cycle_summary.distinct_cycle_count) != cycle_count:
        raise CycleOperatingContextProfileError("Gold cycles require unique cycle IDs")

    features = add_cycle_operating_context(cycles).persist(StorageLevel.DISK_ONLY)
    try:
        if features.count() != cycle_count:
            raise CycleOperatingContextProfileError(
                "Cycle operating-context rows do not reconcile with Gold cycles"
            )
        status_rows = features.groupBy("cycle_context_status").count().collect()
        status_by_name = {row.cycle_context_status: int(row["count"]) for row in status_rows}
        unexpected_statuses = set(status_by_name) - set(CYCLE_OPERATING_CONTEXT_STATUS_ORDER)
        if unexpected_statuses:
            raise CycleOperatingContextProfileError(
                "Cycle operating context contains unexpected statuses: "
                + ", ".join(sorted(str(status) for status in unexpected_statuses))
            )
        if sum(status_by_name.values()) != cycle_count:
            raise CycleOperatingContextProfileError(
                "Cycle operating-context statuses do not reconcile"
            )

        _validate_feature_semantics(features)
        status_counts = tuple(
            CycleOperatingContextStatusCount(
                status=status,
                cycle_count=status_by_name.get(status, 0),
            )
            for status in CYCLE_OPERATING_CONTEXT_STATUS_ORDER
        )
        return FullSourceCycleOperatingContextProfile(
            profile_version=CYCLE_OPERATING_CONTEXT_PROFILE_VERSION,
            feature_version=CYCLE_OPERATING_CONTEXT_VERSION,
            cycle_id_version=LOADED_CYCLE_ID_VERSION,
            dataset_version=dataset_version,
            cycle_count=cycle_count,
            status_counts=status_counts,
            distributions=_collect_distributions(features),
        )
    finally:
        features.unpersist()


def profile_full_source_cycle_operating_context(
    spark: SparkSession,
    config: RailPulseConfig,
) -> FullSourceCycleOperatingContextProfile:
    """Read Gold cycles and profile operating context without writing."""

    cycles = spark.read.format("delta").load(str(gold_table_path(config, LOADED_CYCLES_TABLE)))
    return collect_cycle_operating_context_profile(
        cycles,
        dataset_version=config.dataset_version,
    )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--project-root", type=Path, default=None)
    parser.add_argument("--master", default="local[4]")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the read-only full-source cycle operating-context profile."""

    args = _parse_args(argv)
    config = load_config(args.config, project_root=args.project_root)
    spark = create_local_spark_session(
        "railpulse-cycle-operating-context-profile", master=args.master
    )
    try:
        profile = profile_full_source_cycle_operating_context(spark, config)
    finally:
        spark.stop()
    print(json.dumps(asdict(profile), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
