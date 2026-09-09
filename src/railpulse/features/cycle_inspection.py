"""Inspect Gold loaded-cycle censoring categories and duration tails."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F

from railpulse.config import RailPulseConfig, load_config
from railpulse.features.cycle_storage import LOADED_CYCLES_TABLE, gold_table_path
from railpulse.spark import create_local_spark_session

CYCLE_INSPECTION_VERSION = "loaded-cycle-inspection-v1"
CYCLE_INSPECTION_VIEW = "gold_loaded_cycles"
CYCLE_INSPECTION_COLUMNS = (
    "loaded_cycle_start_type",
    "is_right_censored",
    "cycle_count",
    "loaded_observation_count",
    "duration_cycle_count",
    "minimum_observed_duration_seconds",
    "median_observed_duration_seconds",
    "p95_observed_duration_seconds",
    "maximum_observed_duration_seconds",
)
INSPECTION_SOURCE_COLUMNS = (
    "loaded_cycle_start_type",
    "is_right_censored",
    "loaded_observation_count",
    "observed_duration_seconds",
)


class CycleInspectionError(RuntimeError):
    """Raised when Gold cycle inspection output cannot be reconciled."""


@dataclass(frozen=True)
class CycleInspectionGroup:
    """Descriptive metrics for one visible start/stop censoring category."""

    loaded_cycle_start_type: str
    is_right_censored: bool
    cycle_count: int
    loaded_observation_count: int
    duration_cycle_count: int
    minimum_observed_duration_seconds: int | None
    median_observed_duration_seconds: int | None
    p95_observed_duration_seconds: int | None
    maximum_observed_duration_seconds: int | None


@dataclass(frozen=True)
class GoldCycleInspection:
    """Reconciled descriptive inspection of one Gold cycle snapshot."""

    inspection_version: str
    table_name: str
    cycle_count: int
    loaded_observation_count: int
    duration_cycle_count: int
    groups: tuple[CycleInspectionGroup, ...]


def load_cycle_inspection_query(project_root: Path) -> str:
    """Load the checked-in Spark SQL inspection query."""

    query_path = project_root / "sql" / "gold_cycle_inspection.sql"
    try:
        query = query_path.read_text(encoding="utf-8").strip()
    except OSError as error:
        raise CycleInspectionError(
            f"Unable to load cycle inspection query from {query_path}"
        ) from error
    if not query:
        raise CycleInspectionError(f"Cycle inspection query is empty: {query_path}")
    return query


def _optional_int(value: object) -> int | None:
    return None if value is None else int(value)


def _inspection_group(row: Row) -> CycleInspectionGroup:
    start_type = row.loaded_cycle_start_type
    is_right_censored = row.is_right_censored
    cycle_count = int(row.cycle_count)
    loaded_observation_count = int(row.loaded_observation_count)
    duration_cycle_count = int(row.duration_cycle_count)
    durations = (
        _optional_int(row.minimum_observed_duration_seconds),
        _optional_int(row.median_observed_duration_seconds),
        _optional_int(row.p95_observed_duration_seconds),
        _optional_int(row.maximum_observed_duration_seconds),
    )
    if start_type not in {"observed", "left_censored"}:
        raise CycleInspectionError(f"Unexpected cycle start type in inspection: {start_type}")
    if not isinstance(is_right_censored, bool):
        raise CycleInspectionError("Cycle inspection contains an invalid censoring flag")
    if cycle_count <= 0 or loaded_observation_count < cycle_count:
        raise CycleInspectionError("Cycle inspection group has invalid record counts")
    if is_right_censored:
        if duration_cycle_count != 0 or any(value is not None for value in durations):
            raise CycleInspectionError("Right-censored inspection group contains duration evidence")
    else:
        if duration_cycle_count != cycle_count or any(value is None for value in durations):
            raise CycleInspectionError("Closed inspection group has incomplete duration evidence")
        minimum, median, p95, maximum = durations
        if not (0 < minimum <= median <= p95 <= maximum):
            raise CycleInspectionError("Closed inspection duration statistics are inconsistent")

    return CycleInspectionGroup(
        loaded_cycle_start_type=start_type,
        is_right_censored=is_right_censored,
        cycle_count=cycle_count,
        loaded_observation_count=loaded_observation_count,
        duration_cycle_count=duration_cycle_count,
        minimum_observed_duration_seconds=durations[0],
        median_observed_duration_seconds=durations[1],
        p95_observed_duration_seconds=durations[2],
        maximum_observed_duration_seconds=durations[3],
    )


def inspect_loaded_cycles(cycles: DataFrame, query: str) -> GoldCycleInspection:
    """Execute and reconcile the descriptive Gold cycle SQL query."""

    missing_columns = sorted(set(INSPECTION_SOURCE_COLUMNS) - set(cycles.columns))
    if missing_columns:
        raise CycleInspectionError(
            "Gold cycles are missing inspection columns: " + ", ".join(missing_columns)
        )

    cycles.createOrReplaceTempView(CYCLE_INSPECTION_VIEW)
    result = cycles.sparkSession.sql(query)
    if tuple(result.columns) != CYCLE_INSPECTION_COLUMNS:
        raise CycleInspectionError(
            "Cycle inspection query returned unexpected columns: " + ", ".join(result.columns)
        )

    source_totals = cycles.agg(
        F.count(F.lit(1)).alias("cycle_count"),
        F.coalesce(F.sum("loaded_observation_count"), F.lit(0)).alias("loaded_observation_count"),
        F.count("observed_duration_seconds").alias("duration_cycle_count"),
    ).first()
    rows = result.collect()
    if len({(row.loaded_cycle_start_type, row.is_right_censored) for row in rows}) != len(rows):
        raise CycleInspectionError("Cycle inspection contains duplicate censoring groups")

    groups = tuple(_inspection_group(row) for row in rows)
    cycle_count = int(source_totals.cycle_count)
    loaded_observation_count = int(source_totals.loaded_observation_count)
    duration_cycle_count = int(source_totals.duration_cycle_count)
    if sum(group.cycle_count for group in groups) != cycle_count:
        raise CycleInspectionError("Cycle inspection group counts do not reconcile")
    if sum(group.loaded_observation_count for group in groups) != loaded_observation_count:
        raise CycleInspectionError("Cycle inspection loaded observations do not reconcile")
    if sum(group.duration_cycle_count for group in groups) != duration_cycle_count:
        raise CycleInspectionError("Cycle inspection duration counts do not reconcile")

    return GoldCycleInspection(
        inspection_version=CYCLE_INSPECTION_VERSION,
        table_name="gold.loaded_cycles",
        cycle_count=cycle_count,
        loaded_observation_count=loaded_observation_count,
        duration_cycle_count=duration_cycle_count,
        groups=groups,
    )


def inspect_gold_cycle_table(
    spark: SparkSession,
    config: RailPulseConfig,
) -> GoldCycleInspection:
    """Load the configured Gold cycle table and run its checked-in inspection query."""

    cycles = spark.read.format("delta").load(str(gold_table_path(config, LOADED_CYCLES_TABLE)))
    query = load_cycle_inspection_query(config.project_root)
    return inspect_loaded_cycles(cycles, query)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--project-root", type=Path, default=None)
    parser.add_argument("--master", default="local[2]")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the read-only Gold cycle inspection and print deterministic JSON."""

    args = _parse_args(argv)
    config = load_config(args.config, project_root=args.project_root)
    spark = create_local_spark_session("railpulse-cycle-inspection", master=args.master)
    try:
        inspection = inspect_gold_cycle_table(spark, config)
    finally:
        spark.stop()
    print(json.dumps(asdict(inspection), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
