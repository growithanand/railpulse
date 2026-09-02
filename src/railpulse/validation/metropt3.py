"""Inspect the official MetroPT-3 CSV without loading it fully into memory."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
EXPECTED_INTERVAL_SECONDS = 10
EXPECTED_INDEX_STEP = 10

ANALOG_COLUMNS = (
    "TP2",
    "TP3",
    "H1",
    "DV_pressure",
    "Reservoirs",
    "Oil_temperature",
    "Motor_current",
)
DIGITAL_COLUMNS = (
    "COMP",
    "DV_eletric",
    "Towers",
    "MPG",
    "LPS",
    "Pressure_switch",
    "Oil_level",
    "Caudal_impulses",
)
SENSOR_COLUMNS = ANALOG_COLUMNS + DIGITAL_COLUMNS
EXPECTED_HEADER = ("", "timestamp", *SENSOR_COLUMNS)


class ContractViolation(ValueError):
    """Raised when a file cannot represent the official MetroPT-3 telemetry contract."""


@dataclass(frozen=True)
class NumericRange:
    """Observed minimum and maximum for one numeric source column."""

    minimum: float
    maximum: float


@dataclass(frozen=True)
class TelemetryInspection:
    """Reproducible observations from one complete MetroPT-3 CSV scan."""

    file_size_bytes: int
    sha256: str
    header: tuple[str, ...]
    row_count: int
    first_timestamp: str | None
    last_timestamp: str | None
    minimum_timestamp: str | None
    maximum_timestamp: str | None
    source_index_start: int | None
    source_index_end: int | None
    expected_interval_seconds: int
    expected_interval_count: int
    irregular_interval_count: int
    interval_seconds_counts: dict[int, int]
    short_interval_count: int
    long_interval_count: int
    material_gap_count: int
    estimated_missing_nominal_interval_count: int
    maximum_gap_seconds: int
    duplicate_timestamp_count: int
    out_of_order_timestamp_count: int
    unexpected_index_step_count: int
    malformed_row_count: int
    missing_cell_count: int
    timestamp_parse_error_count: int
    numeric_parse_error_count: int
    non_finite_numeric_count: int
    numeric_ranges: dict[str, NumericRange]
    digital_values: dict[str, tuple[float, ...]]

    @property
    def structurally_valid(self) -> bool:
        """Return whether every row conforms to the source-level structural contract."""

        digital_values_valid = all(
            set(values).issubset({0.0, 1.0}) for values in self.digital_values.values()
        )
        return (
            self.header == EXPECTED_HEADER
            and self.row_count > 0
            and self.malformed_row_count == 0
            and self.missing_cell_count == 0
            and self.timestamp_parse_error_count == 0
            and self.numeric_parse_error_count == 0
            and self.non_finite_numeric_count == 0
            and self.unexpected_index_step_count == 0
            and self.duplicate_timestamp_count == 0
            and self.out_of_order_timestamp_count == 0
            and digital_values_valid
        )

    def to_dict(self) -> dict[str, object]:
        """Convert the inspection to JSON-serializable built-in values."""

        result = asdict(self)
        result["structurally_valid"] = self.structurally_valid
        return result

    def to_summary_dict(self) -> dict[str, object]:
        """Return a compact result with only the ten most frequent timestamp intervals."""

        result = self.to_dict()
        interval_counts = result.pop("interval_seconds_counts")
        dominant_intervals = sorted(
            interval_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )[:10]
        result["dominant_interval_seconds_counts"] = dict(dominant_intervals)
        return result


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _isoformat(value: datetime | None) -> str | None:
    return value.isoformat(sep=" ") if value else None


def inspect_telemetry_csv(csv_path: str | Path) -> TelemetryInspection:
    """Scan an official MetroPT-3 CSV and summarize its source-level integrity."""

    path = Path(csv_path).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(path)

    numeric_minima = {name: math.inf for name in SENSOR_COLUMNS}
    numeric_maxima = {name: -math.inf for name in SENSOR_COLUMNS}
    digital_values: dict[str, set[float]] = {name: set() for name in DIGITAL_COLUMNS}

    row_count = 0
    malformed_row_count = 0
    missing_cell_count = 0
    timestamp_parse_error_count = 0
    numeric_parse_error_count = 0
    non_finite_numeric_count = 0
    expected_interval_count = 0
    irregular_interval_count = 0
    short_interval_count = 0
    long_interval_count = 0
    material_gap_count = 0
    estimated_missing_nominal_interval_count = 0
    maximum_gap_seconds = 0
    duplicate_timestamp_count = 0
    out_of_order_timestamp_count = 0
    unexpected_index_step_count = 0
    interval_seconds_counts: Counter[int] = Counter()

    first_timestamp: datetime | None = None
    last_timestamp: datetime | None = None
    minimum_timestamp: datetime | None = None
    maximum_timestamp: datetime | None = None
    previous_timestamp: datetime | None = None
    source_index_start: int | None = None
    source_index_end: int | None = None
    previous_source_index: int | None = None

    with path.open("r", encoding="utf-8", newline="") as source:
        reader = csv.reader(source)
        try:
            header = tuple(next(reader))
        except StopIteration as error:
            raise ContractViolation("Telemetry CSV is empty") from error

        if header != EXPECTED_HEADER:
            raise ContractViolation(
                f"Unexpected header: expected {EXPECTED_HEADER!r}, received {header!r}"
            )

        for row in reader:
            row_count += 1
            if len(row) != len(EXPECTED_HEADER):
                malformed_row_count += 1
                continue

            missing_cell_count += sum(value == "" for value in row)

            try:
                source_index = int(row[0])
            except ValueError:
                numeric_parse_error_count += 1
            else:
                if source_index_start is None:
                    source_index_start = source_index
                if (
                    previous_source_index is not None
                    and source_index - previous_source_index != EXPECTED_INDEX_STEP
                ):
                    unexpected_index_step_count += 1
                source_index_end = source_index
                previous_source_index = source_index

            try:
                timestamp = datetime.strptime(row[1], TIMESTAMP_FORMAT)
            except ValueError:
                timestamp_parse_error_count += 1
            else:
                if first_timestamp is None:
                    first_timestamp = timestamp
                last_timestamp = timestamp
                minimum_timestamp = (
                    timestamp if minimum_timestamp is None else min(minimum_timestamp, timestamp)
                )
                maximum_timestamp = (
                    timestamp if maximum_timestamp is None else max(maximum_timestamp, timestamp)
                )

                if previous_timestamp is not None:
                    interval_seconds = int((timestamp - previous_timestamp).total_seconds())
                    interval_seconds_counts[interval_seconds] += 1
                    if interval_seconds == EXPECTED_INTERVAL_SECONDS:
                        expected_interval_count += 1
                    else:
                        irregular_interval_count += 1
                        if interval_seconds == 0:
                            duplicate_timestamp_count += 1
                        elif interval_seconds < 0:
                            out_of_order_timestamp_count += 1
                        elif interval_seconds < EXPECTED_INTERVAL_SECONDS:
                            short_interval_count += 1
                        elif interval_seconds > EXPECTED_INTERVAL_SECONDS:
                            long_interval_count += 1
                            maximum_gap_seconds = max(maximum_gap_seconds, interval_seconds)
                            estimated_missing = max(
                                interval_seconds // EXPECTED_INTERVAL_SECONDS - 1,
                                0,
                            )
                            estimated_missing_nominal_interval_count += estimated_missing
                            if estimated_missing > 0:
                                material_gap_count += 1
                previous_timestamp = timestamp

            for column_offset, column_name in enumerate(SENSOR_COLUMNS, start=2):
                try:
                    value = float(row[column_offset])
                except ValueError:
                    numeric_parse_error_count += 1
                    continue
                if not math.isfinite(value):
                    non_finite_numeric_count += 1
                    continue

                numeric_minima[column_name] = min(numeric_minima[column_name], value)
                numeric_maxima[column_name] = max(numeric_maxima[column_name], value)
                if column_name in digital_values:
                    digital_values[column_name].add(value)

    numeric_ranges = {
        name: NumericRange(minimum=numeric_minima[name], maximum=numeric_maxima[name])
        for name in SENSOR_COLUMNS
    }

    return TelemetryInspection(
        file_size_bytes=path.stat().st_size,
        sha256=_sha256(path),
        header=header,
        row_count=row_count,
        first_timestamp=_isoformat(first_timestamp),
        last_timestamp=_isoformat(last_timestamp),
        minimum_timestamp=_isoformat(minimum_timestamp),
        maximum_timestamp=_isoformat(maximum_timestamp),
        source_index_start=source_index_start,
        source_index_end=source_index_end,
        expected_interval_seconds=EXPECTED_INTERVAL_SECONDS,
        expected_interval_count=expected_interval_count,
        irregular_interval_count=irregular_interval_count,
        interval_seconds_counts=dict(sorted(interval_seconds_counts.items())),
        short_interval_count=short_interval_count,
        long_interval_count=long_interval_count,
        material_gap_count=material_gap_count,
        estimated_missing_nominal_interval_count=estimated_missing_nominal_interval_count,
        maximum_gap_seconds=maximum_gap_seconds,
        duplicate_timestamp_count=duplicate_timestamp_count,
        out_of_order_timestamp_count=out_of_order_timestamp_count,
        unexpected_index_step_count=unexpected_index_step_count,
        malformed_row_count=malformed_row_count,
        missing_cell_count=missing_cell_count,
        timestamp_parse_error_count=timestamp_parse_error_count,
        numeric_parse_error_count=numeric_parse_error_count,
        non_finite_numeric_count=non_finite_numeric_count,
        numeric_ranges=numeric_ranges,
        digital_values={name: tuple(sorted(values)) for name, values in digital_values.items()},
    )


def main() -> int:
    """Run the streaming inspector and print a reproducible JSON summary."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_path", type=Path)
    parser.add_argument(
        "--full-interval-distribution",
        action="store_true",
        help="include every observed timestamp interval instead of the ten most frequent",
    )
    args = parser.parse_args()

    inspection = inspect_telemetry_csv(args.csv_path)
    result = (
        inspection.to_dict() if args.full_interval_distribution else inspection.to_summary_dict()
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if inspection.structurally_valid else 1


if __name__ == "__main__":
    raise SystemExit(main())
