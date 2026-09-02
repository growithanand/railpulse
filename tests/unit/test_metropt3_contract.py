import csv
from pathlib import Path

import pytest

from railpulse.validation.metropt3 import (
    EXPECTED_HEADER,
    ContractViolation,
    inspect_telemetry_csv,
)


def _write_csv(path: Path, rows: list[list[str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as destination:
        writer = csv.writer(destination)
        writer.writerow(EXPECTED_HEADER)
        writer.writerows(rows)


def _row(source_index: int, timestamp: str, *, tp2: str = "1.5") -> list[str]:
    return [
        str(source_index),
        timestamp,
        tp2,
        "9.0",
        "8.9",
        "0.0",
        "9.0",
        "55.0",
        "4.0",
        "1.0",
        "0.0",
        "1.0",
        "1.0",
        "0.0",
        "1.0",
        "1.0",
        "0.0",
    ]


def test_inspector_summarizes_valid_rows_and_timestamp_gap(tmp_path: Path) -> None:
    csv_path = tmp_path / "telemetry.csv"
    _write_csv(
        csv_path,
        [
            _row(0, "2020-02-01 00:00:00", tp2="1.0"),
            _row(10, "2020-02-01 00:00:10", tp2="2.0"),
            _row(20, "2020-02-01 00:00:30", tp2="3.0"),
        ],
    )

    inspection = inspect_telemetry_csv(csv_path)

    assert inspection.structurally_valid
    assert inspection.row_count == 3
    assert inspection.expected_interval_count == 1
    assert inspection.irregular_interval_count == 1
    assert inspection.interval_seconds_counts == {10: 1, 20: 1}
    assert inspection.short_interval_count == 0
    assert inspection.long_interval_count == 1
    assert inspection.material_gap_count == 1
    assert inspection.estimated_missing_nominal_interval_count == 1
    assert inspection.maximum_gap_seconds == 20
    assert inspection.numeric_ranges["TP2"].minimum == 1.0
    assert inspection.numeric_ranges["TP2"].maximum == 3.0
    assert inspection.digital_values["COMP"] == (1.0,)
    assert "interval_seconds_counts" not in inspection.to_summary_dict()
    assert inspection.to_summary_dict()["dominant_interval_seconds_counts"] == {10: 1, 20: 1}


def test_inspector_rejects_an_unexpected_header(tmp_path: Path) -> None:
    csv_path = tmp_path / "wrong-header.csv"
    csv_path.write_text("timestamp,TP2\n2020-02-01 00:00:00,1.0\n", encoding="utf-8")

    with pytest.raises(ContractViolation, match="Unexpected header"):
        inspect_telemetry_csv(csv_path)


def test_inspector_reports_invalid_numeric_and_timestamp_values(tmp_path: Path) -> None:
    csv_path = tmp_path / "invalid.csv"
    invalid_row = _row(0, "not-a-timestamp", tp2="not-a-number")
    _write_csv(csv_path, [invalid_row])

    inspection = inspect_telemetry_csv(csv_path)

    assert not inspection.structurally_valid
    assert inspection.timestamp_parse_error_count == 1
    assert inspection.numeric_parse_error_count == 1


def test_inspector_rejects_a_non_binary_digital_domain(tmp_path: Path) -> None:
    csv_path = tmp_path / "non-binary.csv"
    non_binary_row = _row(0, "2020-02-01 00:00:00")
    non_binary_row[9] = "2.0"
    _write_csv(csv_path, [non_binary_row])

    inspection = inspect_telemetry_csv(csv_path)

    assert inspection.digital_values["COMP"] == (2.0,)
    assert not inspection.structurally_valid
