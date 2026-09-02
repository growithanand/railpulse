import csv
import json
import re
from datetime import datetime
from pathlib import Path

from railpulse.config import load_config
from railpulse.validation.metropt3 import EXPECTED_HEADER, SENSOR_COLUMNS

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MANIFEST_PATH = PROJECT_ROOT / "docs" / "dataset_manifest.json"
FAILURE_EVENTS_PATH = PROJECT_ROOT / "data" / "reference" / "metropt3_failure_events.csv"
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")


def _load_manifest() -> dict[str, object]:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def test_manifest_identifies_the_verified_uci_artifact() -> None:
    manifest = _load_manifest()
    dataset = manifest["dataset"]
    archive = manifest["archive"]

    assert dataset["uci_dataset_id"] == 791
    assert dataset["doi"] == "10.24432/C5VW3R"
    assert dataset["license"]["spdx_id"] == "CC-BY-4.0"
    assert dataset["version_id"] == load_config().dataset_version
    assert SHA256_PATTERN.fullmatch(archive["sha256"])
    assert len(archive["members"]) == 2
    assert all(SHA256_PATTERN.fullmatch(member["sha256"]) for member in archive["members"])
    assert SHA256_PATTERN.fullmatch(manifest["failure_reference"]["transcription_sha256"])


def test_manifest_schema_matches_the_inspector_contract() -> None:
    telemetry = _load_manifest()["observed_telemetry"]

    assert telemetry["header"] == list(EXPECTED_HEADER)
    assert telemetry["sensor_feature_count"] == len(SENSOR_COLUMNS) == 15
    assert set(telemetry["numeric_ranges"]) == set(SENSOR_COLUMNS)
    assert telemetry["quality"]["structurally_valid"] is True
    assert telemetry["row_count"] == 1_516_948


def test_failure_reference_preserves_four_ordered_non_overlapping_events() -> None:
    with FAILURE_EVENTS_PATH.open("r", encoding="utf-8", newline="") as source:
        events = list(csv.DictReader(source))

    assert len(events) == 4
    assert [event["source_row"] for event in events] == ["1", "2", "3", "4"]
    assert [event["source_report_number"] for event in events].count("#1") == 2

    previous_end: datetime | None = None
    for event in events:
        start = datetime.strptime(event["start_time_raw"], "%m/%d/%Y %H:%M")
        end = datetime.strptime(event["end_time_raw"], "%m/%d/%Y %H:%M")
        assert start < end
        if previous_end:
            assert previous_end < start
        previous_end = end

    assert events[1]["report_raw"] == "Maintenance on 30Apr at 12:00"
    assert "unresolved" in events[1]["source_note"]
