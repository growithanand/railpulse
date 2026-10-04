from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from railpulse.catalog import CatalogNamespace
from railpulse.jobs.source_preflight import (
    SOURCE_PREFLIGHT_CONTRACT_VERSION,
    SourceFileReader,
    SourcePreflightError,
    VolumeFileReader,
    load_source_contracts,
    reconcile_landed_sources,
)
from railpulse.landing import DatabricksSourceLanding

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MANIFEST_PATH = PROJECT_ROOT / "docs" / "dataset_manifest.json"
NAMESPACE = CatalogNamespace(
    catalog="workspace",
    bronze_schema="railpulse_bronze",
    silver_schema="railpulse_silver",
    gold_schema="railpulse_gold",
)


class _FakeReader(SourceFileReader):
    def __init__(self, files: dict[str, tuple[int, str]]) -> None:
        self.files = files
        self.size_requests: list[str] = []
        self.hash_requests: list[str] = []

    def size_bytes(self, path: str) -> int:
        self.size_requests.append(path)
        return self.files[path][0]

    def sha256(self, path: str) -> str:
        self.hash_requests.append(path)
        return self.files[path][1]


def _landing_and_reader() -> tuple[DatabricksSourceLanding, _FakeReader]:
    contracts = load_source_contracts(MANIFEST_PATH)
    landing = DatabricksSourceLanding(NAMESPACE, contracts.dataset_version)
    reader = _FakeReader(
        {
            landing.telemetry_path: (
                contracts.telemetry_size_bytes,
                contracts.telemetry_sha256,
            ),
            landing.failure_reference_path: (
                contracts.failure_reference_size_bytes,
                contracts.failure_reference_sha256,
            ),
        }
    )
    return landing, reader


def test_manifest_exposes_both_managed_source_contracts() -> None:
    contracts = load_source_contracts(MANIFEST_PATH)

    assert contracts.dataset_version == "uci-791-aab991a970e5"
    assert contracts.telemetry_size_bytes == 218_300_507
    assert contracts.telemetry_sha256.startswith("db30ccb4")
    assert contracts.failure_reference_size_bytes == 556
    assert contracts.failure_reference_sha256.startswith("3a9e0220")


def test_volume_file_reader_streams_exact_file_identity(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    source.write_bytes(b"alpha\nbeta\n")
    reader = VolumeFileReader()

    assert reader.size_bytes(str(source)) == 11
    assert reader.sha256(str(source)) == hashlib.sha256(b"alpha\nbeta\n").hexdigest()


def test_manifest_rejects_missing_positive_size_contract(tmp_path: Path) -> None:
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    manifest["failure_reference"]["size_bytes"] = 0
    invalid_manifest = tmp_path / "dataset_manifest.json"
    invalid_manifest.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(SourcePreflightError, match="positive integer"):
        load_source_contracts(invalid_manifest)


def test_source_preflight_reconciles_both_versioned_volume_paths() -> None:
    contracts = load_source_contracts(MANIFEST_PATH)
    landing, reader = _landing_and_reader()

    result = reconcile_landed_sources(landing, contracts, reader)

    assert result.contract_version == SOURCE_PREFLIGHT_CONTRACT_VERSION
    assert result.landing_contract_version == landing.contract_version
    assert result.dataset_version == contracts.dataset_version
    assert result.qualified_volume == "workspace.railpulse_bronze.source"
    assert [item.artifact for item in result.files] == ["telemetry", "failure_reference"]
    assert reader.size_requests == [landing.telemetry_path, landing.failure_reference_path]
    assert reader.hash_requests == [landing.telemetry_path, landing.failure_reference_path]


def test_source_preflight_rejects_size_mismatch_before_hashing() -> None:
    contracts = load_source_contracts(MANIFEST_PATH)
    landing, reader = _landing_and_reader()
    reader.files[landing.telemetry_path] = (
        contracts.telemetry_size_bytes - 1,
        contracts.telemetry_sha256,
    )

    with pytest.raises(SourcePreflightError, match="Size mismatch for telemetry"):
        reconcile_landed_sources(landing, contracts, reader)

    assert reader.hash_requests == []


def test_source_preflight_rejects_hash_mismatch() -> None:
    contracts = load_source_contracts(MANIFEST_PATH)
    landing, reader = _landing_and_reader()
    reader.files[landing.failure_reference_path] = (
        contracts.failure_reference_size_bytes,
        "0" * 64,
    )

    with pytest.raises(SourcePreflightError, match="SHA-256 mismatch for failure_reference"):
        reconcile_landed_sources(landing, contracts, reader)


def test_source_preflight_rejects_dataset_version_mismatch() -> None:
    contracts = load_source_contracts(MANIFEST_PATH)
    landing = DatabricksSourceLanding(NAMESPACE, "different-version")

    with pytest.raises(SourcePreflightError, match="dataset version does not match"):
        reconcile_landed_sources(landing, contracts, _FakeReader({}))
