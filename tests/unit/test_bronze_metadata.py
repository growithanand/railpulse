import hashlib
import json
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from railpulse.catalog import CatalogNamespace
from railpulse.ingestion.bronze import (
    FAILURE_TABLE,
    TELEMETRY_TABLE,
    BronzeIngestionError,
    catalog_bronze_table,
    deterministic_batch_id,
    file_sha256,
    load_dataset_artifacts,
    verify_file_sha256,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _catalog_namespace() -> CatalogNamespace:
    return CatalogNamespace(
        catalog="workspace",
        bronze_schema="railpulse_bronze",
        silver_schema="railpulse_silver",
        gold_schema="railpulse_gold",
    )


def test_file_sha256_and_verification_use_exact_content(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    source.write_bytes(b"alpha\nbeta\n")
    expected = hashlib.sha256(b"alpha\nbeta\n").hexdigest()

    assert file_sha256(source) == expected
    assert verify_file_sha256(source, expected.upper()) == expected

    with pytest.raises(BronzeIngestionError, match="SHA-256 mismatch"):
        verify_file_sha256(source, "0" * 64)


def test_batch_identifier_is_stable_and_table_specific() -> None:
    first = deterministic_batch_id("dataset-v1", "telemetry_raw", "a" * 64)

    assert first == deterministic_batch_id("dataset-v1", "telemetry_raw", "a" * 64)
    assert first != deterministic_batch_id("dataset-v1", "failure_reports_raw", "a" * 64)
    assert len(first) == 64


def test_catalog_bronze_tables_are_fully_qualified_and_allowlisted() -> None:
    namespace = _catalog_namespace()

    assert catalog_bronze_table(namespace, TELEMETRY_TABLE) == (
        "workspace.railpulse_bronze.telemetry_raw"
    )
    assert catalog_bronze_table(namespace, FAILURE_TABLE) == (
        "workspace.railpulse_bronze.failure_reports_raw"
    )

    with pytest.raises(BronzeIngestionError, match="Unsupported managed Bronze table"):
        catalog_bronze_table(namespace, "temporary_copy")


def test_manifest_exposes_all_bronze_source_identities() -> None:
    manifest_path = PROJECT_ROOT / "docs" / "dataset_manifest.json"
    artifacts = load_dataset_artifacts(manifest_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert artifacts.dataset_version == "uci-791-aab991a970e5"
    assert artifacts.telemetry_sha256.startswith("db30ccb4")
    assert artifacts.failure_source_document_sha256.startswith("b00fac0e")
    failure_reference = PROJECT_ROOT / manifest["failure_reference"]["path"]
    assert manifest["failure_reference"]["size_bytes"] == failure_reference.stat().st_size
    assert artifacts.failure_transcription_sha256 == file_sha256(failure_reference)
