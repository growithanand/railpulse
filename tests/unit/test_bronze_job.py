from __future__ import annotations

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from railpulse.catalog import CatalogNamespace
from railpulse.ingestion.bronze import (
    CATALOG_BRONZE_CONTRACT_VERSION,
    FAILURE_TABLE,
    TELEMETRY_TABLE,
    CatalogBronzeIngestionResult,
    DatasetArtifacts,
)
from railpulse.jobs import bronze_ingestion
from railpulse.jobs.bronze_ingestion import (
    BRONZE_INGESTION_JOB_CONTRACT_VERSION,
    _parse_args,
    run_managed_bronze_ingestion,
)
from railpulse.jobs.source_preflight import SOURCE_PREFLIGHT_CONTRACT_VERSION

NAMESPACE = CatalogNamespace(
    catalog="workspace",
    bronze_schema="railpulse_bronze",
    silver_schema="railpulse_silver",
    gold_schema="railpulse_gold",
)


def _write_result(table_name: str, source_sha256: str) -> CatalogBronzeIngestionResult:
    return CatalogBronzeIngestionResult(
        contract_version=CATALOG_BRONZE_CONTRACT_VERSION,
        table_name=NAMESPACE.table("bronze", table_name),
        source_sha256=source_sha256,
        ingestion_batch_id="batch-id",
        source_row_count=3,
        inserted_row_count=3,
        matched_target_row_count=3,
        target_row_count_before=0,
        target_row_count_after=3,
    )


def test_bronze_job_parses_bundle_named_parameters() -> None:
    args = _parse_args(
        [
            "--catalog",
            "catalog_name",
            "--bronze_schema",
            "bronze_name",
            "--silver_schema",
            "silver_name",
            "--gold_schema",
            "gold_name",
            "--source_volume",
            "volume_name",
            "--manifest_path",
            "/Workspace/bundle/files/docs/dataset_manifest.json",
        ]
    )

    assert vars(args) == {
        "catalog": "catalog_name",
        "bronze_schema": "bronze_name",
        "silver_schema": "silver_name",
        "gold_schema": "gold_name",
        "source_volume": "volume_name",
        "manifest_path": "/Workspace/bundle/files/docs/dataset_manifest.json",
    }


def test_bronze_job_routes_both_sources_to_allowlisted_catalog_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifacts = DatasetArtifacts(
        dataset_version="dataset-v1",
        telemetry_sha256="a" * 64,
        failure_transcription_sha256="b" * 64,
        failure_source_document_sha256="c" * 64,
    )
    telemetry_frame = object()
    failure_frame = object()
    read_calls: list[tuple[str, object, str, dict[str, str]]] = []
    merge_calls: list[tuple[object, str, str, str]] = []

    monkeypatch.setattr(bronze_ingestion, "load_dataset_artifacts", lambda path: artifacts)

    def fake_read_telemetry(
        spark: object,
        path: str,
        **kwargs: str,
    ) -> object:
        read_calls.append(("telemetry", spark, path, kwargs))
        return telemetry_frame

    def fake_read_failures(
        spark: object,
        path: str,
        **kwargs: str,
    ) -> object:
        read_calls.append(("failures", spark, path, kwargs))
        return failure_frame

    def fake_merge(
        frame: object,
        namespace: CatalogNamespace,
        *,
        table_name: str,
        source_sha256: str,
        dataset_version: str,
    ) -> CatalogBronzeIngestionResult:
        assert namespace == NAMESPACE
        merge_calls.append((frame, table_name, source_sha256, dataset_version))
        return _write_result(table_name, source_sha256)

    monkeypatch.setattr(bronze_ingestion, "read_telemetry_bronze", fake_read_telemetry)
    monkeypatch.setattr(bronze_ingestion, "read_failure_reports_bronze", fake_read_failures)
    monkeypatch.setattr(bronze_ingestion, "merge_catalog_bronze_records", fake_merge)

    spark = object()
    result = run_managed_bronze_ingestion(
        spark,
        NAMESPACE,
        source_volume="source",
        manifest_path="manifest.json",
    )

    assert result.contract_version == BRONZE_INGESTION_JOB_CONTRACT_VERSION
    assert result.required_source_preflight_contract_version == SOURCE_PREFLIGHT_CONTRACT_VERSION
    assert result.dataset_version == "dataset-v1"
    assert result.telemetry_source_path.endswith(
        "/metropt3/dataset-v1/telemetry/MetroPT3(AirCompressor).csv"
    )
    assert result.failure_reference_source_path.endswith(
        "/metropt3/dataset-v1/reference/metropt3_failure_events.csv"
    )
    assert [write.table_name for write in result.writes] == [
        "workspace.railpulse_bronze.telemetry_raw",
        "workspace.railpulse_bronze.failure_reports_raw",
    ]
    assert read_calls == [
        (
            "telemetry",
            spark,
            result.telemetry_source_path,
            {"source_sha256": "a" * 64, "dataset_version": "dataset-v1"},
        ),
        (
            "failures",
            spark,
            result.failure_reference_source_path,
            {
                "source_sha256": "b" * 64,
                "source_document_sha256": "c" * 64,
                "dataset_version": "dataset-v1",
            },
        ),
    ]
    assert merge_calls == [
        (telemetry_frame, TELEMETRY_TABLE, "a" * 64, "dataset-v1"),
        (failure_frame, FAILURE_TABLE, "b" * 64, "dataset-v1"),
    ]
