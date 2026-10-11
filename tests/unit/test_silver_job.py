from __future__ import annotations

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from railpulse.catalog import CatalogNamespace
from railpulse.jobs import silver_processing
from railpulse.jobs.silver_processing import (
    SILVER_PROCESSING_JOB_CONTRACT_VERSION,
    _parse_args,
    run_managed_silver_processing,
)

NAMESPACE = CatalogNamespace(
    catalog="workspace",
    bronze_schema="railpulse_bronze",
    silver_schema="railpulse_silver",
    gold_schema="railpulse_gold",
)


def test_silver_job_parses_bundle_named_parameters() -> None:
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
        ]
    )

    assert vars(args) == {
        "catalog": "catalog_name",
        "bronze_schema": "bronze_name",
        "silver_schema": "silver_name",
        "gold_schema": "gold_name",
    }


def test_silver_job_reads_validates_and_persists_both_managed_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    telemetry_bronze = object()
    failure_bronze = object()
    telemetry_split = object()
    failure_split = object()
    telemetry_result = object()
    failure_result = object()
    calls: list[tuple[object, ...]] = []

    class FakeSpark:
        def table(self, table_name: str) -> object:
            calls.append(("read", table_name))
            return {
                "workspace.railpulse_bronze.telemetry_raw": telemetry_bronze,
                "workspace.railpulse_bronze.failure_reports_raw": failure_bronze,
            }[table_name]

    def fake_split_telemetry(frame: object) -> object:
        calls.append(("validate_telemetry", frame))
        return telemetry_split

    def fake_split_failures(frame: object) -> object:
        calls.append(("validate_failures", frame))
        return failure_split

    def fake_persist_telemetry(split: object, namespace: CatalogNamespace) -> object:
        calls.append(("persist_telemetry", split, namespace))
        return telemetry_result

    def fake_persist_failures(split: object, namespace: CatalogNamespace) -> object:
        calls.append(("persist_failures", split, namespace))
        return failure_result

    monkeypatch.setattr(silver_processing, "split_telemetry_by_quality", fake_split_telemetry)
    monkeypatch.setattr(silver_processing, "split_failure_events_by_quality", fake_split_failures)
    monkeypatch.setattr(
        silver_processing,
        "persist_catalog_telemetry_outputs",
        fake_persist_telemetry,
    )
    monkeypatch.setattr(
        silver_processing,
        "persist_catalog_failure_outputs",
        fake_persist_failures,
    )

    result = run_managed_silver_processing(FakeSpark(), NAMESPACE)

    assert result.contract_version == SILVER_PROCESSING_JOB_CONTRACT_VERSION
    assert result.telemetry_source_table == "workspace.railpulse_bronze.telemetry_raw"
    assert result.failure_source_table == "workspace.railpulse_bronze.failure_reports_raw"
    assert result.telemetry is telemetry_result
    assert result.failures is failure_result
    assert calls == [
        ("read", "workspace.railpulse_bronze.telemetry_raw"),
        ("read", "workspace.railpulse_bronze.failure_reports_raw"),
        ("validate_telemetry", telemetry_bronze),
        ("validate_failures", failure_bronze),
        ("persist_telemetry", telemetry_split, NAMESPACE),
        ("persist_failures", failure_split, NAMESPACE),
    ]
