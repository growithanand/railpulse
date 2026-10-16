from __future__ import annotations

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from railpulse.catalog import CatalogNamespace
from railpulse.features.cycles import LOADED_CYCLE_ID_VERSION
from railpulse.jobs import gold_cycle_processing
from railpulse.jobs.gold_cycle_processing import (
    GOLD_CYCLE_PROCESSING_JOB_CONTRACT_VERSION,
    _parse_args,
    run_managed_gold_cycle_processing,
)

NAMESPACE = CatalogNamespace(
    catalog="workspace",
    bronze_schema="railpulse_bronze",
    silver_schema="railpulse_silver",
    gold_schema="railpulse_gold",
)


def test_gold_cycle_job_parses_bundle_named_parameters() -> None:
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


def test_gold_cycle_job_reads_transforms_and_persists_managed_telemetry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    accepted_telemetry = object()
    boundaries = object()
    segments = object()
    cycles = object()
    write_result = object()
    calls: list[tuple[object, ...]] = []

    class FakeSpark:
        def table(self, table_name: str) -> object:
            calls.append(("read", table_name))
            return accepted_telemetry

    def fake_annotate(frame: object) -> object:
        calls.append(("annotate", frame))
        return boundaries

    def fake_assign(frame: object) -> object:
        calls.append(("assign", frame))
        return segments

    def fake_aggregate(frame: object) -> object:
        calls.append(("aggregate", frame))
        return cycles

    def fake_persist(frame: object, namespace: CatalogNamespace) -> object:
        calls.append(("persist", frame, namespace))
        return write_result

    monkeypatch.setattr(gold_cycle_processing, "annotate_loaded_cycle_boundaries", fake_annotate)
    monkeypatch.setattr(gold_cycle_processing, "assign_loaded_cycle_segments", fake_assign)
    monkeypatch.setattr(gold_cycle_processing, "aggregate_loaded_cycles", fake_aggregate)
    monkeypatch.setattr(gold_cycle_processing, "persist_catalog_loaded_cycles", fake_persist)

    result = run_managed_gold_cycle_processing(FakeSpark(), NAMESPACE)

    assert result.contract_version == GOLD_CYCLE_PROCESSING_JOB_CONTRACT_VERSION
    assert result.cycle_id_version == LOADED_CYCLE_ID_VERSION
    assert result.telemetry_source_table == "workspace.railpulse_silver.telemetry_accepted"
    assert result.write is write_result
    assert calls == [
        ("read", "workspace.railpulse_silver.telemetry_accepted"),
        ("annotate", accepted_telemetry),
        ("assign", boundaries),
        ("aggregate", segments),
        ("persist", cycles, NAMESPACE),
    ]
