from __future__ import annotations

from datetime import datetime

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from railpulse.catalog import CatalogNamespace
from railpulse.features.cycles import LOADED_CYCLE_ID_VERSION
from railpulse.features.failure_horizons import (
    DEFAULT_FAILURE_HORIZON_SECONDS,
    FAILURE_HORIZON_VERSION,
)
from railpulse.jobs import gold_failure_horizon_processing
from railpulse.jobs.gold_failure_horizon_processing import (
    GOLD_FAILURE_HORIZON_PROCESSING_JOB_CONTRACT_VERSION,
    ManagedGoldFailureHorizonProcessingError,
    ManagedSourceLineage,
    _parse_args,
    run_managed_gold_failure_horizon_processing,
)

NAMESPACE = CatalogNamespace(
    catalog="workspace",
    bronze_schema="railpulse_bronze",
    silver_schema="railpulse_silver",
    gold_schema="railpulse_gold",
)


def test_gold_failure_horizon_job_parses_bundle_named_parameters() -> None:
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


def test_gold_failure_horizon_job_reads_labels_and_persists_managed_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cycles = object()
    telemetry = object()
    failures = object()
    horizons = object()
    write_result = object()
    observation_end = datetime(2020, 9, 1, 3, 59, 50)
    telemetry_lineage = ManagedSourceLineage("dataset-v1", "telemetry-sha", "telemetry-batch")
    failure_lineage = ManagedSourceLineage("dataset-v1", "failure-sha", "failure-batch")
    calls: list[tuple[object, ...]] = []

    class FakeSpark:
        def table(self, table_name: str) -> object:
            calls.append(("read", table_name))
            return {
                "workspace.railpulse_gold.loaded_cycles": cycles,
                "workspace.railpulse_silver.telemetry_accepted": telemetry,
                "workspace.railpulse_silver.failure_events_accepted": failures,
            }[table_name]

    def fake_lineage(frame: object, *, label: str) -> ManagedSourceLineage:
        calls.append(("lineage", frame, label))
        return telemetry_lineage if frame is telemetry else failure_lineage

    def fake_observation_end(frame: object) -> datetime:
        calls.append(("observation_end", frame))
        return observation_end

    def fake_assign(
        cycle_frame: object,
        failure_frame: object,
        *,
        horizon_seconds: int,
        observation_end: datetime,
    ) -> object:
        calls.append(
            (
                "assign",
                cycle_frame,
                failure_frame,
                horizon_seconds,
                observation_end,
            )
        )
        return horizons

    def fake_persist(frame: object, namespace: CatalogNamespace) -> object:
        calls.append(("persist", frame, namespace))
        return write_result

    monkeypatch.setattr(gold_failure_horizon_processing, "_single_source_lineage", fake_lineage)
    monkeypatch.setattr(
        gold_failure_horizon_processing,
        "_latest_telemetry_timestamp",
        fake_observation_end,
    )
    monkeypatch.setattr(
        gold_failure_horizon_processing,
        "assign_cycle_failure_horizons",
        fake_assign,
    )
    monkeypatch.setattr(
        gold_failure_horizon_processing,
        "persist_catalog_failure_horizons",
        fake_persist,
    )

    result = run_managed_gold_failure_horizon_processing(FakeSpark(), NAMESPACE)

    assert result.contract_version == GOLD_FAILURE_HORIZON_PROCESSING_JOB_CONTRACT_VERSION
    assert result.cycle_id_version == LOADED_CYCLE_ID_VERSION
    assert result.horizon_version == FAILURE_HORIZON_VERSION
    assert result.horizon_seconds == DEFAULT_FAILURE_HORIZON_SECONDS
    assert result.label_observation_end == "2020-09-01 03:59:50"
    assert result.cycle_source_table == "workspace.railpulse_gold.loaded_cycles"
    assert result.telemetry_source_table == "workspace.railpulse_silver.telemetry_accepted"
    assert result.failure_source_table == "workspace.railpulse_silver.failure_events_accepted"
    assert result.telemetry_lineage is telemetry_lineage
    assert result.failure_lineage is failure_lineage
    assert result.write is write_result
    assert calls == [
        ("read", "workspace.railpulse_gold.loaded_cycles"),
        ("read", "workspace.railpulse_silver.telemetry_accepted"),
        ("read", "workspace.railpulse_silver.failure_events_accepted"),
        ("lineage", telemetry, "Managed accepted telemetry"),
        ("lineage", failures, "Managed accepted failure events"),
        ("observation_end", telemetry),
        ("assign", cycles, failures, 7_200, observation_end),
        ("persist", horizons, NAMESPACE),
    ]


def test_gold_failure_horizon_job_rejects_mixed_dataset_versions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cycles = object()
    telemetry = object()
    failures = object()

    class FakeSpark:
        def table(self, table_name: str) -> object:
            return {
                "workspace.railpulse_gold.loaded_cycles": cycles,
                "workspace.railpulse_silver.telemetry_accepted": telemetry,
                "workspace.railpulse_silver.failure_events_accepted": failures,
            }[table_name]

    def fake_lineage(frame: object, *, label: str) -> ManagedSourceLineage:
        dataset_version = "telemetry-v1" if frame is telemetry else "failure-v2"
        return ManagedSourceLineage(dataset_version, "source-sha", "batch-id")

    monkeypatch.setattr(gold_failure_horizon_processing, "_single_source_lineage", fake_lineage)

    with pytest.raises(ManagedGoldFailureHorizonProcessingError, match="do not match"):
        run_managed_gold_failure_horizon_processing(FakeSpark(), NAMESPACE)
