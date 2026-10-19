from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from railpulse.catalog import CatalogNamespace
from railpulse.features.cycle_operating_context import CYCLE_OPERATING_CONTEXT_VERSION
from railpulse.features.eligibility_profile import (
    MOTOR_CURRENT_ELIGIBILITY_PROFILE_VERSION,
    EligibilityReasonCount,
    EligibilityStatusCount,
)
from railpulse.features.feature_eligibility import MOTOR_CURRENT_ELIGIBILITY_VERSION
from railpulse.features.feature_snapshot_v2_schema import FEATURE_SNAPSHOT_V2_VERSION
from railpulse.features.temporal_features import MOTOR_CURRENT_FEATURE_VERSION
from railpulse.jobs import gold_feature_snapshot_v2_processing
from railpulse.jobs.gold_feature_snapshot_v2_processing import (
    GOLD_FEATURE_SNAPSHOT_V2_PROCESSING_JOB_CONTRACT_VERSION,
    ManagedFeatureSnapshotSourceLineage,
    ManagedGoldFeatureSnapshotV2ProcessingError,
    _ManagedFeatureSnapshotV2Build,
    _parse_args,
    run_managed_gold_feature_snapshot_v2_processing,
)

NAMESPACE = CatalogNamespace(
    catalog="workspace",
    bronze_schema="railpulse_bronze",
    silver_schema="railpulse_silver",
    gold_schema="railpulse_gold",
)


def test_gold_feature_snapshot_v2_job_parses_bundle_named_parameters() -> None:
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


def test_gold_feature_snapshot_v2_job_reads_builds_and_persists_managed_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cycles = object()
    telemetry = object()
    snapshots = object()
    write = SimpleNamespace(source_snapshot_count=2)
    status_counts = (EligibilityStatusCount("eligible", 2),)
    reason_counts = (EligibilityReasonCount("material_window_gap", 0),)
    lineage = ManagedFeatureSnapshotSourceLineage("dataset-v1", "source-sha", "batch-id")
    build = _ManagedFeatureSnapshotV2Build(
        telemetry_lineage=lineage,
        accepted_telemetry_record_count=7,
        cycle_count=2,
        eligibility_status_counts=status_counts,
        eligibility_reason_counts=reason_counts,
        snapshots=snapshots,
    )
    calls: list[tuple[object, ...]] = []

    class FakeSpark:
        def table(self, table_name: str) -> object:
            calls.append(("read", table_name))
            return {
                "workspace.railpulse_gold.loaded_cycles": cycles,
                "workspace.railpulse_silver.telemetry_accepted": telemetry,
            }[table_name]

    def fake_build(cycle_frame: object, telemetry_frame: object) -> object:
        calls.append(("build", cycle_frame, telemetry_frame))
        return build

    def fake_persist(frame: object, namespace: CatalogNamespace) -> object:
        calls.append(("persist", frame, namespace))
        return write

    monkeypatch.setattr(
        gold_feature_snapshot_v2_processing,
        "_build_managed_feature_snapshots_v2",
        fake_build,
    )
    monkeypatch.setattr(
        gold_feature_snapshot_v2_processing,
        "persist_catalog_feature_snapshots_v2",
        fake_persist,
    )

    result = run_managed_gold_feature_snapshot_v2_processing(FakeSpark(), NAMESPACE)

    assert result.contract_version == GOLD_FEATURE_SNAPSHOT_V2_PROCESSING_JOB_CONTRACT_VERSION
    assert result.snapshot_version == FEATURE_SNAPSHOT_V2_VERSION
    assert result.feature_version == MOTOR_CURRENT_FEATURE_VERSION
    assert result.eligibility_profile_version == MOTOR_CURRENT_ELIGIBILITY_PROFILE_VERSION
    assert result.eligibility_version == MOTOR_CURRENT_ELIGIBILITY_VERSION
    assert result.cycle_context_version == CYCLE_OPERATING_CONTEXT_VERSION
    assert result.telemetry_lineage is lineage
    assert result.accepted_telemetry_record_count == 7
    assert result.cycle_count == 2
    assert result.eligibility_status_counts is status_counts
    assert result.eligibility_reason_counts is reason_counts
    assert result.write is write
    assert calls == [
        ("read", "workspace.railpulse_gold.loaded_cycles"),
        ("read", "workspace.railpulse_silver.telemetry_accepted"),
        ("build", cycles, telemetry),
        ("persist", snapshots, NAMESPACE),
    ]


def test_gold_feature_snapshot_v2_job_rejects_write_count_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    build = _ManagedFeatureSnapshotV2Build(
        telemetry_lineage=ManagedFeatureSnapshotSourceLineage(
            "dataset-v1",
            "source-sha",
            "batch-id",
        ),
        accepted_telemetry_record_count=7,
        cycle_count=2,
        eligibility_status_counts=(),
        eligibility_reason_counts=(),
        snapshots=object(),
    )

    class FakeSpark:
        def table(self, table_name: str) -> object:
            return object()

    monkeypatch.setattr(
        gold_feature_snapshot_v2_processing,
        "_build_managed_feature_snapshots_v2",
        lambda cycles, telemetry: build,
    )
    monkeypatch.setattr(
        gold_feature_snapshot_v2_processing,
        "persist_catalog_feature_snapshots_v2",
        lambda snapshots, namespace: SimpleNamespace(source_snapshot_count=1),
    )

    with pytest.raises(
        ManagedGoldFeatureSnapshotV2ProcessingError,
        match="do not reconcile",
    ):
        run_managed_gold_feature_snapshot_v2_processing(FakeSpark(), NAMESPACE)
