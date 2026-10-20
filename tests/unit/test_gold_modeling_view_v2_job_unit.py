from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from railpulse.catalog import CatalogNamespace
from railpulse.evaluation.modeling_view_v2_schema import MODELING_VIEW_V2_VERSION
from railpulse.features.failure_horizons import FAILURE_HORIZON_VERSION
from railpulse.features.feature_snapshot_v2_schema import FEATURE_SNAPSHOT_V2_VERSION
from railpulse.jobs import gold_modeling_view_v2_processing
from railpulse.jobs.gold_modeling_view_v2_processing import (
    GOLD_MODELING_VIEW_V2_PROCESSING_JOB_CONTRACT_VERSION,
    ManagedGoldModelingViewV2ProcessingError,
    ManagedModelingFailureLineage,
    ManagedModelingFeatureLineage,
    _parse_args,
    run_managed_gold_modeling_view_v2_processing,
)

NAMESPACE = CatalogNamespace(
    catalog="workspace",
    bronze_schema="railpulse_bronze",
    silver_schema="railpulse_silver",
    gold_schema="railpulse_gold",
)


def test_gold_modeling_view_v2_job_parses_bundle_named_parameters() -> None:
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


def test_gold_modeling_view_v2_job_reads_profiles_and_persists_managed_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshots = object()
    horizons = object()
    failures = object()
    modeling_rows = object()
    feature_lineage = ManagedModelingFeatureLineage(
        "dataset-v1",
        "telemetry-sha",
        "telemetry-batch",
    )
    failure_rows = (
        SimpleNamespace(record_id="failure-1"),
        SimpleNamespace(record_id="failure-2"),
    )
    failure_lineage_values = (
        "dataset-v1",
        "failure-sha",
        "failure-document-sha",
        "failure-batch",
    )
    summary = SimpleNamespace(
        cycle_count=3,
        failure_event_counts=(
            SimpleNamespace(trainable_positive_cycle_count=2),
            SimpleNamespace(trainable_positive_cycle_count=0),
        ),
    )
    write = SimpleNamespace(source_row_count=3)
    calls: list[tuple[object, ...]] = []

    class FakeSpark:
        def table(self, table_name: str) -> object:
            calls.append(("read", table_name))
            return {
                "workspace.railpulse_gold.feature_snapshots_v2": snapshots,
                "workspace.railpulse_gold.failure_horizons": horizons,
                "workspace.railpulse_silver.failure_events_accepted": failures,
            }[table_name]

    def fake_feature_lineage(frame: object) -> ManagedModelingFeatureLineage:
        calls.append(("feature_lineage", frame))
        return feature_lineage

    def fake_failure_events(frame: object) -> tuple[tuple[object, ...], tuple[str, ...]]:
        calls.append(("failure_events", frame))
        return failure_rows, failure_lineage_values

    def fake_build(
        snapshot_frame: object,
        horizon_frame: object,
        *,
        failure_dataset_version: str,
        failure_source_sha256: str,
        failure_ingestion_batch_id: str,
    ) -> object:
        calls.append(
            (
                "build",
                snapshot_frame,
                horizon_frame,
                failure_dataset_version,
                failure_source_sha256,
                failure_ingestion_batch_id,
            )
        )
        return modeling_rows

    def fake_summary(
        frame: object,
        rows: tuple[object, ...],
        *,
        modeling_view_version: str,
        exclusion_reason_order: tuple[str, ...],
    ) -> object:
        calls.append(
            (
                "summary",
                frame,
                rows,
                modeling_view_version,
                exclusion_reason_order,
            )
        )
        return summary

    def fake_persist(frame: object, namespace: CatalogNamespace) -> object:
        calls.append(("persist", frame, namespace))
        return write

    monkeypatch.setattr(
        gold_modeling_view_v2_processing,
        "_single_feature_lineage",
        fake_feature_lineage,
    )
    monkeypatch.setattr(
        gold_modeling_view_v2_processing,
        "_collect_failure_events",
        fake_failure_events,
    )
    monkeypatch.setattr(gold_modeling_view_v2_processing, "build_modeling_view_v2", fake_build)
    monkeypatch.setattr(
        gold_modeling_view_v2_processing,
        "collect_modeling_view_summary",
        fake_summary,
    )
    monkeypatch.setattr(
        gold_modeling_view_v2_processing,
        "persist_catalog_modeling_view_v2",
        fake_persist,
    )

    result = run_managed_gold_modeling_view_v2_processing(FakeSpark(), NAMESPACE)

    assert result.contract_version == GOLD_MODELING_VIEW_V2_PROCESSING_JOB_CONTRACT_VERSION
    assert result.modeling_view_version == MODELING_VIEW_V2_VERSION
    assert result.feature_snapshot_version == FEATURE_SNAPSHOT_V2_VERSION
    assert result.failure_horizon_version == FAILURE_HORIZON_VERSION
    assert result.feature_lineage is feature_lineage
    assert result.failure_lineage == ManagedModelingFailureLineage(*failure_lineage_values)
    assert result.accepted_failure_event_count == 2
    assert result.matched_trainable_failure_event_count == 1
    assert result.summary is summary
    assert result.write is write
    assert calls[0:3] == [
        ("read", "workspace.railpulse_gold.feature_snapshots_v2"),
        ("read", "workspace.railpulse_gold.failure_horizons"),
        ("read", "workspace.railpulse_silver.failure_events_accepted"),
    ]
    assert calls[3:5] == [
        ("feature_lineage", snapshots),
        ("failure_events", failures),
    ]
    assert calls[5] == (
        "build",
        snapshots,
        horizons,
        "dataset-v1",
        "failure-sha",
        "failure-batch",
    )
    assert calls[-1] == ("persist", modeling_rows, NAMESPACE)


def test_gold_modeling_view_v2_job_rejects_mixed_dataset_versions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshots = object()
    horizons = object()
    failures = object()

    class FakeSpark:
        def table(self, table_name: str) -> object:
            return {
                "workspace.railpulse_gold.feature_snapshots_v2": snapshots,
                "workspace.railpulse_gold.failure_horizons": horizons,
                "workspace.railpulse_silver.failure_events_accepted": failures,
            }[table_name]

    monkeypatch.setattr(
        gold_modeling_view_v2_processing,
        "_single_feature_lineage",
        lambda frame: ManagedModelingFeatureLineage(
            "feature-dataset-v1",
            "telemetry-sha",
            "telemetry-batch",
        ),
    )
    monkeypatch.setattr(
        gold_modeling_view_v2_processing,
        "_collect_failure_events",
        lambda frame: (
            (SimpleNamespace(record_id="failure-1"),),
            ("failure-dataset-v2", "failure-sha", "document-sha", "failure-batch"),
        ),
    )

    with pytest.raises(ManagedGoldModelingViewV2ProcessingError, match="do not match"):
        run_managed_gold_modeling_view_v2_processing(FakeSpark(), NAMESPACE)


def test_gold_modeling_view_v2_job_rejects_unreconciled_summary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshots = object()
    horizons = object()
    failures = object()

    class FakeSpark:
        def table(self, table_name: str) -> object:
            return {
                "workspace.railpulse_gold.feature_snapshots_v2": snapshots,
                "workspace.railpulse_gold.failure_horizons": horizons,
                "workspace.railpulse_silver.failure_events_accepted": failures,
            }[table_name]

    monkeypatch.setattr(
        gold_modeling_view_v2_processing,
        "_single_feature_lineage",
        lambda frame: ManagedModelingFeatureLineage(
            "dataset-v1",
            "telemetry-sha",
            "telemetry-batch",
        ),
    )
    monkeypatch.setattr(
        gold_modeling_view_v2_processing,
        "_collect_failure_events",
        lambda frame: (
            (SimpleNamespace(record_id="failure-1"),),
            ("dataset-v1", "failure-sha", "document-sha", "failure-batch"),
        ),
    )
    monkeypatch.setattr(
        gold_modeling_view_v2_processing,
        "build_modeling_view_v2",
        lambda *args, **kwargs: object(),
    )
    monkeypatch.setattr(
        gold_modeling_view_v2_processing,
        "collect_modeling_view_summary",
        lambda *args, **kwargs: SimpleNamespace(cycle_count=3, failure_event_counts=()),
    )
    monkeypatch.setattr(
        gold_modeling_view_v2_processing,
        "persist_catalog_modeling_view_v2",
        lambda *args, **kwargs: SimpleNamespace(source_row_count=2),
    )

    with pytest.raises(ManagedGoldModelingViewV2ProcessingError, match="do not reconcile"):
        run_managed_gold_modeling_view_v2_processing(FakeSpark(), NAMESPACE)
