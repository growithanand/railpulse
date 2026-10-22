from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("pyspark")

from railpulse.catalog import CatalogNamespace
from railpulse.jobs import gold_model_candidate_comparison
from railpulse.jobs.gold_model_candidate_comparison import (
    GOLD_MODEL_CANDIDATE_COMPARISON_JOB_CONTRACT_VERSION,
    ManagedGoldModelCandidateComparisonError,
    _parse_args,
    run_managed_gold_model_candidate_comparison,
)
from railpulse.models.candidate_development_data import ModelCandidateDevelopmentDataError

NAMESPACE = CatalogNamespace(
    catalog="workspace",
    bronze_schema="railpulse_bronze",
    silver_schema="railpulse_silver",
    gold_schema="railpulse_gold",
)


def test_model_candidate_job_parses_explicit_tracking_identity() -> None:
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
            "--experiment_name",
            "/Shared/railpulse-candidate-comparison",
            "--code_version",
            "reviewed-code-version",
        ]
    )

    assert vars(args) == {
        "catalog": "catalog_name",
        "bronze_schema": "bronze_name",
        "silver_schema": "silver_name",
        "gold_schema": "gold_name",
        "experiment_name": "/Shared/railpulse-candidate-comparison",
        "code_version": "reviewed-code-version",
    }


def test_model_candidate_job_reads_prepares_compares_and_tracks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    view = object()
    context = SimpleNamespace(experiment_name="experiment")
    development_data = object()
    comparison = object()
    tracking = object()
    mlflow_api = object()
    calls: list[tuple[object, ...]] = []

    class FakeSpark:
        def table(self, table_name: str) -> object:
            calls.append(("read", table_name))
            return view

    def fake_context(
        frame: object,
        *,
        experiment_name: str,
        code_version: str,
    ) -> object:
        calls.append(("context", frame, experiment_name, code_version))
        return context

    def fake_prepare(frame: object) -> object:
        calls.append(("prepare", frame))
        return development_data

    def fake_compare(data: object) -> object:
        calls.append(("compare", data))
        return comparison

    def fake_track(
        result: object,
        tracking_context: object,
        *,
        mlflow_api: object,
    ) -> object:
        calls.append(("track", result, tracking_context, mlflow_api))
        return tracking

    monkeypatch.setattr(
        gold_model_candidate_comparison,
        "_collect_candidate_tracking_context",
        fake_context,
    )
    monkeypatch.setattr(
        gold_model_candidate_comparison,
        "prepare_model_candidate_development_data",
        fake_prepare,
    )
    monkeypatch.setattr(
        gold_model_candidate_comparison,
        "compare_model_candidates",
        fake_compare,
    )
    monkeypatch.setattr(
        gold_model_candidate_comparison,
        "track_candidate_comparison",
        fake_track,
    )

    result = run_managed_gold_model_candidate_comparison(
        FakeSpark(),
        NAMESPACE,
        experiment_name="experiment",
        code_version="code-version",
        mlflow_api=mlflow_api,
    )

    assert result.contract_version == GOLD_MODEL_CANDIDATE_COMPARISON_JOB_CONTRACT_VERSION
    assert result.source_table == "workspace.railpulse_gold.modeling_view_v2"
    assert result.tracking_context is context
    assert result.comparison is comparison
    assert result.tracking is tracking
    assert calls == [
        ("read", "workspace.railpulse_gold.modeling_view_v2"),
        ("context", view, "experiment", "code-version"),
        ("prepare", view),
        ("compare", development_data),
        ("track", comparison, context, mlflow_api),
    ]


def test_model_candidate_job_wraps_development_contract_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    view = object()

    class FakeSpark:
        def table(self, table_name: str) -> object:
            return view

    monkeypatch.setattr(
        gold_model_candidate_comparison,
        "_collect_candidate_tracking_context",
        lambda *args, **kwargs: object(),
    )
    monkeypatch.setattr(
        gold_model_candidate_comparison,
        "prepare_model_candidate_development_data",
        lambda frame: (_ for _ in ()).throw(ModelCandidateDevelopmentDataError("unsafe rows")),
    )

    with pytest.raises(ManagedGoldModelCandidateComparisonError, match="unsafe rows"):
        run_managed_gold_model_candidate_comparison(
            FakeSpark(),
            NAMESPACE,
            experiment_name="experiment",
            code_version="code-version",
            mlflow_api=object(),
        )
