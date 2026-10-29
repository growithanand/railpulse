from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("pyspark")

from railpulse.catalog import CatalogNamespace
from railpulse.jobs import gold_selected_candidate_validation_alert_evaluation
from railpulse.jobs.gold_selected_candidate_validation_alert_evaluation import (
    GOLD_SELECTED_CANDIDATE_VALIDATION_ALERT_EVALUATION_JOB_CONTRACT_VERSION,
    ManagedGoldSelectedCandidateValidationAlertEvaluationError,
    _parse_args,
    run_managed_gold_selected_candidate_validation_alert_evaluation,
)
from railpulse.models.selected_candidate_validation_alert_evaluation import (
    SelectedCandidateValidationAlertEvaluationError,
)

NAMESPACE = CatalogNamespace(
    catalog="workspace",
    bronze_schema="railpulse_bronze",
    silver_schema="railpulse_silver",
    gold_schema="railpulse_gold",
)


def test_validation_alert_evaluation_job_parses_bundle_named_parameters() -> None:
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


def test_validation_alert_evaluation_job_reads_scores_and_returns_compact_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    view = object()
    failures = object()
    development = object()
    scores = object()
    profile = SimpleNamespace(profile_version="profile-v1")
    policy_evaluations = (
        SimpleNamespace(policy_candidate_id="p90-single-cycle"),
        SimpleNamespace(policy_candidate_id="p95-single-cycle"),
    )
    evaluation = SimpleNamespace(
        adapter_version="evaluation-adapter-v1",
        evaluation_version="evaluation-v1",
        evaluated_cycle_count=4_730,
        operating_day_count=30,
        accepted_failure_event_count=1,
        alert_candidates=SimpleNamespace(profile=profile),
        policy_evaluations=policy_evaluations,
    )
    calls: list[tuple[object, ...]] = []

    class FakeSpark:
        def table(self, table_name: str) -> object:
            calls.append(("read", table_name))
            return {
                "workspace.railpulse_gold.modeling_view_v2": view,
                "workspace.railpulse_silver.failure_events_accepted": failures,
            }[table_name]

    def fake_prepare(frame: object) -> object:
        calls.append(("prepare", frame))
        return development

    def fake_score(data: object) -> object:
        calls.append(("score", data))
        return scores

    def fake_evaluate(score_result: object, failure_frame: object) -> object:
        calls.append(("evaluate", score_result, failure_frame))
        return evaluation

    monkeypatch.setattr(
        gold_selected_candidate_validation_alert_evaluation,
        "prepare_model_candidate_development_data",
        fake_prepare,
    )
    monkeypatch.setattr(
        gold_selected_candidate_validation_alert_evaluation,
        "score_selected_candidate_validation",
        fake_score,
    )
    monkeypatch.setattr(
        gold_selected_candidate_validation_alert_evaluation,
        "evaluate_selected_candidate_validation_alerts",
        fake_evaluate,
    )

    result = run_managed_gold_selected_candidate_validation_alert_evaluation(
        FakeSpark(),
        NAMESPACE,
    )

    assert (
        result.contract_version
        == GOLD_SELECTED_CANDIDATE_VALIDATION_ALERT_EVALUATION_JOB_CONTRACT_VERSION
    )
    assert result.modeling_source_table == "workspace.railpulse_gold.modeling_view_v2"
    assert result.failure_source_table == "workspace.railpulse_silver.failure_events_accepted"
    assert result.evaluation_adapter_version == "evaluation-adapter-v1"
    assert result.evaluation_version == "evaluation-v1"
    assert result.evaluated_cycle_count == 4_730
    assert result.operating_day_count == 30
    assert result.accepted_failure_event_count == 1
    assert result.profile is profile
    assert result.policy_evaluations is policy_evaluations
    assert not hasattr(result, "alert_candidates")
    assert calls == [
        ("read", "workspace.railpulse_gold.modeling_view_v2"),
        ("read", "workspace.railpulse_silver.failure_events_accepted"),
        ("prepare", view),
        ("score", development),
        ("evaluate", scores, failures),
    ]


def test_validation_alert_evaluation_job_wraps_evaluation_contract_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeSpark:
        def table(self, table_name: str) -> object:
            return object()

    monkeypatch.setattr(
        gold_selected_candidate_validation_alert_evaluation,
        "prepare_model_candidate_development_data",
        lambda frame: object(),
    )
    monkeypatch.setattr(
        gold_selected_candidate_validation_alert_evaluation,
        "score_selected_candidate_validation",
        lambda data: object(),
    )
    monkeypatch.setattr(
        gold_selected_candidate_validation_alert_evaluation,
        "evaluate_selected_candidate_validation_alerts",
        lambda scores, failures: (_ for _ in ()).throw(
            SelectedCandidateValidationAlertEvaluationError("unsafe alert evidence")
        ),
    )

    with pytest.raises(
        ManagedGoldSelectedCandidateValidationAlertEvaluationError,
        match="unsafe alert evidence",
    ):
        run_managed_gold_selected_candidate_validation_alert_evaluation(
            FakeSpark(),
            NAMESPACE,
        )
