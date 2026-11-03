"""Verify the aggregate-only managed sealed-test runner."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("pyspark")

from railpulse.catalog import CatalogNamespace
from railpulse.jobs import gold_selected_candidate_sealed_test_evaluation
from railpulse.jobs.gold_selected_candidate_sealed_test_evaluation import (
    GOLD_SELECTED_CANDIDATE_SEALED_TEST_EVALUATION_JOB_CONTRACT_VERSION,
    ManagedGoldSelectedCandidateSealedTestEvaluationError,
    _parse_args,
    run_managed_gold_selected_candidate_sealed_test_evaluation,
)
from railpulse.models.selected_candidate_sealed_test_alert_evaluation import (
    SelectedCandidateSealedTestAlertEvaluationError,
)

NAMESPACE = CatalogNamespace(
    catalog="workspace",
    bronze_schema="railpulse_bronze",
    silver_schema="railpulse_silver",
    gold_schema="railpulse_gold",
)


def test_runner_parser_requires_explicit_opt_in_for_test_access() -> None:
    defaults = _parse_args([])
    approved = _parse_args(["--allow_sealed_test_read"])

    assert defaults.allow_sealed_test_read is False
    assert approved.allow_sealed_test_read is True


def test_runner_refuses_before_any_table_read_without_approval() -> None:
    class NoReadSpark:
        def table(self, table_name: str) -> object:
            raise AssertionError(f"Unexpected table read: {table_name}")

    with pytest.raises(
        ManagedGoldSelectedCandidateSealedTestEvaluationError,
        match="explicit test-read approval",
    ):
        run_managed_gold_selected_candidate_sealed_test_evaluation(
            NoReadSpark(),
            NAMESPACE,
            sealed_test_access_approved=False,
        )


def test_runner_reads_allowlisted_sources_and_returns_only_approved_aggregates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    view = object()
    failures = object()
    scores = SimpleNamespace(
        adapter_version="scoring-adapter-v1",
        model_candidate_id="motor-current-only",
        test_summary=SimpleNamespace(row_count=40, positive_count=2, negative_count=38),
        test_area_under_pr=0.25,
    )
    event_evaluation = SimpleNamespace(warning_lead_time_seconds=3_540)
    policy_evaluation = SimpleNamespace(
        policy_candidate_id="p95-two-consecutive-cycles",
        detected_failure_event_count=1,
        missed_failure_event_count=0,
        event_detection_rate=1.0,
        alert_episode_count=3,
        warning_episode_count=1,
        inside_failure_episode_count=0,
        false_alert_episode_count=2,
        alert_episodes_per_operating_day=1.5,
        false_alert_episodes_per_operating_day=1.0,
        event_evaluations=(event_evaluation,),
    )
    evaluation = SimpleNamespace(
        contract_version="motor-current-sealed-test-evaluation-contract-v1",
        adapter_version="alert-adapter-v1",
        scoring_result=scores,
        operating_day_count=2,
        accepted_failure_event_count=1,
        policy_evaluation=policy_evaluation,
    )
    calls: list[tuple[object, ...]] = []

    class FakeSpark:
        def table(self, table_name: str) -> object:
            calls.append(("read", table_name))
            return {
                "workspace.railpulse_gold.modeling_view_v2": view,
                "workspace.railpulse_silver.failure_events_accepted": failures,
            }[table_name]

    def fake_score(frame: object) -> object:
        calls.append(("score", frame))
        return scores

    def fake_evaluate(score_result: object, failure_frame: object) -> object:
        calls.append(("evaluate", score_result, failure_frame))
        return evaluation

    monkeypatch.setattr(
        gold_selected_candidate_sealed_test_evaluation,
        "score_selected_candidate_sealed_test",
        fake_score,
    )
    monkeypatch.setattr(
        gold_selected_candidate_sealed_test_evaluation,
        "evaluate_selected_candidate_sealed_test_alerts",
        fake_evaluate,
    )

    result = run_managed_gold_selected_candidate_sealed_test_evaluation(
        FakeSpark(),
        NAMESPACE,
        sealed_test_access_approved=True,
    )

    assert (
        result.job_contract_version
        == GOLD_SELECTED_CANDIDATE_SEALED_TEST_EVALUATION_JOB_CONTRACT_VERSION
    )
    assert result.modeling_source_table == "workspace.railpulse_gold.modeling_view_v2"
    assert result.failure_source_table == "workspace.railpulse_silver.failure_events_accepted"
    assert result.model_candidate_id == "motor-current-only"
    assert result.alert_policy_candidate_id == "p95-two-consecutive-cycles"
    assert result.metrics.test_row_count == 40
    assert result.metrics.test_positive_count == 2
    assert result.metrics.test_negative_count == 38
    assert result.metrics.test_area_under_precision_recall_curve == 0.25
    assert result.metrics.warning_lead_time_seconds == 3_540
    assert result.metrics.false_alert_episode_count == 2
    assert not hasattr(result, "scores")
    assert not hasattr(result, "episodes")
    assert not hasattr(result.metrics, "failure_record_id")
    assert calls == [
        ("read", "workspace.railpulse_gold.modeling_view_v2"),
        ("read", "workspace.railpulse_silver.failure_events_accepted"),
        ("score", view),
        ("evaluate", scores, failures),
    ]


def test_runner_wraps_guarded_evaluation_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeSpark:
        def table(self, table_name: str) -> object:
            return object()

    monkeypatch.setattr(
        gold_selected_candidate_sealed_test_evaluation,
        "score_selected_candidate_sealed_test",
        lambda frame: object(),
    )
    monkeypatch.setattr(
        gold_selected_candidate_sealed_test_evaluation,
        "evaluate_selected_candidate_sealed_test_alerts",
        lambda scores, failures: (_ for _ in ()).throw(
            SelectedCandidateSealedTestAlertEvaluationError("unsafe held-out evidence")
        ),
    )

    with pytest.raises(
        ManagedGoldSelectedCandidateSealedTestEvaluationError,
        match="unsafe held-out evidence",
    ):
        run_managed_gold_selected_candidate_sealed_test_evaluation(
            FakeSpark(),
            NAMESPACE,
            sealed_test_access_approved=True,
        )
