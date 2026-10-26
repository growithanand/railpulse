from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("pyspark")

from railpulse.catalog import CatalogNamespace
from railpulse.jobs import gold_selected_candidate_validation_profile
from railpulse.jobs.gold_selected_candidate_validation_profile import (
    GOLD_SELECTED_CANDIDATE_VALIDATION_PROFILE_JOB_CONTRACT_VERSION,
    ManagedGoldSelectedCandidateValidationProfileError,
    _parse_args,
    run_managed_gold_selected_candidate_validation_profile,
)
from railpulse.models.selected_candidate_validation import SelectedCandidateValidationError

NAMESPACE = CatalogNamespace(
    catalog="workspace",
    bronze_schema="railpulse_bronze",
    silver_schema="railpulse_silver",
    gold_schema="railpulse_gold",
)


def test_validation_profile_job_parses_bundle_named_parameters() -> None:
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


def test_validation_profile_job_reads_prepares_scores_and_profiles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    view = object()
    development = object()
    scores = object()
    profile = SimpleNamespace(profile_version="profile-v1")
    calls: list[tuple[object, ...]] = []

    class FakeSpark:
        def table(self, table_name: str) -> object:
            calls.append(("read", table_name))
            return view

    def fake_prepare(frame: object) -> object:
        calls.append(("prepare", frame))
        return development

    def fake_score(data: object) -> object:
        calls.append(("score", data))
        return scores

    def fake_profile(result: object) -> object:
        calls.append(("profile", result))
        return profile

    monkeypatch.setattr(
        gold_selected_candidate_validation_profile,
        "prepare_model_candidate_development_data",
        fake_prepare,
    )
    monkeypatch.setattr(
        gold_selected_candidate_validation_profile,
        "score_selected_candidate_validation",
        fake_score,
    )
    monkeypatch.setattr(
        gold_selected_candidate_validation_profile,
        "collect_selected_candidate_validation_profile",
        fake_profile,
    )

    result = run_managed_gold_selected_candidate_validation_profile(FakeSpark(), NAMESPACE)

    assert (
        result.contract_version == GOLD_SELECTED_CANDIDATE_VALIDATION_PROFILE_JOB_CONTRACT_VERSION
    )
    assert result.source_table == "workspace.railpulse_gold.modeling_view_v2"
    assert result.profile is profile
    assert calls == [
        ("read", "workspace.railpulse_gold.modeling_view_v2"),
        ("prepare", view),
        ("score", development),
        ("profile", scores),
    ]


def test_validation_profile_job_wraps_scoring_contract_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    view = object()

    class FakeSpark:
        def table(self, table_name: str) -> object:
            return view

    monkeypatch.setattr(
        gold_selected_candidate_validation_profile,
        "prepare_model_candidate_development_data",
        lambda frame: object(),
    )
    monkeypatch.setattr(
        gold_selected_candidate_validation_profile,
        "score_selected_candidate_validation",
        lambda data: (_ for _ in ()).throw(SelectedCandidateValidationError("unsafe rows")),
    )

    with pytest.raises(ManagedGoldSelectedCandidateValidationProfileError, match="unsafe rows"):
        run_managed_gold_selected_candidate_validation_profile(FakeSpark(), NAMESPACE)
