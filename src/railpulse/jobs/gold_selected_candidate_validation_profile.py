"""Profile the selected candidate against the governed managed modelling view."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

from railpulse.catalog import CatalogNamespace
from railpulse.evaluation.modeling_view_v2_schema import MODELING_VIEW_V2_TABLE
from railpulse.evaluation.modeling_view_v2_storage import (
    ModelingViewV2PersistenceError,
    catalog_modeling_view_v2_table,
)
from railpulse.models.candidate_development_data import (
    ModelCandidateDevelopmentDataError,
    prepare_model_candidate_development_data,
)
from railpulse.models.selected_candidate_validation import (
    SelectedCandidateValidationError,
    score_selected_candidate_validation,
)
from railpulse.models.selected_candidate_validation_profile import (
    SelectedCandidateValidationProfile,
    SelectedCandidateValidationProfileError,
    collect_selected_candidate_validation_profile,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

GOLD_SELECTED_CANDIDATE_VALIDATION_PROFILE_JOB_CONTRACT_VERSION = (
    "databricks-gold-selected-candidate-validation-profile-v1"
)


class ManagedGoldSelectedCandidateValidationProfileError(RuntimeError):
    """Raised when managed modelling rows cannot produce one safe validation profile."""


@dataclass(frozen=True)
class ManagedGoldSelectedCandidateValidationProfileResult:
    """Source identity and compact evidence from one read-only managed profile run."""

    contract_version: str
    source_table: str
    profile: SelectedCandidateValidationProfile


def run_managed_gold_selected_candidate_validation_profile(
    spark: SparkSession,
    namespace: CatalogNamespace,
) -> ManagedGoldSelectedCandidateValidationProfileResult:
    """Fit and summarize the retained candidate without scoring test rows or writing scores."""

    try:
        source_table = catalog_modeling_view_v2_table(namespace, MODELING_VIEW_V2_TABLE)
        view = spark.table(source_table)
        development = prepare_model_candidate_development_data(view)
        scores = score_selected_candidate_validation(development)
        profile = collect_selected_candidate_validation_profile(scores)
    except (
        ModelingViewV2PersistenceError,
        ModelCandidateDevelopmentDataError,
        SelectedCandidateValidationError,
        SelectedCandidateValidationProfileError,
    ) as exc:
        raise ManagedGoldSelectedCandidateValidationProfileError(str(exc)) from exc

    return ManagedGoldSelectedCandidateValidationProfileResult(
        contract_version=GOLD_SELECTED_CANDIDATE_VALIDATION_PROFILE_JOB_CONTRACT_VERSION,
        source_table=source_table,
        profile=profile,
    )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--bronze_schema", default="railpulse_bronze")
    parser.add_argument("--silver_schema", default="railpulse_silver")
    parser.add_argument("--gold_schema", default="railpulse_gold")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Print one compact managed validation profile as deterministic JSON."""

    from pyspark.sql import SparkSession

    args = _parse_args(argv)
    namespace = CatalogNamespace(
        catalog=args.catalog,
        bronze_schema=args.bronze_schema,
        silver_schema=args.silver_schema,
        gold_schema=args.gold_schema,
    )
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    result = run_managed_gold_selected_candidate_validation_profile(spark, namespace)
    print(json.dumps(asdict(result), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
