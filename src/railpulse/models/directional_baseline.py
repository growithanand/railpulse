"""Transparent low-current baseline candidates for development evaluation."""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from railpulse.evaluation.modeling_view_schema import MODELING_VIEW_VERSION, STATUS_TRAINABLE
from railpulse.models.engineering_baseline import BASELINE_FEATURE_COLUMN

DIRECTIONAL_BASELINE_VERSION = "motor-current-low-directional-v1"
DIRECTIONAL_ALERT_COLUMN = "low_current_alert_candidate"


class DirectionalBaselineError(ValueError):
    """Raised when a directional baseline candidate cannot be applied safely."""


@dataclass(frozen=True)
class DirectionalThresholdCandidate:
    """One fixed low-current threshold candidate."""

    candidate_id: str
    maximum_mean_current_amperes: float

    def __post_init__(self) -> None:
        if not self.candidate_id.strip():
            raise DirectionalBaselineError("Directional candidate ID must be nonempty")
        if (
            not isfinite(self.maximum_mean_current_amperes)
            or self.maximum_mean_current_amperes <= 0
        ):
            raise DirectionalBaselineError(
                "Directional current threshold must be finite and positive"
            )


DIRECTIONAL_THRESHOLD_CANDIDATES = (
    DirectionalThresholdCandidate("mean-current-at-most-0.9a", 0.9),
    DirectionalThresholdCandidate("mean-current-at-most-1.0a", 1.0),
    DirectionalThresholdCandidate("mean-current-at-most-1.2a", 1.2),
    DirectionalThresholdCandidate("mean-current-at-most-1.5a", 1.5),
    DirectionalThresholdCandidate("mean-current-at-most-2.0a", 2.0),
)


def apply_directional_threshold(
    view: DataFrame,
    candidate: DirectionalThresholdCandidate,
) -> DataFrame:
    """Attach a nullable low-current alert candidate to trainable feature rows."""

    required_columns = {
        "modeling_view_version",
        "modeling_row_status",
        BASELINE_FEATURE_COLUMN,
    }
    missing_columns = sorted(required_columns - set(view.columns))
    if missing_columns:
        raise DirectionalBaselineError(
            "Modelling view is missing directional-baseline columns: " + ", ".join(missing_columns)
        )
    invalid_version = view.where(
        ~F.col("modeling_view_version").eqNullSafe(F.lit(MODELING_VIEW_VERSION))
    ).limit(1)
    if invalid_version.count():
        raise DirectionalBaselineError("Directional baseline requires the supported modelling view")

    scorable = (
        (F.col("modeling_row_status") == STATUS_TRAINABLE)
        & F.col(BASELINE_FEATURE_COLUMN).isNotNull()
        & ~F.isnan(BASELINE_FEATURE_COLUMN)
    )
    return view.withColumn(
        DIRECTIONAL_ALERT_COLUMN,
        F.when(
            scorable,
            F.col(BASELINE_FEATURE_COLUMN) <= F.lit(candidate.maximum_mean_current_amperes),
        ).cast("boolean"),
    )
