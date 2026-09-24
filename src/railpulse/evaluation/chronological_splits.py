"""Fixed calendar candidates for leakage-safe chronological evaluation splits."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

CHRONOLOGICAL_SPLIT_VERSION = "calendar-chronological-splits-v1"
CHRONOLOGICAL_SPLIT_SELECTION_VERSION = "calendar-chronological-split-selection-v1"
SELECTED_SPLIT_CANDIDATE_ID = "validation-2020-06_test-2020-07"

PARTITION_TRAIN = "train"
PARTITION_VALIDATION = "validation"
PARTITION_TEST = "test"


class ChronologicalSplitError(ValueError):
    """Raised when a chronological split candidate is invalid or cannot assign a row."""


@dataclass(frozen=True)
class ChronologicalSplitCandidate:
    """Two calendar boundaries defining contiguous train, validation, and test periods."""

    candidate_id: str
    validation_start: datetime
    test_start: datetime

    def __post_init__(self) -> None:
        if not self.candidate_id.strip():
            raise ChronologicalSplitError("Split candidate ID must be nonempty")
        if self.validation_start.tzinfo is not None or self.test_start.tzinfo is not None:
            raise ChronologicalSplitError("Split boundaries must be timezone-free timestamps")
        if self.validation_start >= self.test_start:
            raise ChronologicalSplitError("Validation must start before the test period")

    def assign(self, prediction_timestamp: datetime) -> str:
        """Assign one prediction timestamp without inspecting features or labels."""

        if not isinstance(prediction_timestamp, datetime):
            raise ChronologicalSplitError("Prediction timestamp must be a datetime")
        if prediction_timestamp.tzinfo is not None:
            raise ChronologicalSplitError("Prediction timestamp must be timezone-free")
        if prediction_timestamp < self.validation_start:
            return PARTITION_TRAIN
        if prediction_timestamp < self.test_start:
            return PARTITION_VALIDATION
        return PARTITION_TEST


SPLIT_CANDIDATES = (
    ChronologicalSplitCandidate(
        candidate_id="validation-2020-05_test-2020-07",
        validation_start=datetime(2020, 5, 1),
        test_start=datetime(2020, 7, 1),
    ),
    ChronologicalSplitCandidate(
        candidate_id="validation-2020-06_test-2020-07",
        validation_start=datetime(2020, 6, 1),
        test_start=datetime(2020, 7, 1),
    ),
    ChronologicalSplitCandidate(
        candidate_id="validation-2020-06_test-2020-08",
        validation_start=datetime(2020, 6, 1),
        test_start=datetime(2020, 8, 1),
    ),
)


def get_split_candidate(candidate_id: str) -> ChronologicalSplitCandidate:
    """Return one declared candidate by its stable identifier."""

    for candidate in SPLIT_CANDIDATES:
        if candidate.candidate_id == candidate_id:
            return candidate
    raise ChronologicalSplitError(f"Unknown chronological split candidate: {candidate_id}")


def get_selected_split_candidate() -> ChronologicalSplitCandidate:
    """Return the reviewed split frozen for baseline and model evaluation."""

    return get_split_candidate(SELECTED_SPLIT_CANDIDATE_ID)
