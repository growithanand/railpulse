from __future__ import annotations

from datetime import UTC, datetime

import pytest

from railpulse.evaluation.chronological_splits import (
    CHRONOLOGICAL_SPLIT_SELECTION_VERSION,
    CHRONOLOGICAL_SPLIT_VERSION,
    PARTITION_TEST,
    PARTITION_TRAIN,
    PARTITION_VALIDATION,
    SELECTED_SPLIT_CANDIDATE_ID,
    SPLIT_CANDIDATES,
    ChronologicalSplitCandidate,
    ChronologicalSplitError,
    get_selected_split_candidate,
    get_split_candidate,
)


def test_declared_candidates_have_stable_unique_calendar_boundaries() -> None:
    assert CHRONOLOGICAL_SPLIT_VERSION == "calendar-chronological-splits-v1"
    assert len(SPLIT_CANDIDATES) == 3
    assert len({candidate.candidate_id for candidate in SPLIT_CANDIDATES}) == 3
    assert all(candidate.validation_start < candidate.test_start for candidate in SPLIT_CANDIDATES)


def test_candidate_assigns_boundary_timestamps_to_later_partition() -> None:
    candidate = get_split_candidate("validation-2020-06_test-2020-07")

    assert candidate.assign(datetime(2020, 5, 31, 23, 59, 59)) == PARTITION_TRAIN
    assert candidate.assign(datetime(2020, 6, 1)) == PARTITION_VALIDATION
    assert candidate.assign(datetime(2020, 6, 30, 23, 59, 59)) == PARTITION_VALIDATION
    assert candidate.assign(datetime(2020, 7, 1)) == PARTITION_TEST


def test_selected_candidate_freezes_reviewed_june_july_boundaries() -> None:
    candidate = get_selected_split_candidate()

    assert CHRONOLOGICAL_SPLIT_SELECTION_VERSION == "calendar-chronological-split-selection-v1"
    assert SELECTED_SPLIT_CANDIDATE_ID == "validation-2020-06_test-2020-07"
    assert candidate in SPLIT_CANDIDATES
    assert candidate.validation_start == datetime(2020, 6, 1)
    assert candidate.test_start == datetime(2020, 7, 1)


def test_candidate_rejects_invalid_boundaries_and_timestamps() -> None:
    with pytest.raises(ChronologicalSplitError, match="ID must be nonempty"):
        ChronologicalSplitCandidate(" ", datetime(2020, 5, 1), datetime(2020, 6, 1))

    with pytest.raises(ChronologicalSplitError, match="before the test"):
        ChronologicalSplitCandidate("reversed", datetime(2020, 7, 1), datetime(2020, 6, 1))

    with pytest.raises(ChronologicalSplitError, match="timezone-free"):
        ChronologicalSplitCandidate(
            "aware",
            datetime(2020, 5, 1, tzinfo=UTC),
            datetime(2020, 6, 1, tzinfo=UTC),
        )

    candidate = SPLIT_CANDIDATES[0]
    with pytest.raises(ChronologicalSplitError, match="must be a datetime"):
        candidate.assign(None)  # type: ignore[arg-type]

    with pytest.raises(ChronologicalSplitError, match="timezone-free"):
        candidate.assign(datetime(2020, 6, 1, tzinfo=UTC))

    with pytest.raises(ChronologicalSplitError, match="Unknown"):
        get_split_candidate("not-declared")
