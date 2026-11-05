from __future__ import annotations

import re
from pathlib import Path


QUERY_PATH = (
    Path(__file__).resolve().parents[2] / "sql" / "maintenance_dashboard_queries.sql"
)


def _query_text() -> str:
    return QUERY_PATH.read_text(encoding="utf-8")


def _statements() -> tuple[str, ...]:
    return tuple(statement.strip() for statement in _query_text().split(";") if statement.strip())


def _without_comments(statement: str) -> str:
    return "\n".join(
        line for line in statement.splitlines() if not line.lstrip().startswith("--")
    ).strip()


def test_dashboard_pack_contains_six_read_only_datasets() -> None:
    statements = _statements()

    assert len(statements) == 6
    assert [f"-- {index}." in statement for index, statement in enumerate(statements, start=1)] == [
        True,
    ] * 6
    for statement in statements:
        query = _without_comments(statement)
        assert query.startswith(("SELECT", "WITH"))
        assert not re.search(
            r"\b(CREATE|INSERT|UPDATE|DELETE|MERGE|DROP|ALTER|TRUNCATE|COPY|CALL)\b",
            query,
            flags=re.IGNORECASE,
        )


def test_dashboard_pack_freezes_the_reviewed_test_evidence() -> None:
    sql = _query_text()

    assert "'NOT_PRODUCTION_READY' AS readiness_status" in sql
    assert "4217 AS test_row_count" in sql
    assert "13 AS test_positive_count" in sql
    assert "4204 AS test_negative_count" in sql
    assert "0.0016708466249376135" in sql
    assert "0 AS detected_failure_event_count" in sql
    assert "1 AS missed_failure_event_count" in sql
    assert "73 AS false_alert_episode_count" in sql
    assert "1.1774193548387097" in sql
    assert "do not retune or rerun July" in sql


def test_dashboard_pack_exposes_no_row_level_model_evidence() -> None:
    query_text = "\n".join(_without_comments(statement) for statement in _statements()).lower()

    for prohibited_identifier in (
        "failure_probability",
        "matched_failure_record_id",
        "matched_failure_source_row",
        "modeling_exclusion_reasons",
        "report_raw",
        "source_report_number_raw",
    ):
        assert prohibited_identifier not in query_text
