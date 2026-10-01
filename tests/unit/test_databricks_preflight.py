from __future__ import annotations

import pytest

from railpulse import __version__
from railpulse.catalog import CatalogNamespace
from railpulse.jobs.preflight import (
    PREFLIGHT_CONTRACT_VERSION,
    DatabricksPreflightError,
    collect_databricks_preflight,
)

NAMESPACE = CatalogNamespace(
    catalog="workspace",
    bronze_schema="railpulse_bronze",
    silver_schema="railpulse_silver",
    gold_schema="railpulse_gold",
)


class _FakeQueryResult:
    def __init__(self, row: object | None) -> None:
        self._row = row

    def first(self) -> object | None:
        return self._row


class _FakeSpark:
    version = "4.2.0"

    def __init__(self, row: object | None) -> None:
        self._row = row
        self.queries: list[str] = []

    def sql(self, query: str) -> _FakeQueryResult:
        self.queries.append(query)
        return _FakeQueryResult(self._row)


def test_preflight_collects_versioned_read_only_runtime_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "serverless")
    spark = _FakeSpark({"current_catalog": "workspace", "current_schema": "default"})

    result = collect_databricks_preflight(spark, NAMESPACE)

    assert result.contract_version == PREFLIGHT_CONTRACT_VERSION
    assert result.package_version == __version__
    assert result.spark_version == "4.2.0"
    assert result.databricks_runtime_version == "serverless"
    assert result.current_catalog == "workspace"
    assert result.current_schema == "default"
    assert result.planned_bronze_schema == "workspace.railpulse_bronze"
    assert result.planned_silver_schema == "workspace.railpulse_silver"
    assert result.planned_gold_schema == "workspace.railpulse_gold"
    assert spark.queries == [
        "SELECT current_catalog() AS current_catalog, current_schema() AS current_schema"
    ]


@pytest.mark.parametrize(
    ("row", "message"),
    [
        (None, "no catalog context row"),
        ({"current_catalog": "", "current_schema": "default"}, "no current catalog"),
        ({"unexpected": "value"}, "incompatible catalog context row"),
    ],
)
def test_preflight_rejects_missing_or_incompatible_runtime_context(
    row: object | None,
    message: str,
) -> None:
    with pytest.raises(DatabricksPreflightError, match=message):
        collect_databricks_preflight(_FakeSpark(row), NAMESPACE)


def test_preflight_rejects_unexpected_current_catalog() -> None:
    spark = _FakeSpark({"current_catalog": "other_catalog", "current_schema": "default"})

    with pytest.raises(DatabricksPreflightError, match="does not match configured catalog"):
        collect_databricks_preflight(spark, NAMESPACE)
