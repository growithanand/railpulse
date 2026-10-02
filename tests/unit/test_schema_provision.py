from __future__ import annotations

import pytest

from railpulse.catalog import CatalogNamespace
from railpulse.jobs.schema_provision import (
    SCHEMA_PROVISION_CONTRACT_VERSION,
    SchemaProvisionError,
    provision_databricks_schemas,
)

NAMESPACE = CatalogNamespace(
    catalog="workspace",
    bronze_schema="railpulse_bronze",
    silver_schema="railpulse_silver",
    gold_schema="railpulse_gold",
)
TARGET_SCHEMAS = {
    "railpulse_bronze",
    "railpulse_silver",
    "railpulse_gold",
}


class _FakeQueryResult:
    def __init__(self, *, row: object | None = None, rows: list[object] | None = None) -> None:
        self._row = row
        self._rows = rows or []

    def first(self) -> object | None:
        return self._row

    def collect(self) -> list[object]:
        return self._rows


class _FakeSpark:
    def __init__(
        self,
        *,
        catalog: str = "workspace",
        schemas: set[str] | None = None,
        unavailable_after_create: set[str] | None = None,
    ) -> None:
        self.catalog = catalog
        self.schemas = set(schemas or set())
        self.unavailable_after_create = set(unavailable_after_create or set())
        self.queries: list[str] = []

    def sql(self, query: str) -> _FakeQueryResult:
        self.queries.append(query)
        if query == "SELECT current_catalog() AS current_catalog":
            return _FakeQueryResult(row={"current_catalog": self.catalog})
        if query.startswith("SELECT schema_name FROM"):
            rows = [
                {"schema_name": name} for name in sorted(self.schemas.intersection(TARGET_SCHEMAS))
            ]
            return _FakeQueryResult(rows=rows)
        if query.startswith("CREATE SCHEMA IF NOT EXISTS"):
            qualified_name = query.split()[5]
            schema_name = qualified_name.split(".", maxsplit=1)[1]
            if schema_name not in self.unavailable_after_create:
                self.schemas.add(schema_name)
            return _FakeQueryResult()
        raise AssertionError(f"Unexpected query: {query}")


def test_schema_provision_creates_only_missing_schemas_and_reconciles() -> None:
    spark = _FakeSpark(schemas={"railpulse_bronze"})

    result = provision_databricks_schemas(spark, NAMESPACE)

    assert result.contract_version == SCHEMA_PROVISION_CONTRACT_VERSION
    assert result.catalog == "workspace"
    assert result.requested_schemas == (
        "workspace.railpulse_bronze",
        "workspace.railpulse_silver",
        "workspace.railpulse_gold",
    )
    assert result.preexisting_schemas == ("workspace.railpulse_bronze",)
    assert result.newly_available_schemas == (
        "workspace.railpulse_silver",
        "workspace.railpulse_gold",
    )
    create_queries = [query for query in spark.queries if query.startswith("CREATE SCHEMA")]
    assert create_queries == [
        "CREATE SCHEMA IF NOT EXISTS workspace.railpulse_silver COMMENT 'RailPulse validated data'",
        "CREATE SCHEMA IF NOT EXISTS workspace.railpulse_gold "
        "COMMENT 'RailPulse decision-support data'",
    ]


def test_schema_provision_rerun_is_a_noop() -> None:
    spark = _FakeSpark(schemas=set(TARGET_SCHEMAS))

    result = provision_databricks_schemas(spark, NAMESPACE)

    assert result.preexisting_schemas == result.requested_schemas
    assert result.newly_available_schemas == ()
    assert not any(query.startswith("CREATE SCHEMA") for query in spark.queries)


def test_schema_provision_rejects_unexpected_current_catalog_before_writes() -> None:
    spark = _FakeSpark(catalog="other_catalog")

    with pytest.raises(SchemaProvisionError, match="does not match configured catalog"):
        provision_databricks_schemas(spark, NAMESPACE)

    assert spark.queries == ["SELECT current_catalog() AS current_catalog"]


def test_schema_provision_fails_when_post_write_inventory_is_incomplete() -> None:
    spark = _FakeSpark(unavailable_after_create={"railpulse_gold"})

    with pytest.raises(SchemaProvisionError, match="did not make all requested schemas visible"):
        provision_databricks_schemas(spark, NAMESPACE)
