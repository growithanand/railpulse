"""Idempotent Unity Catalog schema provisioning for managed RailPulse workloads."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Protocol

from railpulse.catalog import CatalogLayer, CatalogNamespace

SCHEMA_PROVISION_CONTRACT_VERSION = "databricks-schema-provision-v1"

_LAYERS: tuple[CatalogLayer, ...] = ("bronze", "silver", "gold")
_SCHEMA_COMMENTS: dict[CatalogLayer, str] = {
    "bronze": "RailPulse source-aligned data",
    "silver": "RailPulse validated data",
    "gold": "RailPulse decision-support data",
}


class SchemaProvisionError(RuntimeError):
    """Raised when the managed schema boundary cannot be safely reconciled."""


class _QueryResult(Protocol):
    def first(self) -> object | None: ...

    def collect(self) -> list[object]: ...


class _SparkSession(Protocol):
    def sql(self, query: str) -> _QueryResult: ...


@dataclass(frozen=True)
class SchemaProvisionResult:
    """Versioned before/after evidence for one idempotent provisioning run."""

    contract_version: str
    catalog: str
    requested_schemas: tuple[str, ...]
    preexisting_schemas: tuple[str, ...]
    newly_available_schemas: tuple[str, ...]
    available_schemas: tuple[str, ...]


def _ordered_qualified_schemas(namespace: CatalogNamespace) -> tuple[str, ...]:
    return tuple(namespace.schema(layer) for layer in _LAYERS)


def _schema_inventory_query(namespace: CatalogNamespace) -> str:
    names = (namespace.bronze_schema, namespace.silver_schema, namespace.gold_schema)
    literals = ", ".join(f"'{name}'" for name in names)
    return (
        f"SELECT schema_name FROM {namespace.catalog}.information_schema.schemata "
        f"WHERE schema_name IN ({literals}) ORDER BY schema_name"
    )


def _visible_schema_names(
    spark: _SparkSession,
    namespace: CatalogNamespace,
) -> frozenset[str]:
    expected = frozenset((namespace.bronze_schema, namespace.silver_schema, namespace.gold_schema))
    rows = spark.sql(_schema_inventory_query(namespace)).collect()
    visible: set[str] = set()
    for row in rows:
        try:
            value = row["schema_name"]  # type: ignore[index]
        except (KeyError, TypeError) as error:
            raise SchemaProvisionError(
                "Managed runtime returned an incompatible schema inventory row."
            ) from error
        schema_name = str(value).strip() if value is not None else ""
        if schema_name not in expected:
            raise SchemaProvisionError(
                f"Managed runtime returned unexpected schema inventory value {schema_name!r}."
            )
        visible.add(schema_name)
    return frozenset(visible)


def _assert_current_catalog(spark: _SparkSession, namespace: CatalogNamespace) -> None:
    row = spark.sql("SELECT current_catalog() AS current_catalog").first()
    if row is None:
        raise SchemaProvisionError("Managed runtime returned no current catalog row.")
    try:
        value = row["current_catalog"]  # type: ignore[index]
    except (KeyError, TypeError) as error:
        raise SchemaProvisionError(
            "Managed runtime returned an incompatible current catalog row."
        ) from error
    current_catalog = str(value).strip() if value is not None else ""
    if current_catalog.casefold() != namespace.catalog.casefold():
        raise SchemaProvisionError(
            f"Managed runtime catalog {current_catalog!r} does not match configured catalog "
            f"{namespace.catalog!r}."
        )


def provision_databricks_schemas(
    spark: _SparkSession,
    namespace: CatalogNamespace,
) -> SchemaProvisionResult:
    """Create only missing layer schemas and reconcile the visible result."""

    _assert_current_catalog(spark, namespace)
    before = _visible_schema_names(spark, namespace)

    for layer in _LAYERS:
        schema_name = getattr(namespace, f"{layer}_schema")
        if schema_name in before:
            continue
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {namespace.schema(layer)} "
            f"COMMENT '{_SCHEMA_COMMENTS[layer]}'"
        ).collect()

    after = _visible_schema_names(spark, namespace)
    expected = frozenset((namespace.bronze_schema, namespace.silver_schema, namespace.gold_schema))
    missing = expected - after
    if missing:
        raise SchemaProvisionError(
            "Schema provisioning did not make all requested schemas visible: "
            + ", ".join(sorted(missing))
        )

    qualified = {layer: namespace.schema(layer) for layer in _LAYERS}
    requested = _ordered_qualified_schemas(namespace)
    preexisting = tuple(
        qualified[layer] for layer in _LAYERS if getattr(namespace, f"{layer}_schema") in before
    )
    newly_available = tuple(schema for schema in requested if schema not in preexisting)

    return SchemaProvisionResult(
        contract_version=SCHEMA_PROVISION_CONTRACT_VERSION,
        catalog=namespace.catalog,
        requested_schemas=requested,
        preexisting_schemas=preexisting,
        newly_available_schemas=newly_available,
        available_schemas=requested,
    )


def main(
    catalog: str = "workspace",
    bronze_schema: str = "railpulse_bronze",
    silver_schema: str = "railpulse_silver",
    gold_schema: str = "railpulse_gold",
) -> int:
    """Provision the configured schemas inside a managed Spark task."""

    from pyspark.sql import SparkSession

    namespace = CatalogNamespace(
        catalog=catalog,
        bronze_schema=bronze_schema,
        silver_schema=silver_schema,
        gold_schema=gold_schema,
    )
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    result = provision_databricks_schemas(spark, namespace)
    print(json.dumps(asdict(result), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
