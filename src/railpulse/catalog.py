"""Validated Unity Catalog namespace contract for managed RailPulse workloads."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

CatalogLayer = Literal["bronze", "silver", "gold"]

_IDENTIFIER_PATTERN = re.compile(r"^[a-z_][a-z0-9_]*$")
_RESERVED_SCHEMA_NAMES = frozenset({"information_schema"})


class CatalogNamespaceError(ValueError):
    """Raised when a managed catalog namespace is unsafe or ambiguous."""


def _validated_identifier(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER_PATTERN.fullmatch(value):
        raise CatalogNamespaceError(
            f"{field_name} must be a lowercase SQL identifier containing only letters, "
            "digits, and underscores"
        )
    return value


@dataclass(frozen=True)
class CatalogNamespace:
    """Physical three-level namespace used by Databricks data products."""

    catalog: str
    bronze_schema: str
    silver_schema: str
    gold_schema: str

    def __post_init__(self) -> None:
        identifiers = {
            "catalog": self.catalog,
            "bronze_schema": self.bronze_schema,
            "silver_schema": self.silver_schema,
            "gold_schema": self.gold_schema,
        }
        for field_name, value in identifiers.items():
            _validated_identifier(value, field_name=field_name)

        schemas = (self.bronze_schema, self.silver_schema, self.gold_schema)
        if len(set(schemas)) != len(schemas):
            raise CatalogNamespaceError("Bronze, Silver, and Gold schemas must be distinct")
        if any(schema in _RESERVED_SCHEMA_NAMES for schema in schemas):
            raise CatalogNamespaceError("information_schema is reserved by Unity Catalog")

    def schema(self, layer: CatalogLayer) -> str:
        """Return the fully qualified schema for one medallion layer."""

        schema_name = {
            "bronze": self.bronze_schema,
            "silver": self.silver_schema,
            "gold": self.gold_schema,
        }[layer]
        return f"{self.catalog}.{schema_name}"

    def table(self, layer: CatalogLayer, table_name: str) -> str:
        """Return a validated, fully qualified table identifier."""

        validated_table = _validated_identifier(table_name, field_name="table_name")
        return f"{self.schema(layer)}.{validated_table}"
