from __future__ import annotations

import pytest

from railpulse.catalog import CatalogNamespace, CatalogNamespaceError


def test_catalog_namespace_builds_three_level_names() -> None:
    namespace = CatalogNamespace(
        catalog="workspace",
        bronze_schema="railpulse_bronze",
        silver_schema="railpulse_silver",
        gold_schema="railpulse_gold",
    )

    assert namespace.schema("bronze") == "workspace.railpulse_bronze"
    assert namespace.schema("silver") == "workspace.railpulse_silver"
    assert namespace.table("gold", "feature_snapshots_v2") == (
        "workspace.railpulse_gold.feature_snapshots_v2"
    )
    assert namespace.volume("bronze", "source") == "workspace.railpulse_bronze.source"


@pytest.mark.parametrize(
    "identifier", ["", "Workspace", "railpulse-data", "3railpulse", "gold data"]
)
def test_catalog_namespace_rejects_nonportable_identifiers(identifier: str) -> None:
    with pytest.raises(CatalogNamespaceError, match="lowercase SQL identifier"):
        CatalogNamespace(
            catalog=identifier,
            bronze_schema="railpulse_bronze",
            silver_schema="railpulse_silver",
            gold_schema="railpulse_gold",
        )


def test_catalog_namespace_requires_distinct_layer_schemas() -> None:
    with pytest.raises(CatalogNamespaceError, match="must be distinct"):
        CatalogNamespace(
            catalog="workspace",
            bronze_schema="railpulse_data",
            silver_schema="railpulse_data",
            gold_schema="railpulse_gold",
        )


def test_catalog_namespace_rejects_reserved_information_schema() -> None:
    with pytest.raises(CatalogNamespaceError, match="reserved"):
        CatalogNamespace(
            catalog="workspace",
            bronze_schema="railpulse_bronze",
            silver_schema="railpulse_silver",
            gold_schema="information_schema",
        )


def test_catalog_namespace_validates_table_identifiers() -> None:
    namespace = CatalogNamespace(
        catalog="workspace",
        bronze_schema="railpulse_bronze",
        silver_schema="railpulse_silver",
        gold_schema="railpulse_gold",
    )

    with pytest.raises(CatalogNamespaceError, match="table_name"):
        namespace.table("bronze", "telemetry-raw")

    with pytest.raises(CatalogNamespaceError, match="volume_name"):
        namespace.volume("bronze", "source-files")
