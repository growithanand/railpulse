from __future__ import annotations

import pytest

from railpulse.catalog import CatalogNamespace, CatalogNamespaceError
from railpulse.landing import (
    FAILURE_REFERENCE_FILENAME,
    SOURCE_LANDING_CONTRACT_VERSION,
    TELEMETRY_SOURCE_FILENAME,
    DatabricksSourceLanding,
    SourceLandingError,
)

NAMESPACE = CatalogNamespace(
    catalog="workspace",
    bronze_schema="railpulse_bronze",
    silver_schema="railpulse_silver",
    gold_schema="railpulse_gold",
)


def test_source_landing_builds_versioned_volume_paths() -> None:
    landing = DatabricksSourceLanding(
        namespace=NAMESPACE,
        dataset_version="uci-791-aab991a970e5",
    )

    assert landing.contract_version == SOURCE_LANDING_CONTRACT_VERSION
    assert landing.qualified_volume == "workspace.railpulse_bronze.source"
    assert landing.volume_root == "/Volumes/workspace/railpulse_bronze/source"
    assert landing.dataset_root == (
        "/Volumes/workspace/railpulse_bronze/source/metropt3/uci-791-aab991a970e5"
    )
    assert landing.telemetry_path == f"{landing.dataset_root}/telemetry/{TELEMETRY_SOURCE_FILENAME}"
    assert landing.failure_reference_path == (
        f"{landing.dataset_root}/reference/{FAILURE_REFERENCE_FILENAME}"
    )


def test_source_landing_builds_cli_uri_without_changing_the_volume_path() -> None:
    landing = DatabricksSourceLanding(
        namespace=NAMESPACE,
        dataset_version="uci-791-aab991a970e5",
    )

    assert landing.cli_uri(landing.telemetry_path) == f"dbfs:{landing.telemetry_path}"
    with pytest.raises(SourceLandingError, match="configured Volume root"):
        landing.cli_uri("data/raw/telemetry.csv")
    with pytest.raises(SourceLandingError, match="configured Volume root"):
        landing.cli_uri("/Volumes/workspace/railpulse_bronze/other/file.csv")


@pytest.mark.parametrize(
    "dataset_version",
    ["", "UCI-791", "uci 791", "../uci-791", "uci-791/second", "uci--791"],
)
def test_source_landing_rejects_unsafe_dataset_versions(dataset_version: str) -> None:
    with pytest.raises(SourceLandingError, match="dataset_version"):
        DatabricksSourceLanding(namespace=NAMESPACE, dataset_version=dataset_version)


def test_source_landing_reuses_catalog_identifier_rules_for_volume_name() -> None:
    with pytest.raises(CatalogNamespaceError, match="volume_name"):
        DatabricksSourceLanding(
            namespace=NAMESPACE,
            dataset_version="uci-791-aab991a970e5",
            volume_name="source-files",
        )
