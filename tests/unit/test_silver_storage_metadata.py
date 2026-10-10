from __future__ import annotations

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from railpulse.catalog import CatalogNamespace
from railpulse.validation.silver_storage import (
    FAILURE_ACCEPTED_TABLE,
    FAILURE_QUARANTINE_TABLE,
    QUALITY_BATCH_ID_FIELD,
    TELEMETRY_ACCEPTED_TABLE,
    TELEMETRY_QUALITY_TABLE,
    TELEMETRY_QUARANTINE_TABLE,
    SilverPersistenceError,
    catalog_silver_key_field,
    catalog_silver_table,
)


def _catalog_namespace() -> CatalogNamespace:
    return CatalogNamespace(
        catalog="workspace",
        bronze_schema="railpulse_bronze",
        silver_schema="railpulse_silver",
        gold_schema="railpulse_gold",
    )


def test_catalog_silver_tables_are_fully_qualified_allowlisted_and_keyed() -> None:
    namespace = _catalog_namespace()
    record_tables = (
        TELEMETRY_ACCEPTED_TABLE,
        TELEMETRY_QUARANTINE_TABLE,
        FAILURE_ACCEPTED_TABLE,
        FAILURE_QUARANTINE_TABLE,
    )

    for table_name in record_tables:
        assert catalog_silver_table(namespace, table_name) == (
            f"workspace.railpulse_silver.{table_name}"
        )
        assert catalog_silver_key_field(table_name) == "record_id"

    assert catalog_silver_table(namespace, TELEMETRY_QUALITY_TABLE) == (
        "workspace.railpulse_silver.telemetry_quality_metrics"
    )
    assert catalog_silver_key_field(TELEMETRY_QUALITY_TABLE) == QUALITY_BATCH_ID_FIELD

    with pytest.raises(SilverPersistenceError, match="Unsupported managed Silver table"):
        catalog_silver_table(namespace, "temporary_copy")
