# Databricks namespace contract

## Status

The three development schemas are declared, validated, provisioned, and remotely reconciled. No
RailPulse table or source file exists in them yet. The managed Bronze source Volume is deployed,
independently verified, and empty.

## Development mapping

RailPulse will reuse the existing `workspace` Unity Catalog catalog and isolate each medallion layer
in a project-specific schema:

| Logical layer | Physical development schema |
| --- | --- |
| Bronze | `workspace.railpulse_bronze` |
| Silver | `workspace.railpulse_silver` |
| Gold | `workspace.railpulse_gold` |

Managed workloads must use fully qualified `catalog.schema.table` names. For example, the physical
name for the logical `gold.feature_snapshots_v2` table will be
`workspace.railpulse_gold.feature_snapshots_v2`.

The bundle exposes `catalog`, `bronze_schema`, `silver_schema`, and `gold_schema` variables so a
later target can override the physical names without changing transformation code. The defaults are
passed to the read-only preflight job, which verifies the current catalog and reports the planned
fully qualified schemas.

## Identifier rules

The package accepts only unquoted, lowercase identifiers containing letters, digits, and
underscores. An identifier cannot begin with a digit. The three layer schemas must be distinct, and
`information_schema` is rejected because Unity Catalog reserves it.

These intentionally narrow rules keep generated identifiers portable and make accidental cross-layer
writes easier to detect. Future table adapters must build names through the shared
`CatalogNamespace` contract rather than concatenate unchecked job parameters.

## Provisioning boundary

The authenticated development workspace exposes the managed `workspace` catalog. Before
provisioning, a catalog API check found only its system-created `default` and `information_schema`
schemas. This established that the reviewed operation would add exactly three persistent schemas and
would require `USE CATALOG` and `CREATE SCHEMA` privileges.

The updated bundle validates successfully. Its deployment changed the existing preflight job and
added or deleted no resources. The version 2 run completed successfully on 2026-10-01 and reported
the active `workspace.default` context plus all three planned schemas. A subsequent catalog API check
confirmed that only `default` and `information_schema` exist, so the preflight created nothing.

The schema-provisioning boundary is now implemented and tested. It inventories the three target
names through `information_schema.schemata`, skips visible schemas, executes fully qualified `CREATE
SCHEMA IF NOT EXISTS` statements only for missing layers, and reconciles the final inventory. A rerun
with all three schemas present executes no creation statements.

The updated bundle returns `Validation OK!`. On 2026-10-02, its reviewed deployment added one
unscheduled schema-provisioning job, updated the existing preflight job's shared wheel reference, and
deleted nothing. The first provisioner run terminated successfully with the
`databricks-schema-provision-v1` contract, no preexisting target schemas, and all three targets newly
available. An independent catalog API check then confirmed the three names and their intended layer
comments. No tables or project data were created.

A second managed run terminated successfully with all three targets reported as preexisting and no
newly available schemas. This verifies remote idempotency as well as the first-run creation path.

The governed source-data landing contract is documented in `docs/databricks_source_landing.md`.
The next platform checkpoint is local and remote checksum reconciliation for the two contracted
source files. Uploading ignored local source data remains a separate, reviewed action from Volume
deployment.

Databricks documents the
[three-level Unity Catalog namespace](https://docs.databricks.com/aws/en/data-governance/unity-catalog/access-control/permissions-concepts)
and the required [schema-creation privileges](https://docs.databricks.com/aws/en/schemas/create-schema).
