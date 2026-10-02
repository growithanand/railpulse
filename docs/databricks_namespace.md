# Databricks namespace contract

## Status

The namespace is declared and validated in code but is not provisioned. This boundary does not
create schemas, tables, volumes, or grants.

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

The authenticated development workspace exposes the managed `workspace` catalog. At this checkpoint,
only its system-created `default` and `information_schema` schemas exist. Creating the three RailPulse
schemas is a separate reviewed operation because it changes persistent workspace state and requires
`USE CATALOG` and `CREATE SCHEMA` privileges.

The updated bundle validates successfully. Its deployment changed the existing preflight job and
added or deleted no resources. The version 2 run completed successfully on 2026-10-01 and reported
the active `workspace.default` context plus all three planned schemas. A subsequent catalog API check
confirmed that only `default` and `information_schema` exist, so the preflight created nothing.

The schema-provisioning boundary is now implemented and tested. It inventories the three target
names through `information_schema.schemata`, skips visible schemas, executes fully qualified `CREATE
SCHEMA IF NOT EXISTS` statements only for missing layers, and reconciles the final inventory. A rerun
with all three schemas present executes no creation statements.

The updated bundle returns `Validation OK!`. Its plan adds one unscheduled schema-provisioning job,
updates the existing preflight job's shared wheel reference, and deletes nothing. That plan has not
been applied, and the provisioner has not been run. The first catalog-backed table write remains out
of scope until the three schemas are provisioned and independently verified.

Databricks documents the
[three-level Unity Catalog namespace](https://docs.databricks.com/aws/en/data-governance/unity-catalog/access-control/permissions-concepts)
and the required [schema-creation privileges](https://docs.databricks.com/aws/en/schemas/create-schema).
