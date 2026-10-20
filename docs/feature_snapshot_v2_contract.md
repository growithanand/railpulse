# Gold feature-snapshot version 2 contract

## Purpose

`motor-current-15m-cycle-context-snapshot-v2` extends the verified motor-current snapshot with the
past-only cycle operating context that passed source-coverage and development-only checks. It is a
new contract and logical table; it does not alter or replace version 1 rows.

## Table

| Property | Value |
| --- | --- |
| Logical name | `gold.feature_snapshots_v2` |
| Primary key | `loaded_cycle_id` |
| Snapshot version | `motor-current-15m-cycle-context-snapshot-v2` |
| Format | Delta Lake |

Keeping version 2 in a separate table preserves the reproducibility and insert-only guarantees of
`gold.feature_snapshots`. Consumers must opt into the expanded contract explicitly.

## Column groups

Version 2 retains every version 1 motor-current feature and eligibility column, then adds:

| Column | Type | Meaning |
| --- | --- | --- |
| `cycle_context_feature_version` | `string` | Fixed operating-context contract version |
| `cycle_context_status` | `string` | Explicit completeness or censoring result |
| `cycle_context_current_duration_seconds` | `long` | Complete current loaded duration when available |
| `cycle_context_previous_cycle_id` | `string` | Auditable predecessor identity |
| `cycle_context_previous_duration_seconds` | `long` | Complete predecessor loaded duration when available |
| `cycle_context_previous_idle_seconds` | `long` | Non-negative time between predecessor stop and current start |

The snapshot retains the existing dataset, telemetry-source, and ingestion-batch lineage columns.
`feature_snapshot_version` changes to the version 2 identifier. Fixed column order is defined by
`FEATURE_SNAPSHOT_V2_COLUMNS`.

## Construction invariants

- Version 1 feature rows and cycle-context rows must each have unique, non-null cycle IDs.
- Their populations and keys must join one-to-one.
- Motor-current and eligibility versions must still satisfy the version 1 contract.
- Cycle context must use `loaded-cycle-operating-context-v1` and a supported explicit status.
- Complete context rows must contain all three duration values and the predecessor ID.
- Durations must be positive where present; idle intervals must be non-negative.
- Extra input columns, including failure-horizon labels, are discarded.

## Label and version isolation

The table contains information available at the cycle prediction boundary. It does not contain
failure labels, chronological partitions, model scores, or test outcomes. Snapshot v1 remains
unchanged, and the held-out test period remains unavailable during v2 construction.

## Persistence guarantees

`persist_feature_snapshots_v2` writes only to the separate version 2 table. It validates required
columns, versions, unique keys, lineage, and cycle-context values before every write. It inserts
unseen cycle IDs, treats an identical rerun as unchanged, and rejects any attempt to modify an
existing row. Source, inserted, unchanged, before, and after counts are reconciled after each Delta
operation.

`persist_catalog_feature_snapshots_v2` applies the same immutable contract to the allowlisted
managed `gold.feature_snapshots_v2` table. The managed writer additionally requires one source
lineage, refuses to mix that lineage with an existing target, and uses table-backed Delta operations
without caching DataFrames so it remains compatible with the verified serverless runtime. Its
result records the storage-contract version, fully qualified table, key field, and reconciliation
counts.

## Deferred work

The schema, in-memory builder, local Delta writer, managed catalog persistence boundary, managed
Silver/Gold-to-Gold construction runner, and
[full-source build](feature_snapshot_v2_build.md) are tested. The local table contains 15,766
reconciled rows, and an identical rerun inserts zero rows while the existing version 1 table remains
unchanged. The Databricks workflow is deployed, and its first managed write and independent SQL
validation are complete. Its managed unchanged rerun also inserts zero rows, reports all 15,766
source snapshots unchanged, and preserves the 15,766-row target.

## Managed workflow boundary

The validated, unscheduled `gold_feature_snapshot_v2_processing` bundle job first invokes the
existing managed loaded-cycle workflow. That upstream job already refreshes Bronze and Silver
before reconciling `gold.loaded_cycles`. Only after the upstream job succeeds does the feature task
invoke `databricks_gold_feature_snapshot_v2_processing` on serverless environment version 4. This
keeps the dependency graph explicit without running the Silver workflow twice. The workflow has
queueing enabled and allows only one concurrent run. Bundle validation passes. Immediately after
deployment, a read-only run-history check confirmed that deployment itself had not started compute.

The reviewed resource-scoped bundle plan reported one job to create, three upstream jobs to update,
zero resources to delete, and five unselected resources. Deployment applied that exact scope: it
created the feature workflow, updated the transitive Bronze, Silver, and loaded-cycle workflows,
deleted nothing, and uploaded the shared wheel. No workflow was started.

The subsequent approved first run completed successfully and inserted 15,766 snapshots into an
empty managed Delta target. Independent SQL reconciled every key with managed loaded cycles,
reproduced all feature, eligibility, reason, and cycle-context counts, confirmed one fixed contract
and source lineage, found zero semantic or boundary violations, and found no failure, label, or
target columns. The detailed evidence is preserved in the
[first managed feature-snapshot v2 run record](managed_feature_snapshot_v2_first_run.md).

The subsequent identical full-workflow rerun completed successfully. It refreshed the governed
upstream dependency chain, read the same 1,516,948 accepted telemetry rows and 15,766 loaded cycles,
inserted zero snapshots, matched all 15,766 source snapshots as unchanged, and retained 15,766
target rows. This closes managed feature-snapshot idempotency without modifying the immutable
contract or starting separate validation compute.
