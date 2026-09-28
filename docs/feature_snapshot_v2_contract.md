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

## Deferred work

The schema, in-memory builder, and immutable Delta writer are tested. A separate increment will add
the full-source build command, materialize the table, and reconcile it independently from the
existing v1 table.
