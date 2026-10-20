# Chronological modelling-view version 2 contract

## Purpose

`motor-current-cycle-context-failure-modeling-view-v2` is the reviewed bridge from the immutable
version 2 feature snapshot to a future expanded model. It preserves the original motor-current
inputs and adds three cycle-duration values that are known at the prediction boundary.

This is a separate logical contract, not a rewrite of `gold.modeling_view`. The existing version 1
view and its results remain reproducible.

## Table and inputs

| Property | Value |
| --- | --- |
| Logical name | `gold.modeling_view_v2` |
| Primary key | `loaded_cycle_id` |
| Feature source | `gold.feature_snapshots_v2` |
| Target source | `cycle-failure-horizon-v1` |
| Chronological split | `calendar-chronological-split-selection-v1` |

The version 2 model-input columns retain all five version 1 inputs and add:

| Column | Meaning |
| --- | --- |
| `cycle_context_current_duration_seconds` | Duration of the completed current loaded cycle |
| `cycle_context_previous_duration_seconds` | Duration of the preceding complete loaded cycle |
| `cycle_context_previous_idle_seconds` | Time between the preceding stop and current start |

`cycle_context_previous_cycle_id`, `cycle_context_status`, and
`cycle_context_feature_version` remain auditable metadata. They are not numerical model inputs.
Failure labels, prediction timestamps, lineage, eligibility, exclusion reasons, and identifiers are
also excluded from the model-input list.

## Row eligibility

A version 2 row can be trainable only when:

1. the existing motor-current eligibility status is `eligible`;
2. `cycle_context_status` is `available`, so all three added numerical inputs are present; and
3. the failure-horizon status is `positive` or `negative`.

Rows with unavailable cycle context remain in the view as excluded evidence with the reason
`cycle_context_unavailable`. They are not silently imputed, dropped, or converted into negative
examples. Other exclusion reasons retain their version 1 ordering.

## Leakage and test-isolation rules

- Every added input is computed from boundaries observed no later than the prediction timestamp.
- Failure information remains target and evaluation metadata only.
- The selected June-validation/July-test calendar split remains unchanged.
- July test rows remain sealed while the expanded candidate and its thresholds are designed.
- Any later preprocessing parameters must be fitted on training rows only.

## Implemented builder

The Spark builder reuses the verified version 1 key, horizon, lineage, and prediction-boundary
guards, then adds version 2 snapshot and cycle-context validation. It selects the fixed v2 column
order, changes a row to trainable only when context is available, and emits exclusion reasons in the
contracted order. Inputs are never modified, and extra columns are not carried into the view.

The [read-only full-source profile](modeling_view_v2_profile.md) rebuilds the verified two-hour
horizons, joins the immutable version 2 snapshot, reconciles every row and exclusion reason, and
records the effect of requiring complete cycle context without writing a table.

## Managed persistence boundary

`persist_catalog_modeling_view_v2` writes only to the allowlisted `gold.modeling_view_v2` table. It
requires the fixed feature, eligibility, context, horizon, and modelling-view versions; one complete
feature lineage; one complete failure lineage; and one label-observation boundary. It also verifies
the dataset relationship, supported statuses, target semantics, ordered exclusion reasons, and
feature-window alignment before writing.

The managed table is immutable by `loaded_cycle_id`. New keys may be inserted, an identical rerun
is reconciled as unchanged, and any changed existing row or mixed source identity is rejected. The
writer compares logical Spark types, reconciles source/inserted/unchanged/before/after counts, and
does not cache DataFrames in the serverless runtime. Focused Spark/Delta tests cover its first write,
zero-insert rerun, lineage isolation, immutable-row conflict, invalid-state rejection, and boundary
checks.

## Managed processing runner

`databricks_gold_modeling_view_v2_processing` reads the allowlisted managed
`gold.feature_snapshots_v2` and `gold.failure_horizons` tables. It also reads
`silver.failure_events_accepted` because the horizon table deliberately stores label outcomes but
not the failure file's source identity. The runner requires one complete feature lineage and one
complete failure-event lineage with matching dataset versions before building the view.

Before writing, it profiles row statuses, ordered exclusion reasons, trainable labels, prediction
time coverage, and positive-cycle references to accepted failure events. Its structured result
records the three physical sources, both lineages, accepted and represented failure-event counts,
the reconciled population summary, and immutable-write counts. Unit tests cover orchestration and
failure paths, while a focused managed-style Spark/Delta test verifies a first write and zero-insert
rerun through catalog tables.

## Managed workflow boundary

The validated, unscheduled `gold_modeling_view_v2_processing` bundle job first invokes the existing
managed feature-snapshot v2 workflow. Only after that workflow succeeds does the modelling-view task
invoke `databricks_gold_modeling_view_v2_processing` on serverless environment version 4. Queueing
is enabled, concurrent runs are limited to one, and the downstream task uses an `ALL_SUCCESS` gate.

The already materialized immutable `gold.failure_horizons` table is read rather than rebuilt by a
second nested upstream chain. The runner's one-to-one population, version, boundary, and lineage
checks fail closed if that label table is no longer compatible with the refreshed snapshots. This
avoids duplicating the complete Bronze-to-Gold refresh within one modelling-view run while keeping
stale or mismatched inputs from being written.

Strict bundle validation passes. The reviewed resource-scoped plan reports one job to create, four
transitive upstream jobs to update only with the newly built shared wheel, zero resources to delete,
and five resources outside the selection. No task graph, parameters, concurrency setting, or
schedule on an existing job would change. The plan is read-only: no resource was deployed and no
compute was started.

## Scope boundary

The fixed schema, Spark builder, focused fixture tests, full-source population profile, managed
persistence boundary, packaged processing runner, and validated workflow definition are
implemented. Workflow deployment, first managed write, model fitting, threshold selection, and
held-out test evaluation remain separate reviewable increments. No model-performance claim is made.
