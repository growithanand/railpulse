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

## Scope boundary

The fixed schema, Spark builder, focused fixture tests, and full-source population profile are
implemented. Model fitting, threshold selection, held-out test evaluation, persistence, and
Databricks deployment are separate reviewable increments. No model-performance claim is made.
