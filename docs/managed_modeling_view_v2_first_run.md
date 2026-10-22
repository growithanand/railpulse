# First managed modelling-view v2 run

## Purpose

This document preserves the reviewed baseline, expected output, observed first-write evidence,
independent validation plan, and retry procedure for the first catalog-backed
`gold.modeling_view_v2` execution. Deployment, execution, and the separate SQL audit remain
independently reviewable checkpoints.

The workflow has two ordered tasks:

1. `refresh_feature_snapshots_v2` invokes the existing managed feature-snapshot v2 workflow, which
   retains the complete loaded-cycle, Silver, and Bronze dependency chain.
2. `process_gold_modeling_view_v2` runs only after that refresh succeeds. It reads the immutable
   managed feature snapshots and failure horizons plus accepted failure-event lineage, then writes
   through the allowlisted immutable modelling-view boundary.

The failure-horizon workflow is not launched as a second nested dependency chain. Instead, the
runner fails closed unless the existing horizon population, keys, versions, prediction boundaries,
and failure lineage are compatible with the refreshed feature snapshots.

## Pre-run baseline

Read-only checks on 2026-10-22 confirmed:

| Object | Reviewed state |
| --- | --- |
| Modelling-view workflow | Deployed, unscheduled, and zero runs |
| Workflow task graph | Feature-snapshot refresh followed by `ALL_SUCCESS` modelling task |
| Managed feature snapshots v2 | 15,766 immutable rows from the verified first write and rerun |
| Managed failure horizons | 15,766 immutable rows from the verified first write |
| Accepted Silver failures | 4 rows |
| `gold.modeling_view_v2` | Does not exist |

Deployment created no table and started no compute.

## Frozen first-write expectation

The first managed build must reproduce the verified complete-source version 2 profile:

| Measure | Expected value |
| --- | ---: |
| Source modelling rows | 15,766 |
| Inserted modelling rows | 15,766 |
| Unchanged modelling rows | 0 |
| Target rows before | 0 |
| Target rows after | 15,766 |
| Trainable rows | 15,272 |
| Excluded rows | 494 |
| Trainable positive labels | 22 |
| Trainable negative labels | 15,250 |
| Accepted failure events | 4 |
| Failure events represented by trainable positive cycles | 3 |

The earliest expected trainable prediction is `2020-02-01 00:58:58`; the latest is
`2020-08-31 20:00:58`.

Exclusion reasons can overlap and therefore do not sum to the excluded-row count:

| Exclusion reason | Expected rows |
| --- | ---: |
| `feature_ineligible` | 348 |
| `cycle_context_unavailable` | 391 |
| `horizon_censored` | 3 |
| `inside_failure_interval` | 2 |
| `missing_prediction_boundary` | 63 |
| `unsupported_horizon_status` | 0 |

The complete horizon-status population must remain 22 positive, 15,676 negative, three
horizon-censored, two inside a published failure interval, and 63 missing a prediction boundary.

## Verified first execution

The approved workflow completed successfully on 2026-10-22. Its total duration was approximately
8 minutes 53 seconds. The upstream feature-snapshot refresh completed first in approximately
7 minutes 48 seconds; the gated modelling task then completed in approximately 1 minute 3 seconds,
including setup and cleanup.

The runner reported the fixed modelling, feature-snapshot, and failure-horizon versions and exactly
matched every frozen first-write expectation:

- 15,766 source rows were inserted into an empty target, leaving 15,766 target rows;
- 15,272 rows were trainable and 494 were excluded;
- trainable rows contained 22 positive and 15,250 negative labels;
- four accepted failure events were present and three were represented by trainable positive cycles;
- represented failure events contributed 4, 5, and 13 positive cycles, while one event contributed
  none; and
- all six overlapping exclusion-reason counts matched the reviewed profile.

A read-only Catalog API check confirmed that the result is a Unity Catalog managed Delta table with
all 44 contracted columns. Its automatic statistics reported 15,766 rows, and the cycle key had no
null values. This metadata and the task output are first-write evidence; they do not replace the
independent SQL audit below.

## Independent post-run validation

On 2026-10-22, each statement in
[`managed_modeling_view_v2_first_run_validation.sql`](../sql/managed_modeling_view_v2_first_run_validation.sql)
was executed independently on the existing validation SQL warehouse. All six statements succeeded
without truncated results. They confirmed:

- 15,766 non-null unique keys and exact full-row projections of both governed source tables;
- one fixed modelling, snapshot, feature, eligibility, context, and horizon version;
- one complete feature/failure/observation lineage and matching dataset versions;
- the exact row-status, trainable-label, horizon-status, and exclusion-reason distributions;
- zero contract, lineage, target, row-status, exclusion-order, feature-window, trainable-input, and
  source-projection violations;
- all trainable positive rows reference exact accepted Silver failure events; and
- four accepted failure events, three represented by trainable positive cycles, with 22 positive
  cycles in total.

The source-projection checks found zero missing or unexpected feature and horizon rows. All ten
contract, source, target, row-state, exclusion-order, feature-window, trainable-input, and
leakage-boundary violation counts were zero. The three failure-lineage and reference violation
counts were also zero. Event-level output reconciled four accepted failure events, three represented
events, and 22 trainable positive cycles with per-event counts of 0, 4, 5, and 13.

The validation warehouse was stopped immediately after the audit and independently reported zero
active clusters. The separate read-only Catalog API check had already confirmed that the target is
a Unity Catalog managed Delta table with the 44 contracted columns.

## Failure and retry policy

If an upstream dependency fails, the modelling task must remain unstarted. If the modelling task
fails, record the task output and run only read-only checks before deciding on a retry. Do not
delete, truncate, or manually edit the target as an immediate reaction.

The target uses one Delta transaction and an immutable `loaded_cycle_id` merge. If a run writes the
table but fails before reporting success, an identical retry must report zero inserts, 15,766
unchanged rows, and 15,766 rows before and after. Any changed existing row, different source
lineage, contract revision, or observation boundary must fail rather than overwrite history and
requires an explicit versioned rebuild decision.

## Unchanged-rerun gate

The first managed write and independent SQL audit are complete. They do not authorize an
idempotency rerun. Before starting the workflow again, obtain explicit compute approval and
reconfirm that its immutable merge should report zero inserts, 15,766 unchanged rows, and 15,766
rows before and after.

## Scope boundary

This checkpoint establishes runner, Catalog, and independent SQL evidence for a reproducible,
leakage-safe managed modelling population. It does not fit a model or prove predictive performance,
warning lead time, alert usefulness, false-alarm burden, maintenance impact, or dashboard
readiness.
