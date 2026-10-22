# First managed modelling-view v2 run

## Purpose

This document freezes the reviewed baseline, expected output, independent validation, and retry
procedure for the first catalog-backed `gold.modeling_view_v2` execution. Deployment and execution
remain separate checkpoints. The workflow is unscheduled and must be started only after this
contract is reviewed.

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

## Independent post-run validation

After a successful workflow run, execute each statement in
[`managed_modeling_view_v2_first_run_validation.sql`](../sql/managed_modeling_view_v2_first_run_validation.sql)
independently on a stopped-by-default or promptly stopped SQL warehouse. The checks must confirm:

- 15,766 non-null unique keys and exact full-row projections of both governed source tables;
- one fixed modelling, snapshot, feature, eligibility, context, and horizon version;
- one complete feature/failure/observation lineage and matching dataset versions;
- the exact row-status, trainable-label, horizon-status, and exclusion-reason distributions;
- zero contract, lineage, target, row-status, exclusion-order, feature-window, trainable-input, and
  source-projection violations;
- all trainable positive rows reference exact accepted Silver failure events; and
- four accepted failure events, three represented by trainable positive cycles, with 22 positive
  cycles in total.

Also use a read-only Catalog API check to confirm that the target is a Unity Catalog managed Delta
table with the 44 contracted columns. Stop the validation warehouse immediately after the audit.

## Failure and retry policy

If an upstream dependency fails, the modelling task must remain unstarted. If the modelling task
fails, record the task output and run only read-only checks before deciding on a retry. Do not
delete, truncate, or manually edit the target as an immediate reaction.

The target uses one Delta transaction and an immutable `loaded_cycle_id` merge. If a run writes the
table but fails before reporting success, an identical retry must report zero inserts, 15,766
unchanged rows, and 15,766 rows before and after. Any changed existing row, different source
lineage, contract revision, or observation boundary must fail rather than overwrite history and
requires an explicit versioned rebuild decision.

## Execution gate

Preparing this contract does not authorize compute or table creation. Before starting the workflow,
reconfirm its zero-run history, the absent target, workspace availability, and the expected upstream
source counts. The first run and the independent SQL audit require explicit approval.

## Scope boundary

This checkpoint can establish a reproducible, leakage-safe managed modelling population. It does
not fit a model or prove predictive performance, warning lead time, alert usefulness, false-alarm
burden, maintenance impact, or dashboard readiness.
