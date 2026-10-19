# First managed failure-horizon run

## Purpose

This document preserves the reviewed baseline and verified evidence for the first catalog-backed
two-hour failure-horizon execution. Declaration, deployment, and execution were kept as separate
checkpoints. The workflow remained unscheduled and had no run history until the first run was
started manually.

The workflow has two ordered tasks:

1. `refresh_loaded_cycles` invokes the existing managed Gold-cycle workflow, which retains the
   complete Silver and Bronze dependency chain.
2. `process_gold_failure_horizons` runs only after that refresh succeeds, reads managed Gold cycles
   plus accepted Silver telemetry and failure events, and immutably merges the reviewed labels into
   `workspace.railpulse_gold.failure_horizons`.

## Pre-run baseline

Before execution on 2026-10-19, live checks confirmed that the workflow still had no run history and
that the target table did not exist. The already reconciled managed inputs formed this baseline:

| Object | Reviewed state |
| --- | --- |
| Managed Gold cycles | 15,766 rows in `loaded_cycles` |
| Accepted Silver telemetry | 1,516,948 rows |
| Accepted Silver failures | 4 rows |
| Failure-horizon workflow | Deployed, unscheduled, and no run history |
| `failure_horizons` | Did not exist |

The frozen label contract expected one row per loaded cycle and this distribution:

| Horizon status | Expected cycles | Binary target |
| --- | ---: | --- |
| `positive` | 22 | `true` |
| `negative` | 15,676 | `false` |
| `horizon_censored` | 3 | null |
| `inside_failure_interval` | 2 | null |
| `missing_prediction_boundary` | 63 | null |
| **Total** | **15,766** | — |

## Verified first execution

The complete workflow succeeded on 2026-10-19 after being started with:

```powershell
databricks bundle run -t dev gold_failure_horizon_processing
```

The parent workflow took approximately 5 minutes 49 seconds. The upstream loaded-cycle refresh
took approximately 5 minutes, and the new failure-horizon task used approximately 2 seconds of
setup plus 45 seconds of execution.

All nested gates succeeded. Their write evidence showed unchanged inputs and outputs:

- Bronze matched 1,516,948 telemetry rows and four failure-reference rows, inserting zero rows.
- Silver inserted zero rows into accepted telemetry, telemetry quarantine, quality metrics,
  accepted failures, and failure quarantine; all reviewed counts remained unchanged.
- Gold loaded cycles reported 15,766 source and unchanged cycles, zero inserts, zero updates, and
  15,766 target rows before and after. This also closes the previously pending managed loaded-cycle
  idempotency rerun.

The failure-horizon runner emitted:

| Field | Observed value |
| --- | --- |
| Processing contract | `databricks-gold-failure-horizon-processing-v1` |
| Cycle identity contract | `loaded-cycle-v1` |
| Horizon contract | `cycle-failure-horizon-v1` |
| Write contract | `databricks-gold-failure-horizons-write-v1` |
| Horizon | 7,200 seconds |
| Label observation end | `2020-09-01 03:59:50` |
| Source horizons | 15,766 |
| Inserted horizons | 15,766 |
| Unchanged horizons | 0 |
| Target before | 0 |
| Target after | 15,766 |

## Independent post-run checks

The checked-in
[managed failure-horizon validation SQL](../sql/managed_failure_horizon_first_run_validation.sql)
was executed separately from the writer output. It confirmed:

- `failure_horizons` is a Unity Catalog managed Delta table with all 14 contracted columns.
- Total, non-null-ID, and distinct-ID counts all equal 15,766.
- The five status counts exactly match the frozen 22 / 15,676 / 3 / 2 / 63 distribution.
- Every row uses `cycle-failure-horizon-v1`, a 7,200-second horizon, and the single reviewed
  observation boundary.
- All nine table-level key, contract, prediction-boundary, status-semantic, and supported-status
  violation counts are zero.
- All positive labels resolve to exact accepted Silver failure records, with zero unexpected
  references on non-positive rows.
- All `inside_failure_interval` rows match a published failure interval, and no other prediction
  boundary lies inside one without the exclusion status.

The independent checks used the serverless SQL warehouse only for read-only statements. It was
stopped immediately after validation instead of being left for the auto-stop window.

## Unchanged rerun and retry policy

An unchanged second full failure-horizon run should report zero inserted horizons, 15,766 unchanged
horizons, and 15,766 target rows before and after. That idempotency run remains a separate compute
checkpoint because it repeats the full Bronze-to-Gold dependency chain.

Do not delete or manually edit the table as an immediate reaction to a future failure. Record the
failed task and error, run the independent checks, and retry the complete gated workflow only after
the cause is understood. The immutable writer rejects conflicting changes to an existing cycle
label; a different horizon, observation boundary, or labeling rule requires an explicit versioned
rebuild.

## Scope boundary

This checkpoint proves managed materialization and first-write reconciliation of deterministic
two-hour cycle labels for the development dataset. It does not prove failure prediction, model
quality, warning lead time, alert usefulness, maintenance impact, or dashboard readiness.
