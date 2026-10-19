# First managed Gold loaded-cycle run

## Purpose

This document preserves the review contract and verified evidence for the first catalog-backed Gold
execution. Deployment and execution were treated as separate approvals: the workflow was first
deployed without a run, then started manually after review.

The workflow has two ordered tasks:

1. `refresh_silver` invokes the existing managed Silver workflow, which retains its own Bronze gate.
2. `process_gold_loaded_cycles` runs only after Silver succeeds, reads
   `workspace.railpulse_silver.telemetry_accepted`, derives the existing `loaded-cycle-v1` snapshot,
   and merges it into `workspace.railpulse_gold.loaded_cycles`.

## Scoped deployment-plan review

The following read-only plan was generated on 2026-10-16:

```powershell
databricks bundle plan -t dev --select jobs.gold_cycle_processing
```

It reported one resource to create, two to update, zero to delete, zero unchanged selected resources,
and four resources outside the selection. The new resource is the Gold workflow. The two upstream
updates are the existing Bronze and Silver jobs, which move from their previously deployed wheel to
the newly built wheel containing the Gold entry point. Their task structure, dependencies,
parameters, concurrency, and queue settings remain unchanged. A deployment must be stopped and
reviewed again if a fresh plan includes a deletion or any additional behavioral change.

## Pre-run baseline

A read-only Unity Catalog inventory on 2026-10-16 established:

| Object | Reviewed state |
| --- | --- |
| `workspace.railpulse_silver.telemetry_accepted` | 1,516,948 rows; IDs previously verified non-null and unique |
| `workspace.railpulse_gold` | Empty; no managed Gold tables |
| Gold workflow | Deployed and unscheduled; no run history |

The managed Silver checkpoint also records zero telemetry quarantine rows, one quality row with 354
material forward gaps, four accepted failure events, and zero failure quarantine rows. Starting the
Gold workflow will rerun the complete Bronze-to-Silver dependency chain. With unchanged sources and
contracts, those deterministic merges should insert no new upstream rows.

## Expected first-run output

Only one new table is allowed:

| Managed Gold table | Merge key | Expected rows |
| --- | --- | ---: |
| `workspace.railpulse_gold.loaded_cycles` | `loaded_cycle_id` | 15,766 |

The runner and writer evidence should report:

| Field | Expected value |
| --- | --- |
| Processing contract | `databricks-gold-cycle-processing-v1` |
| Cycle identity contract | `loaded-cycle-v1` |
| Write contract | `databricks-gold-loaded-cycles-write-v1` |
| Source table | `workspace.railpulse_silver.telemetry_accepted` |
| Source cycles | 15,766 |
| Inserted cycles | 15,766 |
| Updated cycles | 0 |
| Unchanged cycles | 0 |
| Target before | 0 |
| Target after | 15,766 |

The expected descriptive reconciliation is inherited from the verified complete-source local Gold
snapshot:

| Visible start | Right-censored | Cycles | Loaded observations | Durations |
| --- | --- | ---: | ---: | ---: |
| `left_censored` | No | 167 | 14,338 | 167 |
| `left_censored` | Yes | 15 | 15,172 | 0 |
| `observed` | No | 15,536 | 188,052 | 15,536 |
| `observed` | Yes | 48 | 26,076 | 0 |

These groups must reconcile to 15,766 cycles, 243,638 loaded observations, 15,703 durations, and 63
right-censored cycles. They describe visible operating boundaries, not equipment health or failure.

## Verified deployment

The reviewed selection was deployed on 2026-10-16. Databricks reported one Gold job created, the
existing Bronze and Silver jobs updated to the new wheel, zero resource deletions, 27 files uploaded,
and zero file deletions. Independent live readback confirmed:

- the workflow remains bundle-managed, UI-locked, unscheduled, queue-enabled, and limited to one
  concurrent run;
- `refresh_silver` invokes the expected managed Silver job;
- `process_gold_loaded_cycles` depends on that task with `ALL_SUCCESS`, uses serverless environment
  version 4, invokes `databricks_gold_cycle_processing`, and receives the four reviewed namespace
  parameters;
- the workflow has no run history;
- `workspace.railpulse_gold` still contains no tables.

Deployment therefore changed configuration and uploaded artifacts only. It did not consume compute
or materialize Gold data.

## Controlled deployment and execution

The first deployment used the checked-in bundle command below after an explicitly approved fresh
plan:

```powershell
databricks bundle deploy -t dev --select jobs.gold_cycle_processing
```

The selection includes the Gold job and its referenced Bronze/Silver dependencies. Future
deployments must confirm again that the only upstream change is the wheel dependency. Deployment
does not start a run because the workflow has no schedule or trigger.

After deployment is independently verified and execution is separately approved, run the complete
gated workflow:

```powershell
databricks bundle run -t dev gold_cycle_processing
```

Do not bypass `refresh_silver` or invoke the wheel entry point directly for the first-run checkpoint.

## Verified first execution

The complete gated workflow succeeded on 2026-10-16. It took approximately 9 minutes 26 seconds:
the nested Bronze and Silver refresh completed successfully, followed by approximately 49 seconds
for Gold task setup and execution. The unchanged upstream inputs produced zero new Bronze or Silver
rows. Silver remained reconciled at 1,516,948 accepted telemetry rows, zero telemetry quarantine
rows, one quality row, four accepted failure events, and zero failure quarantine rows.

The Gold runner emitted the reviewed contract identities and exact first-write reconciliation:

| Field | Observed value |
| --- | --- |
| Processing contract | `databricks-gold-cycle-processing-v1` |
| Cycle identity contract | `loaded-cycle-v1` |
| Write contract | `databricks-gold-loaded-cycles-write-v1` |
| Source table | `workspace.railpulse_silver.telemetry_accepted` |
| Source cycles | 15,766 |
| Inserted cycles | 15,766 |
| Updated cycles | 0 |
| Unchanged cycles | 0 |
| Target before | 0 |
| Target after | 15,766 |

## Independent post-run checks

[The managed Gold validation SQL](../sql/managed_gold_first_run_validation.sql) was executed
separately from the job output. It confirmed:

- The Gold parent workflow and both tasks succeeded, including the nested Silver and Bronze runs.
- The Gold schema contains exactly `loaded_cycles` and no unexpected table.
- The table is managed Delta storage with all nine contracted columns.
- Total, non-null ID, and unique ID counts all equal 15,766.
- The table contains 243,638 loaded observations, 15,703 observed durations, and 63 right-censored
  cycles.
- The four start/censoring groups match the reviewed counts: 167 left-censored settled cycles, 15
  left-censored right-censored cycles, 15,536 observed settled cycles, and 48 observed
  right-censored cycles.
- All seven structural-invariant violation counts are zero.
- The emitted runner evidence reports the three expected contract versions and first-write counts.

Spark reported that the current global ordered window moved data to one partition. This is the
known single-compressor implementation described in the Gold limitations, not a correctness
failure; the Gold execution itself completed in approximately 42 seconds. A future multi-asset
version must introduce an equipment key and partition the ordering by asset.

## Verified unchanged rerun and retry policy

The managed loaded-cycle task ran again on 2026-10-19 as the required upstream dependency of the
approved first failure-horizon workflow. It reported zero inserted cycles, zero updated cycles,
15,766 unchanged cycles, and 15,766 target rows before and after. This closes the managed
loaded-cycle idempotency checkpoint without requiring a standalone duplicate run.

The Gold writer performs one Delta merge, but a job may still fail before the merge, after the merge,
or during final reconciliation. Do not delete or manually edit the table as an immediate reaction.
Record the failed task and error, run the independent checks, confirm that managed Silver still has
1,516,948 accepted telemetry rows, and retry the complete gated workflow only after the cause is
understood. The monotonic writer permits new cycles and extensions or closure of right-censored
cycles; it rejects changes to stable start evidence, observation-count regression, and reopening a
settled cycle.

## Scope boundary

This checkpoint proves managed Bronze-to-Gold loaded-cycle materialization, first-write
reconciliation, and an unchanged full-chain rerun for the development dataset. It does not prove
failure prediction, model quality, warning lead time, maintenance impact, alert usefulness, or
dashboard readiness.
