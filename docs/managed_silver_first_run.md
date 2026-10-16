# First managed Silver run

## Purpose

This document preserves the review contract and observed evidence for the first catalog-backed
Silver execution. The run remained separate from deployment: the workflow was deployed and
unscheduled, and it was started only after the baseline and expected outputs below were reviewed.

The workflow has two ordered tasks:

1. `refresh_bronze` invokes the existing managed Bronze workflow.
2. `process_silver_tables` runs only after Bronze succeeds and validates both Bronze tables before
   writing any Silver output.

## Pre-run baseline

The following read-only inventory was refreshed on 2026-10-13 against the development target:

| Object | Observed state |
| --- | --- |
| `workspace.railpulse_bronze.telemetry_raw` | Managed Delta table, 25 columns, 1,516,948 rows |
| `workspace.railpulse_bronze.failure_reports_raw` | Managed Delta table, 17 columns, 4 rows |
| `workspace.railpulse_silver` | Empty; no managed Silver tables |
| Deployed Silver workflow | No run history |

The current Bronze counts agree with the successful first load and zero-insert rerun already
recorded in the project status. Starting Silver will first invoke Bronze again; because the source
files and deterministic record identifiers have not changed, that refresh is expected to match all
existing rows and insert zero.

## Blocked execution attempt

The reviewed first run was approved and attempted on 2026-10-15. Databricks rejected the request
before creating a run with the message that triggering new organization runs is temporarily
disabled. A separate post-attempt inventory confirmed both safety conditions:

- the deployed Silver workflow still has no run history;
- `workspace.railpulse_silver` still contains no tables.

No task started, the Bronze workflow was not invoked, and no managed table changed. This is an
account-level compute availability gate, not a failure inside either RailPulse task. Databricks
[documents](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations) that Free
Edition compute can become unavailable after quota use while existing data and settings remain
preserved. Retry the same full workflow only after new runs are enabled again; do not bypass the
Bronze dependency or change the five expected outputs in response to this gate.

## Expected and observed first-run outputs

The expected values below come from the complete-source local validation using
`telemetry-validation-v2`. The successful managed run on 2026-10-16 matched every expected table
count.

| Managed Silver table | Merge key | Expected rows | Observed rows |
| --- | --- | ---: | ---: |
| `workspace.railpulse_silver.telemetry_accepted` | `record_id` | 1,516,948 | 1,516,948 |
| `workspace.railpulse_silver.telemetry_quarantine` | `record_id` | 0 | 0 |
| `workspace.railpulse_silver.telemetry_quality_metrics` | `quality_batch_id` | 1 | 1 |
| `workspace.railpulse_silver.failure_events_accepted` | `record_id` | 4 | 4 |
| `workspace.railpulse_silver.failure_events_quarantine` | `record_id` | 0 | 0 |

The single telemetry metrics row also matched the reviewed values:

| Metric | Expected value | Observed value |
| --- | ---: | ---: |
| Validation version | `telemetry-validation-v2` | `telemetry-validation-v2` |
| Total telemetry records | 1,516,948 | 1,516,948 |
| Accepted telemetry records | 1,516,948 | 1,516,948 |
| Quarantined telemetry records | 0 | 0 |
| Material forward gaps | 354 | 354 |
| Rejection reasons | Empty | Empty |

Both reconciliations must hold:

```text
telemetry accepted + telemetry quarantined = 1,516,948
failure accepted + failure quarantined = 4
```

An empty quarantine result is still represented by a real Delta table with the contracted schema.
It must not be interpreted as a missing output.

## Verified execution on 2026-10-16

After compute access resumed, the unchanged deployed workflow completed its first successful run.
A separate read-only Unity Catalog inventory showed exactly the five reviewed Silver tables.
Databricks SQL checks then established:

- all five table counts matched the reviewed acceptance criteria above;
- the telemetry quality row reconciled accepted plus quarantined records to its total;
- the quality row reported 354 material forward gaps and no rejection reasons;
- accepted telemetry returned 1,516,948 total, non-null, and distinct `record_id` values;
- accepted failure events returned four total, non-null, and distinct `record_id` values.

These observations close the managed Silver first-run checkpoint. They do not make a predictive
model, failure-detection, warning-lead-time, maintenance-impact, or dashboard-readiness claim.

## Controlled execution

For a future explicitly approved rerun, invoke the deployed bundle resource from the repository
root:

```powershell
databricks bundle run -t dev silver_processing
```

The command waits for completion by default. Do not bypass the Bronze task or run the packaged
Silver entry point independently for this checkpoint; the upstream gate is part of the evidence.

## Independent post-run checks

The command output is useful evidence, but it is not the final verification. A separate read-only
inspection must confirm all of the following:

- The parent workflow and both tasks finish successfully.
- The nested Bronze run succeeds, matches 1,516,948 telemetry and four failure records, and inserts
  zero rows.
- Exactly the five allowlisted Silver tables exist; no unexpected table is created.
- Every table uses Delta format and the expected merge key is present and non-null.
- Table counts match the expected first-run outputs above.
- The telemetry metrics row uses `telemetry-validation-v2`, has a stable non-null
  `quality_batch_id`, reports 354 forward gaps, and reconciles accepted plus quarantined records to
  its total.
- Failure accepted plus quarantine counts reconcile to the four Bronze failure records.
- The emitted runner evidence names the two expected Bronze sources and reports first-run inserts
  equal to the new target counts.

The observed remote results from 2026-10-16 are recorded above and in the project status.

## Failure and retry policy

Silver writes are five independent Delta transactions rather than one transaction spanning all
tables. A run can therefore fail after creating or populating only some outputs. Do not delete or
manually edit those tables as an immediate reaction.

The writers use deterministic insert-only keys, so an unchanged retry should match previously
written rows and insert only outputs that were not completed. Before retrying:

1. Record the failed task and error.
2. Inventory all five Silver tables and their counts.
3. Confirm that Bronze counts and source identities are unchanged.
4. Fix the reviewed cause in source when needed, deploy that committed change, and rerun the full
   gated workflow.

If validation semantics change, use an explicit versioned rebuild plan instead of silently moving
an existing `record_id` between accepted and quarantine tables.

## Scope boundary

Passing this checkpoint proves managed Bronze-to-Silver validation, persistence, orchestration, and
retry-safe reconciliation for the development dataset. It does not yet prove managed Gold
materialization, model quality, failure prediction, maintenance impact, alert usefulness, or
dashboard readiness.
