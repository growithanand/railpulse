# Maintenance dashboard contract

## Purpose

The RailPulse dashboard is decision-support and portfolio evidence, not a production alerting
surface. It must show both the strength of the governed data platform and the weakness of the final
motor-current model without implying that maintenance teams should act on its alerts.

The read-only datasets are defined in
[`sql/maintenance_dashboard_queries.sql`](../sql/maintenance_dashboard_queries.sql). Each numbered
statement is an independent Databricks SQL dataset. This checkpoint defines and reviews the queries
only; it does not create a dashboard, SQL warehouse, table, or view.

## Required dashboard sections

### 1. Executive outcome

Display the final status as `NOT_PRODUCTION_READY`, alongside test AUPRC, event detection, missed
events, and false alerts per operating day. The zero-of-one event result must be visually prominent
and must not be replaced by the more favorable validation result.

### 2. Validation versus held-out test

Compare the two frozen evidence rows for AUPRC, event detection, warning lead, false episodes, and
daily alert burden. This panel should make the generalization failure clear: false-alert burden
stayed near 1.18 per day while event detection fell from one of one to zero of one.

### 3. Managed data products and quality

Show the Silver and Gold row counts plus the telemetry validation summary. This demonstrates that
the data platform reconciles its inputs even though the predictive result is weak.

### 4. Compressor operations

Chart daily loaded-cycle volume and observed-duration statistics. Show monthly accepted-failure
counts and total failure duration without record IDs, free-text reports, or model predictions.

## Dataset mapping

| Query | Dashboard use | Data boundary |
| --- | --- | --- |
| 1 | Executive KPI tiles and readiness banner | Frozen aggregate test evidence |
| 2 | Validation/test comparison | Frozen aggregate evidence only |
| 3 | Managed product counts | Table counts; no scores |
| 4 | Data-quality tiles | Silver aggregate quality row |
| 5 | Daily operations trend | Gold cycles; no labels or scores |
| 6 | Monthly failure summary | Aggregated accepted events only |

## Guardrails

- Do not expose row-level probabilities, targets, episode timestamps, coefficients, record IDs, or
  report text.
- Do not query or reconstruct held-out scores. Test metrics are the frozen constants from the single
  approved managed result.
- Label the model `NOT_PRODUCTION_READY` and the result `report-only-no-retuning`.
- Do not add a live alert feed, alert action, maintenance-cost claim, or automated recommendation.
- Do not hide the missed held-out event or present validation metrics as final performance.
- Keep dashboard deployment and SQL-warehouse execution as separately reviewed, cost-bearing steps.

## Acceptance criteria

The dashboard is ready for demonstration only when every query returns the expected schema, the
frozen metrics reconcile with the managed result document, all live counts reconcile with the
governed tables, and the visible narrative states that the current model is not production-ready.
