# Maintenance dashboard contract

## Purpose

The RailPulse dashboard is decision-support and portfolio evidence, not a production alerting
surface. It must show both the strength of the governed data platform and the weakness of the final
motor-current model without implying that maintenance teams should act on its alerts.

The read-only datasets are defined in
[`sql/maintenance_dashboard_queries.sql`](../sql/maintenance_dashboard_queries.sql). Each numbered
statement is an independent Databricks SQL dataset. The source-controlled AI/BI definition is
[`dashboards/railpulse_maintenance_dashboard.lvdash.json`](../dashboards/railpulse_maintenance_dashboard.lvdash.json).
It creates no table or view.

## Databricks validation and draft state

On 2026-11-05, all six statements completed successfully on the Serverless Starter Warehouse. They
returned, in order, one frozen-evidence row, two comparison rows, eight managed-product counts, one
Silver quality row, 210 daily-cycle rows, and four monthly-failure rows. The warehouse was stopped
immediately after this bounded validation.

The validated definition was then created as one active, unpublished Databricks AI/BI draft. Remote
readback confirmed six datasets, one canvas page, thirteen widgets, no schedule, and no published
version. Draft creation did not rerun the datasets or restart the stopped warehouse. Publication,
sharing, and any scheduled refresh remain separate review points.

The first live render exposed two presentation issues without changing the evidence: the decision
banner fragments lacked explicit Markdown line breaks, and the small test AUPRC rounded to `0` in
its counter. On 2026-11-12, the source-controlled definition and existing draft were updated in
place. The banner now uses a compact level-two heading with explicit line breaks, and the AUPRC
counter uses a plain-number format with up to six decimal places. API readback confirmed both
settings while the draft remained unpublished and unscheduled and the SQL warehouse remained
stopped. Final visual acceptance remains a manual review because the Databricks console reports that
it does not support automated browser control.

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

The unpublished draft satisfies the data and narrative checks: every query returned its expected
schema, the frozen metrics reconcile with the managed result document, the live aggregates read the
governed tables, and the visible narrative states that the current model is not production-ready.
Publishing is intentionally not part of this acceptance checkpoint.
