# Portfolio demonstration guide

## Purpose

This guide presents RailPulse as an end-to-end data and machine-learning engineering case study. It
is designed for a five-to-eight-minute portfolio demonstration, with enough supporting evidence for
technical follow-up questions.

The central conclusion must remain unchanged:

> The governed data platform and evaluation process worked, but the selected motor-current model
> missed the held-out failure and is not production-ready.

The demonstration does not require another pipeline run, model fit, or sealed-test evaluation.

## One-minute non-technical explanation

RailPulse studies the air compressor used by a metro train's pneumatic systems. Sensors generate
far more measurements than a person could inspect manually, so the project organizes the data and
tests whether abnormal operating patterns can provide useful failure warnings.

The platform first preserves the original records, then checks and cleans them, and finally turns
them into compressor operating cycles and past-only model features. Databricks jobs coordinate
these steps, Delta Lake provides reliable managed tables, and MLflow records the model experiments.

The final model was tested once on a later time period it had never seen. It missed the one accepted
failure and generated 73 false alert episodes. Therefore, the project does not recommend using the
model for real maintenance decisions. The value of the project is the reproducible engineering,
leakage-safe evaluation, and honest decision—not an inflated prediction claim.

## System story

```text
Official telemetry and failure reference
                  |
                  v
Governed Databricks Volume and checksum verification
                  |
                  v
Bronze: immutable source-aligned Delta tables
                  |
                  v
Silver: parsing, validation, quarantine, and quality metrics
                  |
                  v
Gold: compressor cycles, failure horizons, point-in-time features, modelling view
                  |
                  v
MLflow candidate comparison and validation-only alert-policy selection
                  |
                  v
One-time chronological held-out evaluation
                  |
                  v
Databricks AI/BI decision-support dashboard
```

## Demonstration sequence

| Time | Show | Explain | Evidence |
| --- | --- | --- | --- |
| 0:00–0:45 | Problem and architecture | Why compressor telemetry needs a governed pipeline and why four failure records require cautious conclusions | README architecture and dataset section |
| 0:45–1:45 | Unity Catalog Bronze and Silver tables | Raw preservation, deterministic validation, accepted/quarantine separation, and rerun safety | 1,516,948 telemetry rows, four failure records, zero quarantined records, 354 forward gaps |
| 1:45–2:45 | Gold products | How telemetry becomes 15,766 compressor cycles, time-aware labels, and past-only feature snapshots | Gold table inventory and contracts |
| 2:45–3:45 | Databricks Jobs | How task dependencies orchestrate Bronze, Silver, and Gold without schedules or hidden manual steps | Unscheduled workflow graphs and successful run histories |
| 3:45–4:45 | MLflow comparison | Why chronological splits and point-in-time features prevent future information from leaking into training | Candidate comparison and evaluation contracts |
| 4:45–6:15 | AI/BI dashboard | Contrast validation evidence with the final held-out result and separate platform quality from model quality | Six read-only datasets and the `NOT_PRODUCTION_READY` banner |
| 6:15–7:00 | Conclusion and next experiment | Explain why the model was rejected and how richer sensors plus a new future evaluation boundary would be tested | Sealed-test result and limitations |

## Evidence to point out

### Data platform

- `1,516,948` telemetry records reconcile through managed Bronze and Silver tables.
- All four reviewed failure records are preserved.
- Silver separates accepted and quarantined records and records data-quality evidence rather than
  silently repairing source data.
- Gold contains `15,766` deterministic loaded-cycle rows.
- Identical reruns reconcile as unchanged instead of duplicating records.
- Databricks workflows are dependency-gated, unscheduled, and packaged as Python-wheel tasks.

### Evaluation design

- Model inputs use strict past-only windows ending at the cycle prediction timestamp.
- Train, validation, and test partitions follow calendar time; random row splitting is prohibited.
- Model and alert-policy choices were frozen before opening the held-out period.
- The held-out workflow had zero automatic retries and ran exactly once.
- Only aggregate test evidence is reported; row-level test scores are not persisted or exposed.

### Final result

| Metric | Validation | Held-out test |
| --- | ---: | ---: |
| Rows | 4,730 | 4,217 |
| Positive rows | 5 | 13 |
| AUPRC | 0.011954 | 0.001671 |
| Accepted failure events | 1 | 1 |
| Detected failure events | 1 | 0 |
| Missed failure events | 0 | 1 |
| False alert episodes | 33 | 73 |
| False alerts per operating day | 1.1786 | 1.1774 |

The nearly unchanged false-alert rate did not translate into held-out event detection. This is the
main reason the current model and policy are not production-ready.

## Suggested live navigation

1. Open the GitHub README and introduce the architecture and verified evidence.
2. In Databricks Catalog, show the Bronze, Silver, and Gold schemas and a small table inventory.
3. In Workflows, open one dependency-gated pipeline and explain the task order and unscheduled state.
4. In MLflow, show the parent comparison run and two nested candidate runs without presenting the
   validation winner as final performance.
5. Open the unpublished RailPulse AI/BI dashboard and lead with the readiness banner and four
   held-out KPI tiles.
6. Finish with the validation-versus-test table, then show data-quality and operations panels as
   evidence that platform success and model success are different questions.

Opening or refreshing the dashboard can start the assigned SQL warehouse. Start compute only for an
approved live demonstration, and stop the warehouse immediately afterward. Do not rerun the sealed
test or the model-selection workflows for a presentation.

## Likely interview questions

### Why use Spark and Databricks for 1.5 million rows?

The dataset can fit on one machine. Spark and Databricks are used to demonstrate scalable pipeline,
governance, orchestration, Delta, and MLflow patterns—not because this dataset requires a cluster.

### What is the medallion architecture here?

Bronze preserves source-aligned records, Silver applies explicit validation and quarantine rules,
and Gold creates analysis-ready cycles, labels, features, and modelling data.

### What is Delta Lake doing?

Delta tables provide governed, schema-aware storage and reliable merge behavior. RailPulse adds
deterministic keys and reconciliation checks so unchanged reruns do not duplicate data.

### How did the project avoid data leakage?

Features use only observations available before each prediction time. The split follows time, model
and alert-policy decisions use development data only, and the held-out period was opened once after
those choices were frozen.

### Why is the model result still valuable if it failed?

The evaluation discovered that the validation behavior did not generalize. Rejecting the candidate
is the correct engineering outcome and demonstrates that the controls prevented a weak model from
being presented as production-ready.

### What would the next modelling experiment change?

It would introduce a new versioned experiment with additional sensor evidence, such as pressure and
temperature context, and reserve a new future evaluation boundary. It would not retune against or
rerun the existing July test period.

## Evidence-based résumé bullets

- Built a Databricks medallion pipeline that reconciled 1.5 million metro-compressor telemetry
  records through governed Bronze, Silver, and Gold Delta tables with validation, quarantine,
  deterministic identities, and idempotent reruns.
- Engineered 15,766 compressor-cycle records, time-aware failure horizons, and point-in-time
  features with automated tests preventing future-data leakage and silent historical rewrites.
- Orchestrated dependency-gated serverless Databricks workflows and tracked chronological model
  comparisons with MLflow while keeping the held-out period sealed during development.
- Designed a one-time, aggregate-only held-out evaluation and AI/BI dashboard that rejected a weak
  model after it missed the accepted test failure, clearly separating platform reliability from
  production model readiness.

## Claims to avoid

- Do not call RailPulse a deployed predictive-maintenance system.
- Do not claim that the model prevents failures, reduces downtime, or saves maintenance cost.
- Do not describe validation performance as final performance.
- Do not imply statistical certainty from four published failure records.
- Do not expose workspace, account, job, run, dashboard, or user identifiers.
- Do not rerun or retune the sealed July evaluation.

## Remaining demonstration gate

Before publishing or recording the final demonstration:

1. Visually inspect the unpublished dashboard at desktop width.
2. Confirm every widget renders and the readiness warning is visible without scrolling past a
   favorable metric first.
3. Verify that no row-level score, record identifier, or failure-report text is exposed.
4. Stop the SQL warehouse after the review.
5. Decide separately whether to publish the dashboard and what sharing permissions are appropriate.
