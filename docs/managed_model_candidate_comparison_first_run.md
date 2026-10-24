# First managed model-candidate comparison

## Purpose

This record preserves the deployment boundary, ordered workflow execution, MLflow evidence, and
interpretation of RailPulse's first managed model-candidate comparison. The comparison is limited to
the reviewed development partitions and does not inspect the sealed test period.

## Deployment and workflow boundary

The unscheduled comparison workflow and its bundle-managed MLflow experiment were deployed from
Git commit `f9b4fca`. The selected deployment created those two resources and refreshed the shared
wheel reference on five upstream jobs. It deleted no resources and started no compute.

The separately approved workflow run completed successfully on 2026-10-24. Its task order was:

1. refresh the existing modelling-view v2 workflow and its governed upstream dependencies;
2. start the comparison task only after the modelling-view workflow returned `SUCCESS`; and
3. fit and record the two fixed candidates in the managed MLflow experiment.

The loaded-cycle refresh, feature-snapshot v2 processing, modelling-view v2 processing, and final
comparison task all succeeded. The top-level tasks reported approximately 8 minutes 39 seconds of
combined setup and execution time.

## Recorded MLflow evidence

MLflow contains one finished parent run and two finished nested candidate runs. The parent records
the reviewed comparison, dataset, source-lineage, feature-snapshot, modelling-view, chronological
split, code, and sealed-test identities. Both children used the same populations:

| Population | Rows | Positive labels | Negative labels |
| --- | ---: | ---: | ---: |
| Training | 6,325 | 4 | 6,321 |
| Validation | 4,730 | 5 | 4,725 |

The fixed logistic-regression comparison produced:

| Candidate | Inputs | Validation AUPRC |
| --- | ---: | ---: |
| `motor-current-only` | 5 | 0.011954 |
| `motor-current-plus-cycle-context` | 8 | 0.008632 |

The five-input candidate's validation AUPRC was approximately 38% higher than the eight-input
cycle-context candidate's score. Under this fixed estimator, split, and population, the added cycle
duration and idle-time inputs therefore did not improve the primary development metric.

## Interpretation boundary

`motor-current-only` is the provisional development leader for this comparison, not a production
model. The result does not authorize a registered model, deployment, maintenance alert, or test-set
evaluation. In particular:

- only four positive labels were present in training and five in validation;
- no class weighting, resampling, hyperparameter search, calibration, or threshold tuning occurred;
- no July test rows were read or scored;
- no failure-level warning lead time or false-alarm burden was measured; and
- no automatic winner tag or registered model was created.

The next modelling gate is an explicit decision about whether to retain the simpler candidate for
alert-policy development. Any threshold selection must remain development-only, and the sealed test
period must stay untouched until the complete alert policy is frozen.

## Repository boundary

This record deliberately omits Databricks account identifiers, workspace URLs, resource IDs, and
local credential state. Those operational details remain in the managed platform rather than in the
public repository.
