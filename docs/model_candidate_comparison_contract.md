# Cycle-context model candidate comparison contract

## Purpose

This contract fixes the rules for RailPulse's first like-for-like test of whether cycle operating
context adds useful predictive information. It defines the candidates and the development-only
evaluation boundary; it does not fit a model, publish performance, choose an alert threshold, or
inspect the sealed test period.

## Candidates

Both candidates use the same trainable rows from
`motor-current-cycle-context-failure-modeling-view-v2` and the same logistic-regression estimator.
Using one shared population means that any difference is attributable to the input set rather than
to one candidate receiving easier or more complete rows.

| Candidate | Inputs |
| --- | --- |
| `motor-current-only` | The five existing 15-minute motor-current and window-quality inputs. |
| `motor-current-plus-cycle-context` | The same five inputs plus current-cycle duration, previous-cycle duration, and previous idle time. |

Identifiers, timestamps, labels, row-status fields, matched failure IDs, and lineage metadata are
not model inputs.

## Evaluation boundary

- Fit preprocessing and model parameters on rows before 2020-06-01 only.
- Use 2020-06-01 through 2020-06-30 for candidate comparison.
- Rank candidates primarily by area under the precision-recall curve, which is appropriate for the
  very rare positive labels in this dataset.
- Do not read or score rows from 2020-07-01 onward during development.
- Keep alert-threshold selection separate from this feature-set comparison.

The fixed comparison version is `cycle-context-logistic-candidate-comparison-v1`. Every execution
must fit both candidates and collect the validation-only metric against this reviewed contract.

## Fixed estimator configuration

Both candidates use the same Spark binomial logistic-regression configuration:

| Parameter | Fixed value |
| --- | ---: |
| Maximum iterations | 100 |
| L2 regularization | 0.1 |
| Elastic-net mixing | 0.0 |
| Fit intercept | Yes |
| Standardize features | Yes |

This first comparison deliberately does not add class weights, resampling, hyperparameter search,
probability calibration, or an alert threshold. Those choices would add extra degrees of freedom to
a development set containing only one represented failure event per chronological partition.

## Implemented development-data guard

`prepare_model_candidate_development_data` now enforces the comparison boundary before fitting. It
selects trainable version 2 rows strictly before 2020-07-01, validates finite values for all eight
permitted inputs, reconciles target labels, requires unique cycle IDs, and requires both labels in
the training and validation partitions. Its returned frames contain only the cycle ID, prediction
timestamp, target, and permitted model inputs; failure metadata and other columns are removed.

The guard deliberately validates feature and label values only inside the development period. A
change to a July test value therefore cannot change development validation or summaries.

## Implemented comparison engine

`compare_model_candidates` now accepts only the guarded development-data object. It fits the two
declared candidates in fixed order, evaluates validation area under the precision-recall curve, and
returns the shared estimator parameters, train/validation label counts, fitted intercepts and
coefficients, and completed iteration counts. The returned object contains no test frame, test
metric, selected threshold, or automatic winner.

These fields form the minimum evidence required by the MLflow tracking boundary. The managed
comparison has now run, but this contract still prohibits automatic selection and test-period
performance claims.

## Implemented MLflow tracking boundary

`track_candidate_comparison` validates the complete comparison result before opening any run. It
also requires the experiment name, code version, fixed feature-snapshot version, matching telemetry
and failure dataset versions, four lowercase SHA-256 source or ingestion identities, and a
timezone-free label-observation boundary.

One parent run records the fixed comparison, modelling-view, split, estimator, lineage, and sealed
test-partition contract. Two nested child runs then record each reviewed candidate's feature list,
train and validation population counts, optimizer iterations, validation AUPRC, fitted intercept,
and coefficients. The structured contract and candidate evidence are stored as JSON artifacts.

The boundary does not log a test metric, prediction, threshold, registered model, or automatic
winner. Its focused tests use an in-memory MLflow-compatible recorder, and the first managed run
has separately verified the same parent/child structure in the remote MLflow experiment.

## Implemented managed runner

`run_managed_gold_model_candidate_comparison` resolves only the allowlisted
`gold.modeling_view_v2` table. It requires explicit experiment and code-version inputs, derives one
complete tracking identity only from trainable rows before 2020-07-01, prepares the guarded
development frames, fits both fixed candidates, and sends the result to the tracking boundary.

Rows in the sealed test period cannot change the collected tracking identity. Mixed development
lineage, missing boundary columns, incompatible comparison evidence, or incomplete tracking
metadata fail the run before MLflow logging. Three focused unit tests and three Spark tests cover
the orchestration and tracking-context boundary.

## Deployed Databricks workflow

The bundle now declares a separate MLflow experiment and an unscheduled two-task comparison job.
The first task invokes the existing managed modelling-view workflow. Only after that succeeds does
the second task invoke the packaged comparison runner on serverless environment version 4 with the
wheel and `mlflow-skinny` dependencies. The runner receives the bundle-managed experiment path and
the exact bundle Git commit as its code version.

The workflow queues additional requests, allows one concurrent run, and has no schedule or trigger.
Strict bundle validation and a read-only resolved summary passed before deployment. The selected
deployment created the experiment and comparison job, updated five transitive upstream jobs only to
reference the newly built wheel, deleted no resources, and left five resources unselected. The
detailed diff preserved every existing task graph, parameter set, schedule state, queue setting, and
concurrency limit.

The first separately approved managed run completed successfully on 2026-10-24. It refreshed the
modelling view before fitting and recorded one finished parent plus two finished candidate runs.
Both candidates used 6,325 training rows with four positives and 4,730 validation rows with five
positives. `motor-current-only` produced validation AUPRC `0.011954`; the cycle-context extension
produced `0.008632`. This makes the simpler candidate the provisional development leader under the
fixed comparison, not a registered model or a sealed-test result. See the
[first managed comparison record](managed_model_candidate_comparison_first_run.md).
