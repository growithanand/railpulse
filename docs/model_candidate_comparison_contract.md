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

These fields form the minimum evidence required for later MLflow logging. MLflow integration and a
full-source comparison run remain separate increments; no candidate has been selected and no model
performance claim is made here.
