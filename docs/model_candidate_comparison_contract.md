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

The fixed comparison version is `cycle-context-logistic-candidate-comparison-v1`. The next small
increment can implement the training-only fitting and validation-only metric collection against
this reviewed contract.
