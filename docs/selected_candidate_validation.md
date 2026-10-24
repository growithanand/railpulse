# Selected-candidate validation scoring

## Purpose

`motor-current-validation-scores-v1` defines the guarded scoring boundary between the retained
development candidate and later alert-policy evaluation. It fits `motor-current-only` on the frozen
training partition and exposes probabilities only for the frozen validation partition.

This implementation does not read or score July test rows, choose an alert threshold, construct an
alert episode, register a model, or persist scores.

## Required evidence

The scorer accepts only `ModelCandidateDevelopmentData` compatible with the reviewed comparison and
development-selection contracts. It requires the exact managed comparison population:

| Partition | Rows | Positive labels | Negative labels |
| --- | ---: | ---: | ---: |
| Training | 6,325 | 4 | 6,321 |
| Validation | 4,730 | 5 | 4,725 |

Both frames must expose only the previously reviewed development columns. Training timestamps must
remain before 2020-06-01; validation timestamps must be on or after 2020-06-01 and strictly before
2020-07-01. Candidate, comparison, modelling-view, split, and selection versions must remain fixed.

## Fit and score behavior

The scorer resolves the retained feature list from the development-selection contract. It uses the
same fixed Spark binomial logistic-regression configuration as the candidate comparison and fits
only on training rows.

The returned score frame contains exactly:

- `loaded_cycle_id`;
- `prediction_timestamp`;
- `failure_within_horizon`;
- `failure_probability`;
- the fixed `validation` partition marker;
- model-selection version;
- candidate ID; and
- chronological split ID.

Feature values, failure metadata, raw predictions, test rows, and the fitted Spark model are not
exposed. The accompanying result retains validation AUPRC, coefficients, intercept, optimizer
iterations, and reconciled train/validation summaries for auditing.

## Failure behavior

Scoring fails before fitting when contract metadata, columns, population counts, label counts,
unique keys, or chronological boundaries differ from the managed comparison evidence. It also fails
if output probabilities are null, non-finite, outside `[0, 1]`, duplicated by cycle key, or do not
reconcile to all 4,730 validation rows.

Three focused Spark integration tests verify valid scoring, test-row exclusion, probability bounds,
changed-population rejection, July-timestamp rejection, and incompatible-contract rejection.

## Next gate

The boundary is implemented and tested locally. It has not generated or persisted full-source
validation scores in Databricks. The next increment should add reviewed summary evidence for those
scores before any threshold or alert-episode logic is introduced.
