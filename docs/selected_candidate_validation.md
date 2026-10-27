# Selected-candidate validation scoring

## Purpose

`motor-current-validation-scores-v1` defines the guarded scoring boundary between the retained
development candidate and later alert-policy evaluation. It fits `motor-current-only` on the frozen
training partition and exposes probabilities only for the frozen validation partition.

`motor-current-validation-score-profile-v1` adds a compact, read-only evidence boundary over those
scores. It returns aggregate metadata and score distributions without retaining cycle-level
probabilities.

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

## Compact profile behavior

The profile revalidates the score columns, candidate and split identities, unique cycle keys,
labels, probability bounds, and population counts. It then reports the validation AUPRC, fitted
coefficients and intercept, optimizer iterations, positive rate, and separate score distributions
for all, positive, and negative validation rows. Each distribution contains the row count, minimum,
mean, maximum, and the 1st, 5th, 10th, 25th, 50th, 75th, 90th, 95th, and 99th percentiles.

The command below rebuilds the local full-source modelling view and prints deterministic JSON. It
performs no table, file, MLflow, registry, or row-level score write.

```bash
.venv-wsl/bin/python -m railpulse.models.selected_candidate_validation_profile --master "local[4]"
```

## Failure behavior

Scoring fails before fitting when contract metadata, columns, population counts, label counts,
unique keys, or chronological boundaries differ from the managed comparison evidence. It also fails
if output probabilities are null, non-finite, outside `[0, 1]`, duplicated by cycle key, or do not
reconcile to all 4,730 validation rows.

Three focused Spark integration tests verify valid scoring and profiling, test-row exclusion,
probability bounds, changed-population rejection, July-timestamp rejection, incompatible-contract
rejection, and score-identity rejection.

## Full-source verification status

The read-only local command was attempted on 2026-10-25. It stopped before model fitting because the
reconstructed local development population contained 6,320 training rows and 4,729 validation rows,
while the managed comparison evidence requires 6,325 and 4,730. Positive-label counts still matched
at four and five; the difference was six negative rows across the two development partitions.

This is a successful fail-closed provenance check, not validation evidence. No full-source score,
distribution, threshold, or test result was produced. The exact managed population remains the
authority for the retained-candidate decision.

The first managed profile run later passed the same guard and reconciled 6,325 training rows plus
4,730 validation rows, including four and five positive labels respectively. It reproduced
validation AUPRC `0.0119542268` and emitted the reviewed compact profile without a row-level write.
The complete result and interpretation limits are preserved in the
[first managed profile record](managed_selected_candidate_validation_profile_first_run.md).

## Managed runner

The read-only managed runner resolves the allowlisted `gold.modeling_view_v2` table, derives the
reviewed train and validation frames, applies the guarded scorer, and returns only the compact
profile as JSON. It contains no table writer, MLflow writer, model registration, schedule, trigger,
or test-period scoring path. Three unit tests verify its named parameters, ordered routing, source
table identity, compact result, and fail-closed error handling.

The bundle now declares an unscheduled two-task workflow. It invokes the existing managed
modelling-view v2 job first and runs the profile task only after `ALL_SUCCESS`. The profile task uses
serverless environment version 4, the project wheel only, queueing, and a one-run concurrency limit.
Strict bundle validation passes. A resource-scoped plan proposes one job addition, five existing-job
updates, no deletions, and leaves seven resources outside the selection.

The selected deployment was applied from Git commit `a8d3f9f`. It created the profile job, updated
the five planned existing jobs, uploaded 16 bundle files, and deleted no file or resource. A direct
bundle summary confirmed the new job is bundle-managed and UI-locked with the reviewed two tasks,
entry point, named parameters, queue, concurrency limit, and serverless environment. Deployment did
not start a workflow run or compute.

## Next gate

The scorer, compact profile, managed runner, and unscheduled workflow are implemented, validated,
deployed, and reconciled through their first managed run. A separate
[validation-only policy contract](validation_alert_policy_contract.md) now freezes five threshold and
persistence candidates. Its deterministic label-blind episode engine and guarded Spark adapter are
implemented and tested. The next gate is validation-only failure-event evaluation. Row-level score
persistence and the sealed test partition remain out of scope until a policy is frozen.
