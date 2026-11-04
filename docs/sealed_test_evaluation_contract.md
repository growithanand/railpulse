# Sealed-test evaluation contract

## Purpose

`motor-current-sealed-test-evaluation-contract-v1` freezes what RailPulse's final held-out
evaluation is allowed to do before any July test row is read. The contract contains identities and
guardrails only. It does not load data, authorize execution, create a Databricks job, or start
compute.

## Frozen evaluation path

The final evaluation must preserve the choices already made without test feedback:

- use the selected `validation-2020-06_test-2020-07` chronological split;
- fit `motor-current-only` on the training partition only;
- preserve the p95 threshold derived from validation scores;
- apply the selected two-consecutive-cycle persistence rule;
- keep the 60-minute merge gap, 120-minute cooldown, and two-hour warning horizon unchanged; and
- score only the test partition.

The model is not refitted on train plus validation. Doing so would change its probability scale and
make the frozen validation-derived threshold a different policy.

## One-time access rules

The contract permits one separately approved test read. It requires explicit execution approval,
does not itself authorize that execution, and prohibits automatic retries. If another test read is
ever proposed, it requires a new versioned decision rather than an implicit rerun.

Results are report-only and cannot be used to retune the model, threshold, persistence, merge gap,
or cooldown. That makes the held-out evaluation terminal evidence instead of another development
loop.

## Allowed output

Only aggregate evidence may leave the evaluation boundary:

- test row and label counts plus cycle-level area under the precision-recall curve;
- operating-day and accepted failure-event counts;
- detected and missed event counts plus event-detection rate and warning lead;
- total, warning, inside-failure, and false alert-episode counts; and
- total and false alert episodes per operating day.

Row-level probabilities, episode lists, feature values, identifiers, workspace metadata, and run
identifiers are not approved outputs. No result table, model registration, or policy update is part
of this contract.

## Managed outcome

The [sealed-test scoring adapter](sealed_test_scoring_adapter.md) now proves the frozen identities,
train-only fit, held-out-only scoring, and in-memory score reconciliation with synthetic fixtures.
The [sealed-test alert evaluation adapter](sealed_test_alert_evaluation_adapter.md) now strips labels,
applies only the frozen alert policy, and introduces synthetic failure events after construction.
The [sealed-test managed runner](sealed_test_managed_runner.md) requires explicit access approval
and returns only the exact aggregate allowlist. Its declaration, deployment, and execution were
reviewed separately. The [single managed result](managed_sealed_test_evaluation_result.md) missed the
accepted held-out event and is frozen as report-only evidence; the contract prohibits retuning or a
second July run.
