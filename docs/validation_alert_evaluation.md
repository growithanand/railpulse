# Validation-only alert event evaluation

## Purpose

`motor-current-validation-alert-event-evaluation-v1` defines how a completed, label-blind alert
policy is compared with the accepted failure interval represented in the frozen June validation
period. It measures event detection, warning lead time, late alerts, false alerts, and episode
burden without changing any alert timestamp or opening the July test period.

This is an evaluation contract, not a policy-selection result. No managed policy outputs have been
evaluated yet.

## Matching rules

The evaluator uses the existing two-hour failure horizon. For each alert episode, only its start
timestamp is classified:

- **warning episode:** starts from two hours before the failure start up to, but not including, the
  failure start;
- **inside-failure episode:** starts from the failure start through the failure end, inclusive; or
- **false alert episode:** starts outside both intervals.

The left-closed, right-open warning interval matches the existing cycle-label semantics: a
prediction must precede the failure, and a failure exactly two hours later is still within the
horizon. When an event has multiple warning episodes, warning lead time is measured from the first
one to the failure start.

## Reported evidence

For one policy candidate the evaluator returns:

- accepted, detected, and missed failure-event counts plus event-detection rate;
- the first warning timestamp and warning lead time for the June event;
- total, warning, inside-failure, and false alert-episode counts; and
- total and false alert episodes per observed operating day.

Every episode must reconcile to exactly one of the three classifications. Inside-failure episodes
are reported separately because they are too late to be warnings but should not be described as
unrelated false alerts.

## Guardrails

- Accept only the current episode and policy contract versions and a declared candidate ID.
- Require chronological, non-overlapping episodes entirely inside the selected validation period.
- Require exactly one accepted event whose complete two-hour warning window and failure interval
  stay inside validation; this reflects the current data rather than claiming general coverage.
- Reject zero or impossible operating-day counts.
- Keep July test rows, raw event descriptions, feature values, storage, and policy selection out of
  this layer.

Three focused unit tests cover an exact two-hour warning boundary, a late inside-failure alert, an
unmatched false alert, a missed event, burden reconciliation, sealed-test rejection, invalid day
counts, changed versions, and timezone-aware failures.

## Next gate

Add a guarded adapter that derives the observed June operating-day count and the single accepted
June failure interval from the governed Spark inputs, then evaluates all five in-memory policy
results. Review that evidence before freezing one policy. Do not access the sealed July test period
or persist row-level scores during this step.
