# Validation-only alert event evaluation

## Purpose

`motor-current-validation-alert-event-evaluation-v1` defines how a completed, label-blind alert
policy is compared with the accepted failure interval represented in the frozen June validation
period. It measures event detection, warning lead time, late alerts, false alerts, and episode
burden without changing any alert timestamp or opening the July test period.

This is an evaluation contract, not a policy-selection result. Its first managed execution has now
produced validation evidence, but no policy has been selected.

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

## Guarded Spark adapter

`motor-current-validation-alert-evaluation-adapter-v1` first invokes the existing guarded,
label-blind construction path for all five policies. Only after those episodes exist does it derive
the distinct operating dates from validation timestamps and select accepted failures whose start is
inside the frozen June validation boundaries.

The adapter projects only record ID, failure start, failure end, and rejection reasons from the
failure input. It rejects missing columns and any validation event with rejection reasons. Events
outside June are filtered before collection, so the July event cannot enter evaluation. Every
policy result must reconcile to the same operating-day count, single June event, and source episode
count. Results remain in memory and no policy is selected.

The focused Spark integration suite passes all three cases. Its complete-path case proves that
4,730 validation cycles flow through all five policies, only the June event is exposed when April
and July events are present, operating days are derived from timestamps, every episode is
classified exactly once, and a rejected June row fails closed.

## Read-only managed runner

`databricks-gold-selected-candidate-validation-alert-evaluation-v1` resolves only the allowlisted
managed `gold.modeling_view_v2` and `silver.failure_events_accepted` tables. It prepares the frozen
development populations, refits the selected candidate, invokes the guarded five-policy evaluation,
and returns deterministic JSON.

The output contains the two source table identities, contract versions, reconciled cycle/day/event
counts, the compact validation-score profile, and five policy evaluations. It deliberately omits
cycle probabilities and the in-memory episode collection. The runner contains no table writer,
MLflow writer, model registration, policy-selection rule, schedule, trigger, or test-scoring path.
Three focused unit tests verify named parameters, allowlisted table reads, operation order, compact
output routing, and wrapped contract failures. The repository's system-Python wheel build succeeds,
and direct wheel inspection confirms both the runner module and console entry point are packaged.
A Databricks workflow now runs this compact evaluator only after a successful managed modelling-view
refresh.

## Next gate

An unscheduled two-task workflow is now declared. It invokes the existing managed modelling-view v2
job first and runs the compact evaluation entry point only after `ALL_SUCCESS`. The evaluation task
uses serverless environment version 4, the project wheel only, queueing, and a one-run concurrency
limit. It has no schedule or trigger.

The bundle, resource-scoped deployment, and first separately approved managed run are complete. Do
not access the sealed July test period or persist row-level scores. Policy selection remains a
separate reviewed decision.

Strict bundle validation passes. The selected read-only plan proposes one job creation, five
existing upstream job updates, zero deletions, zero unchanged selected resources, and leaves eight
resources outside the selection. Detailed plan inspection confirms that every existing-job update
changes only the dynamically versioned shared-wheel dependency; task graphs, parameters, schedules,
and concurrency settings remain unchanged. At that plan checkpoint, nothing had been deployed or
run.

## Deployment status

The reviewed selection was deployed from Git commit `e46307c`. Deployment created the evaluation
job, refreshed the five planned upstream shared-wheel references, uploaded 20 bundle files, and
deleted no file or resource. It did not start a workflow run.

Independent remote readback confirms a UI-locked two-task job with the expected modelling-view job
reference, `ALL_SUCCESS` dependency, evaluation entry point, four namespace parameters, serverless
environment version 4, one wheel dependency, queueing, and one-run concurrency. No schedule or
trigger is present.

## First managed result

The first separately approved run completed successfully after refreshing the governed modelling
view. It reconciled 4,730 June validation cycles, 28 operating days, and one accepted failure event.
The output remained compact and no row-level score or episode table was written.

| Policy candidate | Event detected | Warning lead | Alert episodes | False episodes | False episodes/day |
| --- | ---: | ---: | ---: | ---: | ---: |
| p90 single cycle | Yes | 1h 56m 24s | 68 | 67 | 2.39 |
| p95 single cycle | Yes | 1h 33m 26s | 67 | 66 | 2.36 |
| p99 single cycle | No | — | 27 | 27 | 0.96 |
| p90 two consecutive cycles | Yes | 1h 33m 26s | 59 | 58 | 2.07 |
| p95 two consecutive cycles | Yes | 27m 41s | 34 | 33 | 1.18 |

The p95 two-consecutive-cycle candidate has the lowest false-alert burden among the policies that
detected the event, but it also provides the shortest warning lead. The p99 single-cycle candidate
is quieter but misses the only accepted validation event. Because the comparison contains only one
failure event, it does not establish general production performance. July remains sealed and the
next gate is an explicit policy-review decision.
