# Cycle-to-failure horizon contract

## Scope

`cycle-failure-horizon-v1` defines how RailPulse assigns a future failure-onset target to a loaded
cycle. It is a pure Spark transformation over Gold cycles and accepted Silver failure events. This
increment does not persist a new table, derive sensor features, or report full-source label counts.

The default evaluation horizon is two hours (`7,200` seconds), matching the project question. The
transformation still requires the horizon to be supplied explicitly and records it on every row.

## Prediction boundary

The prediction timestamp is the cycle's observed exclusive stop timestamp. This ensures later
feature work can use only evidence available by the time the cycle ends. A closed left-censored
cycle remains label-eligible because its stop is observed; its incomplete start history must remain
visible to downstream feature eligibility rules. A right-censored cycle has no observed stop and
therefore receives `missing_prediction_boundary`, not a binary target.

## Failure-event rules

Only structurally accepted Silver failure events are inputs. The publisher does not specify whether
failure interval endpoints are inclusive, so version 1 conservatively treats the complete
`[failure_start, failure_end]` interval as ineligible for prediction. A cycle stop inside that
interval receives `inside_failure_interval` and no binary target.

For an otherwise eligible cycle, a failure onset is positive when:

```text
prediction_timestamp < failure_start <= prediction_timestamp + horizon
```

The strict lower bound means a failure beginning exactly at prediction time is not presented as a
future warning opportunity; it is covered by the in-failure exclusion. The upper bound is included.
When several events share the earliest eligible start, `source_row` and then `record_id` provide a
deterministic tie-break. The matched event identity, timestamps, and integer lead seconds remain in
the output for traceability.

## Observation and censoring rules

`label_observation_end` is the latest timestamp through which target outcomes are considered known.
A reported failure must start no later than this boundary to provide positive evidence. A positive
can be assigned as soon as that event is observed, even if the rest of the horizon has not elapsed.

| Status | Binary target | Meaning |
| --- | --- | --- |
| `positive` | `true` | The earliest eligible failure starts within the horizon. |
| `negative` | `false` | No eligible failure starts within a fully observed horizon. |
| `horizon_censored` | null | No failure is observed, but the dataset ends before the horizon. |
| `inside_failure_interval` | null | The prediction timestamp is within a published failure interval. |
| `missing_prediction_boundary` | null | The cycle has no observed stop timestamp. |

Null targets are deliberate and must not be coerced to negatives.

## Leakage guardrail and deferred work

Failure-horizon columns are targets and evaluation metadata, never model features. Future failure
information is permitted only inside label construction; all sensor features must later end at or
before `prediction_timestamp`. Random row-level splitting remains prohibited.

The full-source join, label-distribution inspection, persisted Gold boundary, past-only sensor
windows, post-failure recovery exclusions, and interval-endpoint sensitivity analysis remain
separate reviewed increments. No health, warning-lead-time, or model-performance result is claimed
by this contract alone.
