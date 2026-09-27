# Cycle operating-context contract

## Purpose

`loaded-cycle-operating-context-v1` adds interpretable, past-only operating context at each loaded
cycle's prediction boundary. It is intended to investigate whether changing duty patterns explain
the motor-current regime drift that made the first engineering baselines unreliable.

The transformation uses only `gold.loaded_cycles`. It does not load failure labels, select a model
threshold, change feature eligibility, or write a new snapshot version.

## Features

Cycles are ordered by visible start timestamp and deterministic cycle ID. For each cycle, the
transformation records:

| Column | Meaning |
| --- | --- |
| `cycle_context_current_duration_seconds` | Complete duration of the current loaded cycle, known at its observed stop |
| `cycle_context_previous_cycle_id` | Auditable identifier of the immediately preceding visible cycle |
| `cycle_context_previous_duration_seconds` | Complete duration of the preceding loaded cycle |
| `cycle_context_previous_idle_seconds` | Time from the preceding observed stop to the current observed start |

Complete duration is emitted only for a cycle with an observed start and stop. Idle time is emitted
only when the current cycle is complete, the previous stop is observed, and the interval is not
negative. Appending a later cycle cannot change context already assigned to earlier cycles.

## Statuses

| Status | Meaning |
| --- | --- |
| `available` | Current and previous cycles are complete and the intervening idle interval is valid |
| `missing_prediction_boundary` | The current cycle has no observed stop, so no prediction is made |
| `left_censored_current_cycle` | The current visible duration is only a lower bound |
| `incomplete_current_cycle` | Current-cycle fields do not support a complete duration |
| `missing_previous_cycle` | No preceding visible cycle exists in the source |
| `incomplete_previous_cycle` | The preceding cycle does not have a complete duration |
| `invalid_previous_interval` | The preceding stop occurs after the current visible start |

The statuses preserve incomplete evidence instead of filling missing durations or assuming
continuity across source boundaries. They are feature-quality results, not health or failure labels.

## Next validation

The next increment will profile every Gold cycle by status and characterize the three duration
features without loading failure labels. No feature-snapshot expansion is justified until that
coverage is reconciled.
