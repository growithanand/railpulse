# Past-only motor-current feature contract

## Scope

`motor-current-15m-v1` is RailPulse's first temporal sensor feature. For each cycle prediction
timestamp, it summarizes accepted `motor_current` observations from the immediately preceding 15
minutes. This increment defines and tests the reusable Spark transformation only; it does not run
the feature over the complete source or persist a feature table.

Motor current is a useful first engineering signal because the publisher identifies it as the
measured current for one motor phase and describes broad operating-state signatures. This feature
is descriptive. It does not encode a health threshold or claim that any current level predicts
failure.

## Time boundary

For prediction time `t`, the feature window is:

```text
(t - 15 minutes, t]
```

The observation exactly 15 minutes before prediction is excluded. An observation exactly at the
prediction timestamp is included because it is available at the cycle's exclusive stop boundary.
Every observation after prediction is excluded. Tests append a large future value and verify that
none of the historical feature values change.

Accepted source timestamps have whole-second precision. The Spark implementation converts
timezone-free timestamps to a seconds axis using `TIMESTAMPDIFF` and applies
`rangeBetween(-899, 0)`. It then joins the rolling result to the exact cycle prediction timestamp.
This avoids a cycle-by-telemetry range join and prevents centered or following-row windows.

## Output

| Column | Meaning |
| --- | --- |
| `motor_current_15m_feature_version` | Fixed contract identifier. |
| `motor_current_15m_window_seconds` | Window length, always 900 seconds. |
| `motor_current_15m_window_start` | Exclusive lower timestamp boundary. |
| `motor_current_15m_status` | Feature availability status. |
| `motor_current_15m_observation_count` | Non-null accepted observations in the window. |
| `motor_current_15m_first_observation_timestamp` | First contributing timestamp. |
| `motor_current_15m_last_observation_timestamp` | Last contributing timestamp. |
| `motor_current_15m_minimum_amperes` | Minimum contributing value. |
| `motor_current_15m_mean_amperes` | Arithmetic mean of contributing values. |
| `motor_current_15m_maximum_amperes` | Maximum contributing value. |

The status is `available` when the exact prediction observation exists,
`missing_prediction_boundary` when the cycle has no prediction timestamp, and
`missing_prediction_observation` when that timestamp is absent from the accepted telemetry input.
Missing features retain a zero observation count and null timestamps/statistics.

## Gaps, causality, and deferred work

The feature does not fill gaps or assume a fixed number of samples. Count and first/last timestamps
make the observed support inspectable. Later work must define a minimum coverage rule before model
training. Feature values must never include failure-horizon targets or observations after the
prediction timestamp.

The current dataset represents one compressor, so the rolling window is globally ordered. A
multi-asset version must partition by a stable equipment identifier. The read-only
[full-source feature profile](temporal_feature_profile.md) now documents availability and observed
support, but it does not establish a minimum coverage rule. Additional sensors/windows,
persistence, model selection, and performance claims remain separate reviewed increments.
