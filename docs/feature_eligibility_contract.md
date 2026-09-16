# Motor-current feature eligibility contract

## Decision

`motor-current-15m-eligibility-v1` defines a label-independent coverage boundary for the existing
15-minute motor-current feature. An available window is eligible when its maximum in-window gap is
strictly less than 20 seconds. A window is ineligible when the gap is at least 20 seconds, when the
feature is unavailable, or when required coverage context is missing or invalid.

This contract does not use failure-horizon status, target values, motor-current magnitude, or model
results. `eligible` means only that the feature has acceptable observed-time continuity under this
rule; it does not mean normal, healthy, or negative.

## Rationale

The source cadence is nominally 10 seconds, and Silver telemetry already defines an interval of at
least 20 seconds as a material forward gap. Reusing that boundary gives the eligibility decision a
source-quality meaning independent of failure labels.

The preceding full-source profile found fifth-percentile cutoffs of 75 observations and 891 seconds
of first-to-last span. Those cutoffs remain diagnostics rather than eligibility conditions:

- 4,582 windows tie exactly at 75 observations, making count percentile selection unstable;
- all 53 count-only strict-tail windows already contain an internal material gap; and
- the 26 tail-only windows have no material gap and miss the span cutoff by only one or two seconds.

The later policy comparison was not used to optimize this boundary. Its target-retention counts are
descriptive evidence only and cannot change the version 1 rule.

## Gap calculation

For an available feature window, the maximum gap is the greater of:

- leading uncovered seconds when the first contributing observation follows a source gap; and
- the largest recorded forward interval after the first contributing observation.

If neither is present, the maximum is zero. A leading source interval may begin before the feature
window, so only its uncovered portion inside the window is used. Internal intervals use their
recorded duration.

## Output

| Column | Meaning |
| --- | --- |
| `motor_current_eligibility_version` | Fixed contract identifier. |
| `motor_current_eligibility_minimum_excluded_gap_seconds` | Versioned boundary, always 20. |
| `motor_current_maximum_window_gap_seconds` | Calculated gap for available windows with valid context; otherwise null. |
| `motor_current_eligibility_status` | `eligible` or `ineligible`. |
| `motor_current_eligibility_reasons` | Deterministic reason codes; empty only for eligible rows. |

Ineligibility reasons are:

| Reason | Meaning |
| --- | --- |
| `feature_missing_prediction_boundary` | The cycle has no causal prediction timestamp. |
| `feature_missing_prediction_observation` | Accepted telemetry lacks the prediction-time anchor. |
| `missing_or_invalid_coverage_context` | An available feature cannot be evaluated safely. |
| `material_window_gap` | Maximum in-window gap is at least 20 seconds. |
| `unsupported_feature_status` | The feature availability status is outside the versioned contract. |

The transformation fails closed: only an available feature with valid context and a maximum gap
below 20 seconds is eligible. Tests cover the exact 19/20-second boundary, leading and internal
gaps, missing features, missing context, and invariance when horizon labels are changed.

## Scope and deferred work

The contract is a lazy Spark transformation and does not write a table. It assumes coverage context
has at most one row per cycle; a later materialization boundary must validate key uniqueness before
write. The next increment will apply and reconcile this exact contract over the complete source
without persisting output.

Eligibility for additional sensors or feature windows requires separately versioned evidence. Model
training, chronological splitting, target balancing, threshold tuning, evaluation, and performance
claims remain outside this contract.
