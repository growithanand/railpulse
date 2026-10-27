# Validation-only alert-policy candidate contract

## Purpose

`motor-current-validation-alert-policy-candidates-v1` freezes the first small policy set for the
retained `motor-current-only` model. It defines what later alert-episode logic may compare on the
validation partition; it does not construct alerts, select a winner, inspect test rows, or write a
table.

## Evidence source

Thresholds come from the all-row probability distribution recorded by
`motor-current-validation-score-profile-v1` during the first successful managed profile run. The
source quantiles are independent of validation labels:

| Quantile | Minimum probability |
| --- | ---: |
| p90 | 0.0006357159376330079 |
| p95 | 0.0006360194297156563 |
| p99 | 0.0006361879416607152 |

These small raw values are expected from the unweighted model and extreme class imbalance. Their
purpose is to define ranked validation candidates, not calibrated failure probabilities.

## Declared candidates

| Candidate | Threshold | Required consecutive cycles | Merge gap | Cooldown |
| --- | ---: | ---: | ---: | ---: |
| `p90-single-cycle` | p90 | 1 | 60 minutes | 120 minutes |
| `p95-single-cycle` | p95 | 1 | 60 minutes | 120 minutes |
| `p99-single-cycle` | p99 | 1 | 60 minutes | 120 minutes |
| `p90-two-consecutive-cycles` | p90 | 2 | 60 minutes | 120 minutes |
| `p95-two-consecutive-cycles` | p95 | 2 | 60 minutes | 120 minutes |

The three single-cycle candidates measure the false-alarm and event-coverage effect of increasingly
selective score thresholds. The two persistence candidates test whether requiring a second
consecutive high-scoring cycle reduces isolated alerts. Merge and cooldown windows are held fixed at
one and two hours respectively so the comparison varies only threshold and persistence. The shorter
merge gap joins brief interruptions, while the longer cooldown can suppress a distinct repeat start;
using identical windows would make cooldown redundant after merging.

## Guardrails

- Candidate IDs are stable and unique.
- Probability thresholds and their source quantiles must be finite and strictly within `(0, 1)`.
- Persistence is a positive integer number of consecutive time-ordered cycles.
- Merge and cooldown values are nonnegative integer minutes.
- The selected model and development-selection identities remain frozen.
- The July test partition remains sealed.
- No candidate is preferred or selected by this contract.

## Deterministic construction

`motor-current-validation-alert-episodes-v1` implements the label-blind policy mechanics over a
sequence containing only cycle ID, prediction timestamp, and probability:

1. reject empty inputs, duplicate cycle IDs, timezone-aware timestamps, and invalid probabilities;
2. sort by prediction timestamp and cycle ID;
3. apply the inclusive probability threshold and let every below-threshold cycle break persistence;
4. trigger a persistent alert on the cycle that satisfies the required consecutive count;
5. merge qualified streaks when the next trigger is no more than 60 minutes after the prior end;
6. suppress a later merged episode whose start falls strictly inside the 120-minute cooldown of the
   last retained episode; and
7. return ordered in-memory episodes plus qualifying, streak, merge, and suppression counts.

Suppressed episodes do not extend or restart cooldown. The engine has no label, failure-event, test,
Spark, storage, or model-registration dependency. Five focused episode tests cover inclusive
thresholds, ordering, persistence timing, merge/cooldown separation, duplicate keys, invalid values,
and changed-policy rejection.

## Guarded Spark adapter

`motor-current-validation-alert-adapter-v1` accepts only the reconciled
`SelectedCandidateValidationScores` result. It reruns the compact-profile guards, projects exactly
`loaded_cycle_id`, `prediction_timestamp`, and `failure_probability`, orders those points, and then
invokes all five policies in their frozen order. Validation labels and feature values never enter the
episode engine.

The adapter requires every policy result to evaluate all 4,730 reviewed validation cycles and to
preserve the declared candidate order. It returns the compact score profile and in-memory episode
evidence; it contains no persistence or test path. Three focused Spark tests now cover the complete
fit/profile/adapter path, test-row exclusion, and fail-closed score identity.

## Event-evaluation boundary

`motor-current-validation-alert-event-evaluation-v1` now compares constructed episode starts with
the accepted failure interval represented in June. It uses the existing two-hour failure horizon,
reports warning lead time and event detection, separates inside-failure late alerts from unrelated
false alerts, and reconciles total and false episode burden per operating day. Alert construction is
unchanged.

The next gate is a guarded Spark adapter that supplies only the governed June event and label-free
operating-day count to this evaluator for all five policies. Policy selection, persistence, and test
evaluation remain separate increments.
