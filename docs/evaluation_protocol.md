# Chronological modelling-view contract

## Purpose

`motor-current-failure-modeling-view-v1` defines the row and column contract between RailPulse's
feature engineering and model-evaluation stages. It combines immutable feature snapshots with the
versioned two-hour failure horizon while keeping input features, labels, eligibility, and split
assignment conceptually separate.

The logical table name is `gold.modeling_view`, keyed by `loaded_cycle_id` with exactly one row per
Gold loaded cycle.

## Inputs and join rules

The view will be assembled from:

- `gold.feature_snapshots`, joined by `loaded_cycle_id`;
- `gold.loaded_cycles`, which supplies the observed cycle stop used as the prediction boundary;
- accepted failure events, passed through `cycle-failure-horizon-v1`;
- explicit failure-source lineage: dataset version, source checksum, and ingestion batch.

All joins must be one-to-one at the cycle key. The horizon `prediction_timestamp` must equal the
cycle's observed stop and must agree with the feature window boundary. A mismatch is a contract
failure, not a value to repair silently.

## Model inputs

The initial input contract contains only past-observable motor-current values and support metadata:

- 15-minute observation count;
- 15-minute minimum, mean, and maximum current;
- maximum gap within the feature window.

Identifiers, timestamps, eligibility fields, lineage, failure-horizon fields, matched-event fields,
and exclusion reasons are metadata—not model inputs. `failure_within_horizon` is the target and
`prediction_timestamp` is the chronological ordering column.

## Trainable rows

A row is `trainable` only when both conditions hold:

1. `motor_current_eligibility_status = 'eligible'`;
2. `failure_horizon_status` is either `positive` or `negative`.

For trainable rows, `failure_within_horizon` must be non-null and agree with the horizon status.
Positive maps to `true`; negative maps to `false`.

All other rows remain present with `modeling_row_status = 'excluded'` and one or more ordered
reasons:

| Reason | Meaning |
| --- | --- |
| `feature_ineligible` | The past feature window failed the label-independent eligibility rule. |
| `horizon_censored` | The complete future label window was not observable. |
| `inside_failure_interval` | The prediction timestamp falls inside a published failure interval. |
| `missing_prediction_boundary` | The cycle has no observed stop at which a prediction can be made. |
| `unsupported_horizon_status` | The row carries an unknown or future horizon status. |

Excluded rows are never converted to negative examples.

## Chronological splitting

Split assignment is deliberately deferred to a separate versioned contract. It must:

- order rows by `prediction_timestamp`;
- assign contiguous train, validation, and test periods;
- fit normalization, thresholds, and model parameters on training data only;
- prevent random row-level splitting;
- keep test data unavailable during model and threshold selection;
- report the positive events and operating duration represented in every split.

Exact cut points will be selected only after the complete modelling view is profiled. This avoids
choosing dates that manufacture convenient failure-event coverage.

## Leakage controls

- Feature eligibility is independent of failure labels.
- Feature windows contain no telemetry after the prediction timestamp.
- Future failure information appears only in target and evaluation metadata.
- Label-censored and in-failure rows remain null, not negative.
- Failure-source and feature-source lineage remain explicit.
- Changing feature, eligibility, horizon, or row-selection semantics requires a new view version.

## Deferred work

The deterministic Spark transformation now validates one-to-one keys, component versions, feature
window and prediction-boundary alignment, label semantics, and source lineage before assigning row
status and ordered exclusion reasons. The complete-source profile, chronological split selection,
persisted Delta view, baseline model, and event-level evaluation are separate increments.
