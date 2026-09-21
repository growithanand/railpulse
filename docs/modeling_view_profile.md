# Full-source modelling-view profile

## Purpose

`motor-current-failure-modeling-view-profile-v1` measures whether the complete modelling view has
enough chronological label and event coverage to define honest train, validation, and test periods.
It is read-only and does not select split boundaries.

## Run locally

```bash
.venv-wsl/bin/python -m railpulse.evaluation.modeling_view_profile --master "local[4]"
```

The command reads the materialized Gold cycles and feature snapshots, rebuilds accepted Silver
telemetry and failure events from Bronze, regenerates the two-hour horizons, applies the modelling
view contract, and prints deterministic JSON.

## Reported evidence

- trainable and excluded cycle counts;
- each exclusion reason counted independently;
- positive and negative trainable labels;
- earliest and latest trainable prediction timestamps;
- trainable positive-cycle coverage for every accepted failure event;
- feature, horizon, telemetry, and failure-source lineage.

Exclusion reasons can overlap, so their counts are not expected to sum to the excluded-row count.
The profile instead verifies that trainable rows have no reasons and every excluded row has at least
one.

## Verified complete-source result

The read-only command was executed against the configured official local source on 2026-09-21.

| Measure | Verified result |
| --- | ---: |
| Total modelling-view rows | 15,766 |
| Trainable rows | 15,413 |
| Excluded rows | 353 |
| Trainable positive labels | 22 |
| Trainable negative labels | 15,391 |
| Accepted failure events | 4 |
| Failure events with trainable positive cycles | 3 |
| Earliest trainable prediction | 2020-02-01 00:24:57 |
| Latest trainable prediction | 2020-08-31 20:00:58 |

| Exclusion reason | Cycles |
| --- | ---: |
| Feature ineligible | 348 |
| Horizon censored | 3 |
| Inside a published failure interval | 2 |
| Missing prediction boundary | 63 |
| Unsupported horizon status | 0 |

The 63 missing-boundary rows are also feature-ineligible, so exclusion-reason counts overlap. The
four accepted failure events contribute 0, 4, 5, and 13 trainable positive cycles in source-row
order. This confirms that the eligibility rule retains all 22 observed positive-cycle labels, but
one accepted event still has no positive cycle under the two-hour horizon.

## Verified complete-source result

The read-only command was executed against the configured official local source on 2026-09-21.

| Measure | Verified result |
| --- | ---: |
| Total modelling-view rows | 15,766 |
| Trainable rows | 15,413 |
| Excluded rows | 353 |
| Trainable positive labels | 22 |
| Trainable negative labels | 15,391 |
| Accepted failure events | 4 |
| Failure events with trainable positive cycles | 3 |
| Earliest trainable prediction | 2020-02-01 00:24:57 |
| Latest trainable prediction | 2020-08-31 20:00:58 |

| Exclusion reason | Cycles |
| --- | ---: |
| Feature ineligible | 348 |
| Horizon censored | 3 |
| Inside a published failure interval | 2 |
| Missing prediction boundary | 63 |
| Unsupported horizon status | 0 |

The 63 missing-boundary rows are also feature-ineligible, so exclusion-reason counts overlap. The
four accepted failure events contribute 0, 4, 5, and 13 trainable positive cycles in source-row
order. This confirms that the eligibility rule retains all 22 observed positive-cycle labels, but
one accepted event still has no positive cycle under the two-hour horizon.

## Interpretation limits

This profile describes modelling eligibility and chronological coverage. It is not model training,
failure-event recall, alert evaluation, or evidence of maintenance benefit. Split boundaries will be
selected only after reviewing this output, and model or threshold selection must not use the future
test period.
