# Development alert-policy selection

## Decision

`motor-current-validation-alert-policy-selection-v1` freezes
`p95-two-consecutive-cycles` for the next development step. This is a validation-only engineering
decision, not a production-performance claim and not a result from the sealed July test period.

The rule is deliberately simple and reproducible:

1. A candidate is eligible only if it detects every accepted validation failure event.
2. Among eligible candidates, select the unique policy with the lowest false-alert episode count.

P99 single-cycle produced fewer false alerts overall, but it is ineligible because it missed the
accepted June event. P95 two-consecutive-cycle is the unique lowest-burden candidate among the four
that detected the event.

## Frozen managed evidence

All candidates were evaluated over the same 4,730 validation cycles, 28 operating days, and one
accepted June failure event.

| Policy candidate | Event detected | Warning lead | False episodes | False episodes/day | Eligible |
| --- | ---: | ---: | ---: | ---: | ---: |
| p90 single cycle | Yes | 1h 56m 24s | 67 | 2.39 | Yes |
| p95 single cycle | Yes | 1h 33m 26s | 66 | 2.36 | Yes |
| p99 single cycle | No | — | 27 | 0.96 | No |
| p90 two consecutive cycles | Yes | 1h 33m 26s | 58 | 2.07 | Yes |
| p95 two consecutive cycles | Yes | 27m 41s | 33 | 1.18 | Yes — selected |

## Guardrails

The selection contract:

- requires the complete five-candidate evidence in its declared order;
- requires exact agreement with the reviewed managed evaluation;
- rejects inconsistent event, episode, and warning counts;
- requires full validation-event detection before burden ranking;
- preserves the original threshold, persistence, merge-gap, and cooldown contract; and
- contains no July metric, row-level score, model registration, table writer, or deployment action.

Only one accepted validation failure was available, so this selection does not establish how the
policy will generalize. Its purpose is to freeze one reproducible candidate before preparing the
single sealed-test evaluation.

## Next gate

The [sealed-test evaluation contract](sealed_test_evaluation_contract.md) now accepts only the frozen
model and this frozen policy while preserving training-only fit and the validation-derived threshold.
It does not read July or authorize execution. The next increment is a guarded Spark adapter tested
only with synthetic fixtures; real test access and managed execution remain separate approvals.
