# Directional engineering baseline contract

## Purpose

`motor-current-low-directional-v1` defines a transparent revision of the first engineering
baseline. It tests whether unusually low 15-minute mean motor current is a useful warning signal.
This direction follows the development-only drift evidence: June's dominant negative regime moves
to higher current while observed positive rows remain lower.

## Fixed candidates

Five inclusive thresholds are declared before candidate evaluation:

| Candidate | Candidate alert condition |
| --- | --- |
| `mean-current-at-most-0.9a` | mean current <= 0.9 A |
| `mean-current-at-most-1.0a` | mean current <= 1.0 A |
| `mean-current-at-most-1.2a` | mean current <= 1.2 A |
| `mean-current-at-most-1.5a` | mean current <= 1.5 A |
| `mean-current-at-most-2.0a` | mean current <= 2.0 A |

The grid spans a strict low-current rule through a more sensitive candidate. It is intentionally
small enough to inspect directly and does not use the test period.

## Semantics and guardrails

- Only trainable modelling-view rows with a finite feature value receive a Boolean candidate alert.
- The comparison will report train and validation results separately.
- Candidate selection may use validation evidence, but the test period remains sealed.
- Cycle alerts are not yet merged into operational alert episodes.
- A threshold is an empirical benchmark, not a compressor diagnosis or published equipment limit.

This contract defines candidates only. It does not select a threshold or claim predictive value.
