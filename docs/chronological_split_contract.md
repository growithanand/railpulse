# Chronological split candidate contract

## Purpose

`calendar-chronological-splits-v1` defines fixed train, validation, and test candidates before any
model is fitted. The boundaries use prediction time only. Features, labels, failure identities, and
future performance cannot influence a row's assignment.

Each interval is left-closed and right-open. A timestamp exactly on a boundary belongs to the later
period:

- train: before `validation_start`;
- validation: from `validation_start` up to, but not including, `test_start`;
- test: at or after `test_start`.

## Declared candidates

| Candidate | Validation starts | Test starts |
| --- | --- | --- |
| `validation-2020-05_test-2020-07` | 2020-05-01 | 2020-07-01 |
| `validation-2020-06_test-2020-07` | 2020-06-01 | 2020-07-01 |
| `validation-2020-06_test-2020-08` | 2020-06-01 | 2020-08-01 |

These candidates deliberately expose different calendar allocations. The read-only full-source
comparison in `docs/chronological_split_profile.md` reports row, label, prediction-time, and
failure-event coverage for every period.

## Selected split

`calendar-chronological-split-selection-v1` freezes
`validation-2020-06_test-2020-07` for subsequent modelling:

- train: prediction timestamps before 2020-06-01;
- validation: prediction timestamps from 2020-06-01 through 2020-06-30;
- test: prediction timestamps on or after 2020-07-01.

It is the only declared candidate with a represented positive failure event in every period. The
selection used coverage feasibility only; no feature threshold, model score, alert threshold, or
test-period performance was inspected. From this decision onward, test rows are unavailable for
model and threshold selection.

## Leakage controls

- Boundaries are whole-month calendar timestamps and are independent of feature values.
- Assignment does not inspect the outcome label or matched failure event.
- Validation always precedes test; periods never overlap.
- Timestamps are timezone-free, matching the published source contract.
- The future test period remains unavailable for model or threshold selection after a candidate is
  chosen.

The contract does not fit a model, balance classes, infer equipment health, or claim broad event
coverage. Each selected period contains only one represented failure event.
