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

These candidates deliberately expose different calendar allocations. They are definitions, not a
split selection. The next read-only profile will compare row counts, positive and negative labels,
operating-time coverage, and represented failure events in every period. A candidate will only be
selected after that evidence is reviewed.

## Leakage controls

- Boundaries are whole-month calendar timestamps and are independent of feature values.
- Assignment does not inspect the outcome label or matched failure event.
- Validation always precedes test; periods never overlap.
- Timestamps are timezone-free, matching the published source contract.
- The future test period remains unavailable for model or threshold selection after a candidate is
  chosen.

The contract does not fit a model, balance classes, infer equipment health, or claim that any
candidate has sufficient event coverage.
