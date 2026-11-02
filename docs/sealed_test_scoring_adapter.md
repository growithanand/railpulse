# Sealed-test scoring adapter

## Purpose

`motor-current-sealed-test-scoring-adapter-v1` implements the model-fitting and scoring portion of
the frozen final-evaluation contract. It accepts a Spark modelling view, fits the selected
`motor-current-only` logistic model on training rows, and scores held-out rows in memory.

This module is not connected to a job, console entry point, bundle resource, table writer, or model
registry. Its integration tests use generated rows only; no real July data has been read.

## Partition roles

The adapter uses prediction timestamps to create two disjoint inputs:

- **training:** timestamps before the June validation boundary;
- **held out:** timestamps at or after the July test boundary.

Validation-period rows are neither fitted nor scored. The synthetic test deliberately places an
invalid row in June and proves that it cannot influence either path.

The training population must exactly match the frozen managed model-selection evidence: 6,325 rows,
including four positive and 6,321 negative cycle labels. The held-out population is not frozen in
advance because counting it would itself inspect the sealed partition; at execution time it must be
nonempty, contain both labels, and have unique cycle identifiers.

## Guardrails

Before fitting or scoring, the adapter requires:

- the current modelling-view version and selected five-feature contract;
- valid trainable row status, horizon status, target agreement, keys, and finite feature values;
- chronological train and held-out boundaries;
- unique keys inside and across both partitions; and
- exact reconciliation of output scores to the held-out population.

The output records the frozen contract, split, model-selection, model-candidate, policy-selection,
and policy-candidate identities. Probabilities must be finite and within `[0, 1]`; model
coefficients, intercept, iteration count, and test AUPRC are also validated.

Row-level scores exist only as an in-memory internal input for later episode construction. They are
not an approved managed-job response and must not be persisted.

## Synthetic evidence

Three Spark integration tests pass. They prove the valid training-only fit and held-out-only scoring
path, isolation from invalid validation rows, fixed metadata, bounded probabilities, population
reconciliation, and fail-closed handling of altered training counts, duplicate cross-partition keys,
invalid test features, and missing columns.

## Next gate

The [sealed-test alert evaluation adapter](sealed_test_alert_evaluation_adapter.md) now strips targets
from these in-memory scores, applies only the frozen p95 two-consecutive-cycle policy, and evaluates
episodes against synthetic failure events only after construction. The next gate is a compact
aggregate-only managed runner; real test access, workflow declaration, deployment, and compute
remain separate approvals.
