# Sealed-test alert evaluation adapter

## Purpose

`motor-current-sealed-test-alert-evaluation-adapter-v1` connects guarded held-out scores to the
frozen p95 two-consecutive-cycle alert policy and the existing two-hour event-matching semantics.
It remains an in-memory library boundary with no job, CLI entry point, writer, bundle resource, or
real-data execution.

## Evidence ordering

The adapter preserves the separation between predictions and outcomes:

1. Revalidate the score result, population counts, metadata columns, selected model, selected
   policy, split, and contract versions.
2. Project only cycle ID, prediction timestamp, and failure probability.
3. Construct episodes using only the frozen selected policy.
4. Derive operating days from score timestamps.
5. Only after episodes exist, filter accepted failure events to the held-out period.
6. Apply the unchanged warning, inside-failure, and false-alert classification rules.

Targets and event timestamps therefore cannot move, merge, suppress, or create an alert. The adapter
also avoids rerunning the other four development candidates on held-out data.

## Guardrails

The boundary requires:

- exact scoring-adapter, model, policy, split, and sealed-test contract identities;
- the frozen 6,325-row training summary and a reconciled held-out score population;
- one test identity across every score row, unique keys, bounded probabilities, and timestamps at or
  after the test boundary;
- the unchanged selected threshold, persistence, merge gap, cooldown, episode version, and
  event-evaluation version; and
- exactly one accepted held-out failure event with its complete two-hour warning window inside the
  test period.

Row-level targets remain inside the guarded scoring object for cycle-level AUPRC reconciliation but
are removed before episode construction. Episodes and scores remain in memory and are not approved
managed outputs.

## Synthetic evidence

Three Spark tests use four generated held-out score points plus synthetic June and July failure
events. The selected policy produces one warning episode and detects the synthetic held-out event
with 3,540 seconds of lead time. The suite also rejects altered policy identity, changed score
counts, rejected test events, and absence of the required test event.

The shared pure evaluator also has a separate sealed-period test proving that June evidence is
rejected by the held-out boundary while the original validation behavior remains unchanged.

## Next gate

The [sealed-test managed runner](sealed_test_managed_runner.md) now calls the frozen scoring and alert
adapters only after an explicit access flag and returns the contract's exact aggregate metric set.
Wheel inspection now confirms the runner module and console entry point. The next increment is an
unscheduled workflow declaration. Do not deploy or execute that declaration; deployment and the
single test read remain separate reviews.
