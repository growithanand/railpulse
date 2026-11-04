# Sealed-test managed runner

## Purpose

`databricks-gold-selected-candidate-sealed-test-evaluation-v1` is the compact, read-only runner for
the frozen final evaluation. It connects the managed modelling view and accepted-failure table to
the guarded scoring and selected-policy adapters, then returns only approved aggregate evidence.

The runner is implemented, packaged, deployed through an unscheduled Databricks workflow, and has
completed its single approved execution. Both tasks were declared with zero retries.

## Explicit access gate

The runner requires `--allow_sealed_test_read`. Without that flag it fails before resolving or
reading either managed table. This makes an accidental invocation fail closed.

The flag was an execution gate, not proof by itself that the test had remained unread. Before the
single managed execution, remote review confirmed empty run history, no schedule or trigger, zero
automatic retries, and separate human approval.

## Read and evaluation order

After approval, the runner:

1. resolves only `gold.modeling_view_v2` and `silver.failure_events_accepted` through their existing
   catalog allowlists;
2. reads those two sources;
3. fits and scores through the frozen sealed-test scoring adapter;
4. constructs and evaluates only the selected alert policy; and
5. converts the result to the contract's exact aggregate allowlist.

It contains no writer, merge, cache, MLflow run, model registration, policy update, or retry loop.

## Output boundary

The serialized result contains source-table identities, contract and adapter versions, selected
model and policy IDs, and the approved aggregate metrics:

- test row and label counts plus AUPRC;
- operating-day and failure-event counts;
- detection, missed-event, and warning-lead evidence; and
- total, warning, inside-failure, and false episode counts and daily burden.

It excludes row-level probabilities, targets, feature values, fitted coefficients, episode lists,
event record IDs, event timestamps, workspace URLs, job IDs, and run IDs. The metric dataclass field
order must exactly match the contract allowlist or execution fails.

## Local evidence

Four focused tests pass. They prove that missing approval causes zero table reads, the approved path
uses only the two allowlisted sources in the expected order, compact metrics are mapped correctly,
unsafe adapter failures are wrapped, and detailed score, episode, and event identities are absent
from the result.

The project wheel builds successfully without dependency downloads. Direct archive inspection
confirms both the runner module and
`databricks_gold_selected_candidate_sealed_test_evaluation` console entry point are packaged.

## Workflow boundary

The declared two-task workflow first invokes the existing governed modelling-view refresh. Only
after that dependency succeeds can the aggregate evaluator start. It has one-run concurrency,
queueing, zero retries on both tasks, no schedule, and no trigger. The access flag is supplied only
to the evaluator task.

## Validation and plan evidence

Authenticated strict bundle validation passes. The resource-scoped plan proposes one addition for
this job, five upstream updates caused only by the newly built shared wheel, zero deletions, and nine
unrelated resources left unselected. The five planned updates are the existing Bronze, Silver,
loaded-cycle, feature-snapshot-v2, and modelling-view-v2 jobs in the dependency chain.

## Deployment evidence

The selected deployment created the bundle-managed job and applied the five reviewed shared-wheel
updates without starting a run. Remote inspection confirms that the job is UI-locked, unscheduled,
has no trigger, queues requests, permits one concurrent run, and contains exactly the two reviewed
tasks. The evaluator alone receives the explicit access flag. Its run history is empty.

The bundle declaration fixes both tasks at zero retries; Databricks omits these default-valued fields
from the returned job JSON.

## Managed result

After explicit approval for held-out access and serverless charges, the workflow ran exactly once.
Both tasks succeeded on attempt zero. The aggregate output reconciled 4,217 test rows, 13 positives,
4,204 negatives, 62 operating days, and one accepted failure event. The selected policy detected no
event, missed the accepted failure, and classified all 73 alert episodes as false alerts. Test AUPRC
was `0.0016708466`.

The result is frozen and report-only. The workflow will not be rerun, and its outcome will not be
used to retune the model or alert policy. See the
[managed sealed-test evaluation result](managed_sealed_test_evaluation_result.md) for the complete
aggregate evidence and interpretation.
