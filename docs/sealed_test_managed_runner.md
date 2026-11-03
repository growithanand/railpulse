# Sealed-test managed runner

## Purpose

`databricks-gold-selected-candidate-sealed-test-evaluation-v1` is the compact, read-only runner for
the frozen final evaluation. It connects the managed modelling view and accepted-failure table to
the guarded scoring and selected-policy adapters, then returns only approved aggregate evidence.

The runner is implemented and packaged locally. An unscheduled Databricks workflow is declared with
zero retries on both tasks, but it has not been deployed or run. No real test row has been read by
this checkpoint.

## Explicit access gate

The runner requires `--allow_sealed_test_read`. Without that flag it fails before resolving or
reading either managed table. This makes an accidental invocation fail closed.

The flag is an execution gate, not proof that a test read has never occurred. The future workflow
review must still confirm empty run history, no schedule or trigger, zero automatic retries, and a
separate human approval before the single managed execution.

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

The resource remains bundle-managed and has not been deployed. Its next gate is a strict bundle
validation and resource-scoped deployment-plan review. Deployment and the single serverless test
read remain separate explicit approvals.
