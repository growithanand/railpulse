# Managed sealed-test evaluation result

## Outcome

The single approved sealed-test workflow run completed successfully on 2026-11-04. Both tasks
finished on their first attempt, the job history contains exactly one run, and no automatic or
manual retry occurred.

The workflow refreshed the governed modelling view, fitted the frozen `motor-current-only`
candidate on training rows only, scored the held-out July partition, and evaluated the unchanged
`p95-two-consecutive-cycles` alert policy. It returned aggregate evidence only.

## Held-out evidence

| Metric | Result |
| --- | ---: |
| Test rows | 4,217 |
| Positive rows | 13 |
| Negative rows | 4,204 |
| Test AUPRC | 0.0016708466 |
| Operating days | 62 |
| Accepted failure events | 1 |
| Detected failure events | 0 |
| Missed failure events | 1 |
| Event detection rate | 0% |
| Warning lead time | Not applicable |
| Alert episodes | 73 |
| Warning episodes | 0 |
| Inside-failure episodes | 0 |
| False-alert episodes | 73 |
| Alert episodes per operating day | 1.1774 |
| False-alert episodes per operating day | 1.1774 |

The counts reconcile: 4,217 rows equal 13 positive plus 4,204 negative rows; the single accepted
event equals zero detected plus one missed event; and all 73 alert episodes are classified as false
alerts.

## Interpretation

The managed data and evaluation system worked as designed, but the selected model and alert policy
did not generalize to the held-out failure event. The validation policy detected the one June event
with 33 false episodes across 28 days. On the held-out period, its false-alert rate remained almost
identical at about 1.18 per day, but it missed the one accepted July event entirely. The result does
not support a claim that this motor-current-only model is ready for operational maintenance use.

This is still useful project evidence: the leakage controls, frozen selection, one-time access gate,
aggregate-only reporting, and no-retry rule prevented an optimistic validation result from being
presented as final performance.

## Frozen conclusion

This held-out result is report-only. RailPulse will not retune the threshold, change the candidate,
or rerun July in response to it. Future modelling work must be a new versioned experiment using
additional sensor evidence and a newly defined future evaluation boundary. The current project now
moves to honest KPI presentation, the Databricks SQL dashboard, and final demonstration material.
