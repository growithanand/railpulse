# Loaded-cycle boundary semantics

## Scope

RailPulse initially treats a contiguous `DV_eletric = 1` interval as evidence of loaded compressor
operation. The dataset documentation describes this outlet-valve control as active during loaded
operation, making it a defensible boundary signal. This is a provisional operational segmentation,
not a failure label, health state, or independently verified account of the complete compressor
control sequence.

Only accepted Silver telemetry is eligible. Boundary annotation uses the current observation and
its validated predecessor; it does not inspect future rows or failure reports.

## Boundary rules

| Output | Rule | Interpretation |
| --- | --- | --- |
| `is_loaded_operation` | Current `DV_eletric = 1` | Current observation supports loaded operation. |
| `is_cycle_start` | Continuous predecessor is inactive and current row is active | An observed `0 → 1` transition. |
| `is_cycle_stop` | Continuous predecessor is active and current row is inactive | An observed `1 → 0` transition; the current timestamp is the exclusive stop boundary. |
| `is_left_censored_cycle` | Current row is active without a continuous predecessor | Loaded operation is visible, but its true start is unknown. |

A predecessor is continuous only when it can be matched by `previous_source_index` and the current
row is not marked `is_forward_gap`. No start or stop transition is inferred across a material gap.
An active row after a gap begins a left-censored segment rather than an observed cycle.

## Deferred decisions

This increment does not assign cycle identifiers, aggregate durations, infer a right-censored final
cycle, or combine `COMP`, `MPG`, pressure, and motor-current behavior. Those steps require separate
tests and empirical inspection. In particular, approximate current levels and undocumented control
relationships must not be promoted to hard operating-state rules without evidence.
