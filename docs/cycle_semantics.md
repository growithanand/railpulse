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

## Segment identity

Every loaded segment is anchored to its first visible active record. An observed `0 → 1` start has
start type `observed`; an active record at the data boundary or after a material gap has start type
`left_censored`. The anchor record identifier and the `loaded-cycle-v1` contract label produce a
deterministic SHA-256 `loaded_cycle_id`.

The identifier and start metadata appear on every loaded row in the segment and on its observed
stop-boundary row. Carrying the identifier onto that inactive boundary preserves the exclusive stop
timestamp for later aggregation. Other inactive rows have null segment fields. The ordered
propagation uses only the current and preceding source rows, so appending future telemetry cannot
change identifiers already assigned.

## Cycle-level aggregation

Segment rows are reduced to one record per `loaded_cycle_id`. Each record retains the visible start
record and timestamp, start type, optional observed stop record and exclusive stop timestamp, and
the count of loaded observations. `observed_duration_seconds` is calculated from the visible start
to the exclusive stop only when that stop exists.

For an observed start and stop, this duration represents the complete visible cycle. For a
left-censored start, it is a lower bound because operation began before continuous observation. If
no stop is visible, the cycle is marked `is_right_censored = true`, its stop fields remain null, and
no duration is inferred from the final active sample.

Unlike a stable segment ID, right-censoring describes the current input snapshot. Later telemetry
can close a previously right-censored cycle.

## Gold persistence contract

The path-backed `gold.loaded_cycles` table merges on `loaded_cycle_id`. A new identifier inserts one
row. An existing right-censored row may gain loaded observations and may later receive its exclusive
stop boundary and duration. The merge retains the same identifier and start evidence throughout
that progression. Repeating an equivalent snapshot is a logical no-op.

Updates are monotonic. A source snapshot is rejected before the merge if it would reduce the loaded
observation count, change the anchor record, start type, or start timestamp, or modify or reopen a
cycle that is already closed. Source rows must also reconcile their censoring flag with their stop
fields and duration, and each identifier is checked against the `loaded-cycle-v1` derivation. These
rules prevent an older or incompatible snapshot from silently replacing stronger cycle evidence.

## Deferred decisions

This persistence boundary does not yet provide a full-source Gold build command, summarize sensor
behavior, or combine `COMP`, `MPG`, pressure, and motor-current behavior. Those steps require
separate tests and empirical inspection. In particular, approximate current levels and undocumented
control relationships must not be promoted to hard operating-state rules without evidence.
