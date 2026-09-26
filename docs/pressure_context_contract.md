# Pressure-context feature contract

## Purpose

`panel-reservoir-pressure-difference-15m-v1` adds past-only operating context beyond motor current.
The MetroPT-3 documentation says downstream reservoir pressure should be close to pneumatic-panel
pressure, but it does not publish an acceptable difference. RailPulse therefore records the
observed relationship without treating any value as a fault limit.

## Prediction-time window

At each observed cycle-stop prediction timestamp `t`, the feature uses accepted Silver telemetry
from the strict trailing interval `(t - 15 minutes, t]`. The lower boundary is excluded and the
observation at `t` is included. Appending telemetry after `t` cannot change an existing feature.

For each row with both `TP3` and `Reservoirs`, the signed pressure difference is:

```text
TP3 - Reservoirs
```

The feature records:

- paired-observation count and first/last supporting timestamps;
- mean signed pressure difference;
- mean absolute pressure difference;
- maximum absolute pressure difference.

## Status and limitations

Missing prediction boundaries and missing prediction-time observations remain explicit statuses.
The transformation does not use failure reports, labels, centered windows, or future telemetry.

This pressure difference is operating context, not proof of leakage or failure. Exact sensor
locations, expected tolerance, and causal failure thresholds are not published. Full-source
availability and distribution profiling must precede snapshot integration or model use.
