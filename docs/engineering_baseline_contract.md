# Engineering baseline contract

## Purpose

`motor-current-robust-deviation-v1` is RailPulse's first interpretable condition score. It measures
how far a cycle's 15-minute mean motor current is from the typical negative training cycle. It does
not yet produce an alert or a failure prediction.

## Training population

The baseline fits only rows that are:

- marked trainable by the modelling-view contract;
- earlier than the frozen validation boundary at 2020-06-01;
- labelled negative under the two-hour failure-horizon contract.

Positive training rows do not define normal operation. Validation and test rows cannot influence
the fitted parameters. Excluded modelling rows are also ignored.

## Parameters and score

The training population supplies two transparent parameters:

1. the median 15-minute mean motor current;
2. the median absolute deviation from that median.

For an eligible feature value `x`, the score is:

```text
abs(x - training_median) / training_median_absolute_deviation
```

A score of zero is at the training median; larger values are farther from typical negative training
behaviour. The score is symmetric and intentionally simple. It does not imply that high or low
current is a failure, and no alert threshold has been selected.

## Guardrails and limitations

- The fitted center and scale carry explicit model and modelling-view versions.
- Duplicate cycle IDs, missing or non-finite reference values, and zero scale fail closed.
- Threshold selection will use validation data only.
- The test period remains sealed until the baseline and alert rule are frozen.
- One current feature cannot represent every compressor failure mode; this is a transparent
  benchmark for later comparison, not a production diagnosis.
