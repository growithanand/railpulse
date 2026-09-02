# MetroPT-3 data dictionary

## Interpretation rules

This dictionary preserves the official UCI/PDF descriptions and adds complete-file observed ranges.
The descriptions are **supported by dataset documentation**, not independently verified equipment
manuals. They should not be promoted to causal physical explanations without further evidence.

Analogue measurements remain floating-point values in Silver. Digital source values are encoded as
floating-point `0.0`/`1.0`; Silver will validate and normalize them without discarding the raw token.

## Identifier and time fields

| Source field | Planned canonical field | Source type | Observation and use |
| --- | --- | --- | --- |
| unnamed first column (`index` in UCI metadata) | `source_index` | integer | 0-15,169,470 in steps of 10. Treat as a source sequence identifier, not event time. |
| `timestamp` | `event_timestamp` | text -> timestamp | `yyyy-MM-dd HH:mm:ss`; 2020-02-01 00:00:00 through 2020-09-01 03:59:50; timezone unpublished. |

## Analogue sensors

| Source field | Unit | Observed range | Dataset-supported meaning | Caution |
| --- | --- | ---: | --- | --- |
| `TP2` | bar | -0.032 to 10.676 | Pressure measured on the compressor. | Exact measurement location and tolerance are not specified. |
| `TP3` | bar | 0.730 to 10.302 | Pressure generated at the pneumatic panel. | Expected operating limits require engineering validation. |
| `H1` | bar | -0.036 to 10.288 | Pressure associated with the pressure drop during cyclonic-separator filter discharge. | Do not infer a failure directly from a low/high value. |
| `DV_pressure` | bar | -0.032 to 9.844 | Pressure drop when the air-dryer towers discharge; the PDF says zero indicates loaded operation. | Near-zero negative values may reflect sensor offset. |
| `Reservoirs` | bar | 0.712 to 10.300 | Downstream reservoir pressure, expected by the PDF to be close to `TP3`. | The acceptable `TP3` difference is not published. |
| `Oil_temperature` | degrees Celsius | 15.400 to 89.050 | Compressor oil temperature. | Alarm limits are not published. |
| `Motor_current` | A | 0.020 to 9.295 | Current for one phase of the three-phase motor. The PDF associates approximately 0 A with off, 4 A with unloaded, 7 A with loaded, and 9 A with startup. | These are approximate operating signatures, not validated classification thresholds. |

## Digital sensors

Every digital field has the observed domain `{0.0, 1.0}`.

| Source field | Active-state description from dataset documentation | Caution |
| --- | --- | --- |
| `COMP` | Air-intake-valve signal; active when there is no intake, indicating off or unloaded operation. | The PDF later says MPG activates COMP; exact control logic needs cycle-level validation. |
| `DV_eletric` | Compressor outlet-valve control; active during loaded operation. | Source spelling `eletric` is preserved deliberately. |
| `Towers` | Selects dryer tower: inactive is tower one, active is tower two. | This is a selector state, not a health label. |
| `MPG` | Starts loaded operation when APU pressure falls below 8.2 bar and activates COMP according to the PDF. | Validate observed transitions against pressures before relying on the stated relationship. |
| `LPS` | Low-pressure switch; active below 7 bar. | A switch activation is an operating signal, not automatically a recorded failure event. |
| `Pressure_switch` | Detects discharge in the air-drying towers. | Active polarity beyond the published description is not independently verified. |
| `Oil_level` | Active when compressor oil is below expected level. | “Expected” threshold is not published. |
| `Caudal_impulses` | Pulse output associated with air flow from the APU to the reservoirs. | Values are binary pulse indications, not a cumulative flow measurement; conversion to flow needs calibration not supplied here. |

## Candidate engineering features

The following are hypotheses for later phases, not measured findings:

| Candidate | Inputs | Interpretation status |
| --- | --- | --- |
| Pressure-loss rate | `TP2`, `TP3`, `Reservoirs` over past-only windows | Reasonable engineering hypothesis |
| Pressure recovery time | Cycle boundaries plus `TP3`/`Reservoirs` | Reasonable engineering hypothesis |
| Loaded/unloaded motor behavior | `Motor_current`, `COMP`, `DV_eletric`, `MPG` | Supported in broad terms by dataset documentation; thresholds require empirical validation |
| Oil-temperature trend | `Oil_temperature` over past-only windows | Reasonable engineering hypothesis |
| Compressor duty cycle | `COMP`, `DV_eletric`, `Motor_current` | Reasonable engineering hypothesis |
| Dryer-tower behavior | `Towers`, `Pressure_switch`, `DV_pressure` | Supported in broad terms by dataset documentation; exact sequence requires empirical validation |

No feature may use centered/future windows or failure-report information unavailable at scoring time.
