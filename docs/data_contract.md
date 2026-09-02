# MetroPT-3 data contract

## Status and scope

This contract covers the official **MetroPT-3 Dataset**, UCI dataset ID 791, and the two files in
the UCI archive retrieved on 2026-09-02. It defines the implemented
`bronze.telemetry_raw` and `bronze.failure_reports_raw` boundary. It does not define Silver labels or
claim that every published statement is internally consistent.

MetroPT-3 is distinct from the newer **MetroPT** dataset. MetroPT-3 contains 2020 compressor data,
15 sensor signals, no GPS variables, and a nominal 10-second cadence. The separate MetroPT dataset
described in the [Scientific Data article](https://doi.org/10.1038/s41597-022-01877-3) contains
January-June 2022 data, 20 variables including GPS, and 1 Hz acquisition. RailPulse uses only UCI
MetroPT-3 ID 791.

## Provenance

| Field | Verified value |
| --- | --- |
| Publisher | UCI Machine Learning Repository |
| Dataset | MetroPT-3 Dataset |
| UCI ID | 791 |
| DOI | [10.24432/C5VW3R](https://doi.org/10.24432/C5VW3R) |
| Landing page | [Official UCI record](https://archive.ics.uci.edu/dataset/791/metropt%2B3%2B) |
| Direct archive | [UCI archive](https://archive.ics.uci.edu/static/public/791/metropt%2B3%2Bdataset.zip) |
| Retrieval date | 2026-09-02 |
| License | [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) |
| RailPulse version ID | `uci-791-aab991a970e5` |

UCI does not publish a semantic version for this artifact. RailPulse therefore identifies the
retrieved archive by UCI ID plus the first 12 hexadecimal characters of its SHA-256 digest. The full
artifact and member hashes are stored in `docs/dataset_manifest.json`.

Required attribution:

> Davari, N., Veloso, B., Ribeiro, R., & Gama, J. (2021). MetroPT-3 Dataset [Dataset]. UCI Machine
> Learning Repository. https://doi.org/10.24432/C5VW3R.

## Verified source artifacts

| Artifact | Bytes | SHA-256 |
| --- | ---: | --- |
| `metropt3-uci-791.zip` | 218,381,995 | `aab991a970e58210de853bb8078ce0e63abb4d9412fdc5c79792dae3d8e1721a` |
| `MetroPT3(AirCompressor).csv` | 218,300,507 | `db30ccb4ea402e3c8bf2c99db06e288d4f2a772f6928f9dbe26a920d69793e24` |
| `Data Description_Metro.pdf` | 81,208 | `b00fac0e8899854078309bef4adaa480d82ecf14dc81c5097c3646973e824127` |

Raw artifacts belong under ignored `data/raw/source/` and must never be committed.

## Telemetry source contract

The CSV has 1,516,948 data rows and 17 physical columns: one unnamed source-index column,
`timestamp`, and 15 sensor signals. The exact header is:

```text
,timestamp,TP2,TP3,H1,DV_pressure,Reservoirs,Oil_temperature,Motor_current,COMP,DV_eletric,Towers,MPG,LPS,Pressure_switch,Oil_level,Caudal_impulses
```

Source spelling and case are contractual. In particular, `DV_eletric` is spelled with one `c`, and
the first header cell is empty. The UCI variable table calls that first column `index`; ingestion
will map it by position to `source_index_raw` while retaining the original header in provenance.

### Observed structure

| Property | Complete-file observation |
| --- | ---: |
| Rows | 1,516,948 |
| Source index | 0 through 15,169,470; every step is 10 |
| Timestamp format | `yyyy-MM-dd HH:mm:ss` |
| Timestamp timezone | Not published |
| First timestamp | `2020-02-01 00:00:00` |
| Last timestamp | `2020-09-01 03:59:50` |
| Exact 10-second transitions | 1,337,521 |
| Shorter transitions | 128,279, almost all 9 seconds |
| Longer transitions | 51,147, mostly 11-13 seconds |
| Material gaps implying at least one missing nominal slot | 354 |
| Estimated absent nominal 10-second slots | 326,972 |
| Maximum forward gap | 172,918 seconds (about 48.03 hours) |
| Duplicate timestamps | 0 |
| Out-of-order timestamps | 0 |

The defensible description is **nominal 10-second cadence with timestamp jitter and material gaps**.
The source index remains perfectly regular even when event timestamps jump, so it must not replace
the timestamp as an event-time clock.

### Complete-file structural checks

The verified artifact contains zero malformed rows, empty cells, timestamp parse errors, numeric
parse errors, non-finite numeric values, duplicate timestamps, reversed timestamps, or unexpected
source-index steps. All eight digital columns contain only numeric `0.0` and `1.0` values.

These observations do not make the measurements physically valid. Small negative pressure readings,
for example, remain legitimate raw observations until Silver range rules distinguish sensor tolerance
from invalid data.

## Implemented Bronze telemetry schema

Bronze preserves input tokens and never silently cleans them. The implementation uses an explicit
schema with source values represented as strings:

| Bronze field | Type | Source/meaning |
| --- | --- | --- |
| `record_id` | string | SHA-256 of table, input digest, and `source_index_raw` |
| `source_index_raw` | string | Unnamed first CSV column |
| `event_timestamp_raw` | string | Original `timestamp` token |
| `*_raw` for each sensor | string | Original numeric token for the 15 source sensors |
| `source_filename` | string | Input artifact name |
| `source_sha256` | string | Verified source-member digest |
| `source_modified_at` | timestamp or null | Filesystem/object metadata when available |
| `ingested_at` | timestamp | RailPulse ingestion time |
| `ingestion_batch_id` | string | Deterministic batch identifier |
| `dataset_version` | string | `uci-791-aab991a970e5` |
| `corrupt_record` | string or null | Full row when the explicit CSV shape cannot be parsed |

The local table is path-backed at `data/delta/bronze/telemetry_raw`; the logical table name remains
`bronze.telemetry_raw`. A rerun uses Delta `MERGE` on `record_id` and inserts only unseen source
records. Duplicate record IDs inside one input fail ingestion instead of silently collapsing rows.

Silver, not Bronze, will convert the source index to a long, timestamp text to an explicitly chosen
timestamp representation, analogue signals to doubles, and digital signals to validated binary
values. Until timezone evidence exists, RailPulse must not label these timestamps as UTC.

## Failure-report source contract

The dataset has no per-row target column. The company failure table in
`Data Description_Metro.pdf` is transcribed separately into
`data/reference/metropt3_failure_events.csv` under CC BY 4.0.

| Field | Meaning |
| --- | --- |
| `source_row` | RailPulse transcription row, 1-4; provides a stable unique key |
| `source_report_number` | Publisher text such as `#1`; not unique in the source |
| `start_time_raw`, `end_time_raw` | Original local-looking time text; timezone and endpoint semantics unknown |
| `failure_raw` | Original failure capitalization |
| `severity_raw` | Original severity text |
| `report_raw` | Original maintenance note, if present |
| `source_note` | RailPulse note identifying unresolved publisher ambiguity |

The four published intervals are:

| Source row | Report | Start | End | Failure | Report text |
| ---: | --- | --- | --- | --- | --- |
| 1 | `#1` | 2020-04-18 00:00 | 2020-04-18 23:59 | Air leak | none |
| 2 | `#1` | 2020-05-29 23:30 | 2020-05-30 06:00 | Air Leak | Maintenance on 30Apr at 12:00 |
| 3 | `#3` | 2020-06-05 10:00 | 2020-06-07 14:30 | Air Leak | Maintenance on 8Jun at 16:00 |
| 4 | `#4` | 2020-07-15 14:30 | 2020-07-15 19:00 | Air Leak | Maintenance on 16Jul at 00:00 |

Bronze preserves these strings in `_raw` fields. It records both the ingested transcription digest
`3a9e02204190394c65abf65d24b1553a1764030138162cc4779abf8d8b90fce5` and the official source-PDF
digest. Its deterministic record ID uses the transcription digest plus `source_row_raw`. The local
table is path-backed at `data/delta/bronze/failure_reports_raw`. Silver may add normalized fields,
but it must retain the raw values and ambiguity flags. Failure data must never be used as a
scoring-time feature.

## Known source ambiguities

1. UCI says 1 Hz in one section and 0.1 Hz in another. The CSV supports a nominal 10-second cadence,
   with jitter and gaps.
2. The PDF reports 15,169,480 instances; the UCI landing page and actual CSV contain 1,516,948.
3. Documentation says February-August 2020; the last CSV timestamp is September 1 at 03:59:50.
4. The failure table repeats `#1` for two different intervals.
5. “Maintenance on 30Apr” predates the second failure interval by about one month. RailPulse does
   not change it to May without publisher confirmation.
6. Timezone and interval-end inclusion are not specified.
7. UCI reports no missing values, which agrees with zero blank cells but does not mean continuous
   event time; 354 material timestamp gaps were observed.
8. UCI mentions segmentation, normalization, and feature extraction as preprocessing but does not
   provide enough detail on the landing page/PDF to reverse those steps.

## Reproduction

After manually placing or downloading the official CSV under ignored raw storage:

```powershell
.venv\Scripts\python -m railpulse.validation.metropt3 `
  "data\raw\source\metropt3-uci-791\MetroPT3(AirCompressor).csv"
```

The command streams through the complete file, prints JSON, and exits nonzero when the structural
contract fails. A successful structure check does not waive later Silver range, gap, and business
validation. Add `--full-interval-distribution` when every distinct timestamp interval is needed.

Bronze ingestion then verifies both input digests, writes the separate Delta tables, and prints row
reconciliation JSON:

```bash
.venv-wsl/bin/python -m railpulse.ingestion.bronze --master "local[4]"
```
