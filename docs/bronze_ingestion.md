# Bronze ingestion

## Purpose

Phase 3 materializes the verified MetroPT-3 inputs as separate, source-aligned Delta tables. Bronze
keeps all source tokens as strings and adds lineage; parsing, normalization, sensor-range decisions,
and quarantine belong to Silver.

| Logical table | Local generated path | Stable source key |
| --- | --- | --- |
| `bronze.telemetry_raw` | `data/delta/bronze/telemetry_raw` | `source_index_raw` |
| `bronze.failure_reports_raw` | `data/delta/bronze/failure_reports_raw` | `source_row_raw` |

Generated Delta storage and `_delta_log` content are ignored. They must be reproduced from the
checksum-verified sources rather than committed.

## Runtime

The verified local pairing is:

- Python 3.12.3 in Ubuntu WSL
- Eclipse Temurin JDK 21.0.12.1 LTS
- PySpark / Apache Spark 4.2.0
- Delta Lake 4.4.0

Spark 4.2 supports Java 17, 21, or 25 and Python 3.10+. Delta Lake 4.4 is built and tested for Spark
4.2. Native Windows is not the verified Spark path because its Hadoop layer requires a separate
`winutils.exe`; no third-party helper binary was added to this repository.

Install Python dependencies inside WSL:

```bash
python3 -m venv .venv-wsl
.venv-wsl/bin/python -m pip install --upgrade pip
.venv-wsl/bin/python -m pip install -e ".[dev,spark]"
```

If the Ubuntu Python installation lacks `ensurepip`, create the environment with `--without-pip`
and bootstrap pip from the official PyPA `get-pip.py`, or install Ubuntu's `python3-venv` package.

Use a supported JDK 21 and configure the current shell. A project-local JDK may be placed below the
ignored `.tools/` directory:

```bash
export JAVA_HOME=/path/to/a/jdk-21
export PYSPARK_PYTHON="$PWD/.venv-wsl/bin/python"
export PYSPARK_SUBMIT_ARGS="--driver-memory 3g --conf spark.driver.extraJavaOptions=-Djdk.lang.Process.launchMechanism=VFORK pyspark-shell"
```

The `VFORK` option avoids a JDK `jspawnhelper` shutdown warning when the portable JDK resides on a
Windows-mounted filesystem. It is not needed for a normal Linux-installed JDK.

## Inputs and execution

Place the official CSV at:

```text
data/raw/source/metropt3-uci-791/MetroPT3(AirCompressor).csv
```

The failure transcription is versioned at
`data/reference/metropt3_failure_events.csv`. Both input digests and the failure source-PDF digest
come from `docs/dataset_manifest.json`.

Run both ingestions from the repository root in WSL:

```bash
.venv-wsl/bin/python -m railpulse.ingestion.bronze --master "local[4]"
```

The command fails before Spark ingestion if either input digest differs from the manifest. It prints
JSON with source, inserted, matched, before, and after counts for each logical table.

## Schema and preservation

Telemetry keeps `source_index_raw`, `event_timestamp_raw`, and all 15 sensor tokens in `_raw`
string fields. Failure reports similarly keep eight `_raw` source fields. Bronze adds:

| Field | Meaning |
| --- | --- |
| `record_id` | SHA-256 of logical table, input SHA-256, and stable source key |
| `source_filename` | Spark file metadata name |
| `source_sha256` | Digest of the ingested CSV |
| `source_modified_at` | Spark file modification metadata |
| `ingested_at` | UTC ingestion timestamp |
| `ingestion_batch_id` | Deterministic SHA-256 for dataset version, table, and source digest |
| `dataset_version` | Manifest-backed RailPulse dataset version |
| `source_document_sha256` | Failure table only; digest of the official PDF |
| `corrupt_record` | Complete malformed CSV row under permissive parsing |

`record_id` is not a health label or event-time identifier. Duplicate source-derived IDs inside one
input raise `BronzeIngestionError`; they are not silently deduplicated.

## Idempotency and reconciliation

An absent target is written once as Delta. An existing target uses `MERGE` on `record_id` with an
insert-only unmatched branch. After every run, Bronze verifies that every source ID exists in the
target and reports the target count change. A no-op `MERGE` does not create an extra Delta
transaction.

Full-source verification on 2026-09-02 produced:

| Table | Source rows | Target rows after | Inserts on verified rerun |
| --- | ---: | ---: | ---: |
| `bronze.telemetry_raw` | 1,516,948 | 1,516,948 | 0 |
| `bronze.failure_reports_raw` | 4 | 4 | 0 |

The official files produced zero corrupt records. This is a structural Bronze observation, not a
claim that all measurements or failure metadata are semantically valid.

## Tests

Run the deterministic unit and Spark/Delta integration suite inside the configured WSL shell:

```bash
.venv-wsl/bin/python -m pytest
```

Fixtures cover raw numeric-token preservation, source metadata, checksum rejection, malformed-row
retention, duplicate-key rejection, separate failure provenance, first writes, and zero-insert
reruns.
