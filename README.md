# RailPulse

**Predictive Maintenance and Analytics Platform for Metro Compressor Systems**

RailPulse is an industrial data and machine-learning portfolio project based on the
MetroPT-3 compressor telemetry dataset. Its central question is:

> Can abnormal compressor behaviour be identified early enough to support maintenance
> intervention—ideally at least two hours before a recorded failure—without producing an
> unacceptable number of false alarms?

## Current status

The repository scaffold, official MetroPT-3 data contract, Bronze Delta ingestion, and Silver
telemetry validation are implemented and locally verified. Accepted and quarantined telemetry can
also be persisted to separate idempotent Silver Delta tables, as can accepted and quarantined failure
events. Telemetry quality summaries also have a versioned, idempotent Silver output. The first Gold
transformation derives and profiles provisional loaded-operation cycles, and a reproducible build
command has materialized all 15,766 segments to local Delta with a verified zero-change rerun. All
four start/stop censoring combinations and their duration tails are exposed through a tested,
read-only SQL inspection. All later Gold tables, models, alerts, dashboards, and Databricks
resources are still planned. No performance or maintenance-impact claims have been established.

See [the project status](docs/project_status.md) for verified environment details and
[the project plan](docs/project_plan.md) for delivery phases.

## Intended user and outcome

RailPulse is designed for a maintenance or reliability engineer who needs to inspect equipment
condition, review alert episodes, compare alerts with recorded failures, measure useful warning
lead time and false-alarm frequency, and compare maintenance policies.

The planned flow is:

```text
Official MetroPT-3 telemetry + separately documented failure reports
                              |
                              v
                     Bronze Delta tables
                  raw values + source metadata
                              |
                              v
                     Silver Delta tables
              validation + quarantine + quality metrics
                              |
                              v
                      Gold data products
          cycles + temporal features + horizons + reliability KPIs
                              |
                              v
               baseline/anomaly scores -> alert episodes
                              |
                              v
                Databricks SQL decision-support dashboard
```

This is a production-pattern demonstration. Approximately 1.5 million records at a nominal
10-second cadence do not inherently require distributed computing; Spark is used to demonstrate
scalable, incremental, and Databricks-compatible engineering practices.

## Implemented now

- A Python `src` package with clear ingestion, validation, feature, model, evaluation, and
  monitoring boundaries.
- Checked-in TOML defaults loaded through a typed, validated configuration module.
- Deterministic package and configuration tests.
- A checksum-backed official dataset manifest, sensor dictionary, failure-event reference, and
  streaming CSV contract inspector.
- Explicit all-string Bronze schemas, source/checksum metadata, corrupt-record capture, deterministic
  identifiers, Delta `MERGE` idempotency, and row reconciliation.
- Locally materialized `bronze.telemetry_raw` and `bronze.failure_reports_raw` Delta tables; generated
  table storage remains ignored.
- Typed Silver telemetry with explicit parsing, domain, range, duplicate, and timestamp-sequence
  quality reasons, plus separate accepted and quarantine Delta outputs.
- Typed Silver failure events with structural rejection reasons and separate accepted and quarantine
  Delta outputs.
- Versioned telemetry quality summaries with reconciled row, gap, and rejection-reason counts.
- Causal loaded-cycle boundaries, deterministic segment identifiers, cycle-level aggregation, and
  a reproducible full-source profile using the documented `DV_eletric` operating signal.
- Monotonic `gold.loaded_cycles` Delta merge semantics that insert new IDs, close right-censored
  cycles without changing their IDs, and reject regressive or incompatible snapshots.
- A source-bound `loaded-cycle-build-v1` command with reconciled full-source first-write and rerun
  evidence for the local `gold.loaded_cycles` table.
- A tested Spark SQL inspection that reconciles censoring-group counts and descriptive duration
  tails without assigning health or failure meaning.
- Data and artifact exclusion rules that allow only the placement guide and vetted reference
  metadata to be versioned under `data/`.
- A minimal Databricks Asset Bundle entry point. It has not been deployed or CLI-validated.
- Durable project planning, status, and architecture-decision documentation.

## Runtime stack

Bronze and the current Silver pipeline use Python, PySpark 4.2.0, Apache Spark 4.2.0, Delta Lake
4.4.0, pytest, and Ruff. Later phases will add Spark SQL, Databricks resources, MLflow, Structured
Streaming, and GitHub Actions where each is justified. Streaming will replay historical files; it
will not be described as a live train connection.

The local Spark/Delta integration is verified in Ubuntu WSL with Python 3.12 and Eclipse Temurin JDK
21. Native Windows Spark is not the verified path because Hadoop requires a separate Windows helper.
Databricks deployment and runtime compatibility have not been tested.

## Quick start

Python 3.11 or newer is required. Package-only checks can run in a normal virtual environment:

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install --upgrade pip
.venv\Scripts\python -m pip install -e ".[dev,spark]"
.venv\Scripts\python -m pytest -m "not spark"
.venv\Scripts\python -m ruff check .
.venv\Scripts\python -m ruff format --check .
```

Spark/Delta integration and ingestion are run inside Ubuntu WSL with a supported JDK 21. From the
repository mounted in WSL:

```bash
python3 -m venv .venv-wsl
.venv-wsl/bin/python -m pip install --upgrade pip
.venv-wsl/bin/python -m pip install -e ".[dev,spark]"
export JAVA_HOME=/path/to/a/jdk-21
export PYSPARK_PYTHON="$PWD/.venv-wsl/bin/python"
export PYSPARK_SUBMIT_ARGS="--driver-memory 3g --conf spark.driver.extraJavaOptions=-Djdk.lang.Process.launchMechanism=VFORK pyspark-shell"
.venv-wsl/bin/python -m pytest
```

The local JDK and virtual environment belong under ignored `.tools/` and `.venv-wsl/` when kept in
the project. See [Bronze ingestion](docs/bronze_ingestion.md) for source placement, execution, table
locations, idempotency behavior, and verified counts.

To reproduce the complete-file source inspection after downloading the official CSV into ignored
raw storage:

```powershell
.venv\Scripts\python -m railpulse.validation.metropt3 `
  "data\raw\source\metropt3-uci-791\MetroPT3(AirCompressor).csv"
```

After source inspection, materialize or safely reconcile both Bronze tables:

```bash
.venv-wsl/bin/python -m railpulse.ingestion.bronze --master "local[4]"
```

Then derive, reconcile, and persist the full-source Gold cycle table:

```bash
.venv-wsl/bin/python -m railpulse.features.cycle_build --master "local[4]"
```

Inspect censoring categories and observed-duration tails without modifying Gold data:

```bash
.venv-wsl/bin/python -m railpulse.features.cycle_inspection --master "local[2]"
```

## Repository layout

```text
configs/             Checked-in, environment-independent defaults
data/                Placement instructions; actual data is ignored
docs/                Plans, decisions, contracts, protocols, and status
src/railpulse/       Reusable production code
tests/               Deterministic unit and integration tests
sql/                 Spark/Databricks SQL introduced with data products
notebooks/           Thin exploration and demonstration entry points
dashboards/          Dashboard queries and specifications
resources/           Databricks deployment resources
```

Reusable transformations belong in Python modules or SQL, not hidden notebook state.

## Verified dataset and evaluation guardrails

RailPulse uses the [official UCI MetroPT-3 dataset](https://archive.ics.uci.edu/dataset/791/metropt%2B3%2B),
ID 791 and DOI [10.24432/C5VW3R](https://doi.org/10.24432/C5VW3R), under CC BY 4.0. The retrieved
archive, CSV, and PDF are identified by SHA-256 in the
[dataset manifest](docs/dataset_manifest.json). Raw artifacts are never committed.

The actual CSV contains 1,516,948 rows, 15 sensor signals, and timestamps from 2020-02-01 through
2020-09-01. Its cadence is nominally 10 seconds with jitter and material gaps. Published failures
are stored separately from telemetry and retain unresolved source ambiguities.

Evaluation will be chronological and failure-event-aware. Future-looking windows, centered
features, random row-level splits, full-dataset normalization, and test-set threshold selection are
prohibited. Sparse failure events will be reported honestly; inconclusive results are acceptable.

## Documentation

- [Project plan](docs/project_plan.md)
- [Project status](docs/project_status.md)
- [Architecture decisions](docs/decisions.md)
- [Data placement](data/README.md)
- [Dataset manifest](docs/dataset_manifest.json)
- [Data contract](docs/data_contract.md)
- [Data dictionary](docs/data_dictionary.md)
- [Bronze ingestion](docs/bronze_ingestion.md)
- [Full-source loaded-cycle profile](docs/cycle_profile.md)
- [Gold loaded-cycle build](docs/gold_cycle_build.md)
- [Gold loaded-cycle inspection](docs/gold_cycle_inspection.md)

An evaluation protocol, dashboard instructions, a demo script, and evidence-based résumé bullets
will be added only when their supporting phases are implemented.
