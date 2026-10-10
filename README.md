# RailPulse

**Predictive maintenance and analytics for metro compressor systems**

RailPulse is an end-to-end data engineering and machine-learning portfolio project built on the
[MetroPT-3](https://archive.ics.uci.edu/dataset/791/metropt%2B3%2B) air-compressor dataset. It turns
1.5 million telemetry observations and separately published failure reports into governed Delta
tables, point-in-time features, chronological modelling data, and an eventual maintenance dashboard.

The project asks a practical question:

> Can abnormal compressor behaviour be identified early enough to support maintenance
> intervention—ideally at least two hours before a recorded failure—without creating an
> unacceptable false-alarm burden?

No final model-performance or maintenance-impact claim is made yet. The current emphasis is the
reproducible data foundation and leakage-safe evaluation design needed to make such claims credible.

## Current status

The complete Bronze-to-Gold engineering path is verified locally on the official data. The
Databricks foundation is also live: the package runs on serverless compute, the three Unity Catalog
schemas exist, both source files are stored in a governed Volume, and a managed preflight job has
reproduced their manifest-backed sizes and SHA-256 hashes.

A two-task managed Bronze job is implemented, tested, and deployed. Its first task verifies the
landed files; its second task can write the two allowlisted catalog tables only after verification
succeeds. Its first successful managed run created and reconciled 1,516,948 telemetry rows and four
failure-reference rows. An unchanged managed rerun then matched every existing record, inserted
zero rows, and preserved both table counts.

| Area | Verified state | Next boundary |
| --- | --- | --- |
| Source governance | Manifest, immutable paths, checksums, managed Volume | Complete |
| Local pipeline | Verified through Gold v1/v2 snapshots and modelling-view profile | Complete |
| Databricks runtime | Serverless wheel execution and source preflight | Complete |
| Managed Bronze | First load and zero-insert rerun reconciled both source-aligned tables | Complete |
| Managed Silver/Gold | Local contracts already exist | Adapt storage to catalog tables |
| Modelling | Development baselines and drift evidence | Select and evaluate a final candidate |
| Decision support | Maintenance use case and KPI goals identified | Build KPI tables and SQL dashboard |

See [project status](docs/project_status.md) for the detailed implementation record and
[project plan](docs/project_plan.md) for the delivery sequence.

## Architecture

```text
Official telemetry CSV + published failure reports
                         |
                         v
       checksum manifest + governed source Volume
                         |
                         v
                 Bronze Delta tables
        raw strings, corrupt rows, file lineage,
           deterministic record and batch IDs
                         |
                         v
                 Silver Delta tables
      typed values, accepted/quarantine outputs,
       quality reasons, gaps, duplicate detection
                         |
                         v
                  Gold data products
       loaded cycles, point-in-time features,
       feature snapshots v1/v2, failure horizons
                         |
                         v
          chronological modelling and MLflow
                         |
                         v
        alert episodes + event-level evaluation
                         |
                         v
        Databricks SQL maintenance dashboard
```

The dataset is small enough to process on one machine. Spark and Databricks are used deliberately
to demonstrate scalable, incremental, governed patterns rather than to imply that distributed
compute is required for 1.5 million rows.

## What the project demonstrates

### Governed ingestion

- Exact source identity through versioned paths, byte sizes, and SHA-256 checksums.
- Separate provenance for telemetry, the failure transcription, and its official source document.
- All-string Bronze schemas that preserve raw tokens and malformed rows before interpretation.
- Deterministic record and batch identifiers with insert-only Delta merges and count reconciliation.
- Rerunnable writes: an identical source produces zero new rows.

### Data quality and transformation

- Explicit parsing, analogue-envelope, binary-domain, duplicate, and event-time sequence checks.
- Separate accepted and quarantine tables with record-level rejection reasons.
- Material-gap detection without incorrectly labelling measurements after a gap as invalid.
- Causal loaded-cycle segmentation with left- and right-censoring retained as data, not discarded.
- Monotonic Gold merges that prevent settled cycle history from being silently rewritten.

### Point-in-time features and evaluation

- Strict past-only feature windows ending at each cycle prediction timestamp.
- Label-independent feature eligibility and immutable feature snapshots.
- A two-hour cycle-to-failure horizon with explicit null states for incomplete or in-failure cases.
- Calendar-based train, validation, and test periods; random row splitting is prohibited.
- Tests that prove future telemetry cannot change historical feature values.

### Platform engineering

- A Python `src` package with unit and Spark/Delta integration tests.
- Databricks Asset Bundle resources, Python-wheel tasks, serverless execution, and Unity Catalog.
- Validated catalog/schema identifiers and fully qualified managed table names.
- Read-only preflight tasks separated from provisioning, upload, deployment, and table mutation.
- Generated data, Delta logs, credentials, environment files, and workspace state excluded from Git.

## Verified complete-source evidence

| Evidence | Verified result |
| --- | ---: |
| Telemetry rows reconciled through Bronze and Silver | 1,516,948 |
| Published failure records retained | 4 |
| Provisional loaded cycles | 15,766 |
| Available 15-minute motor-current features | 15,703 |
| Feature-eligible cycles | 15,418 |
| Material-window-gap exclusions | 285 |
| Missing prediction boundaries | 63 |
| Trainable modelling-view rows | 15,413 |
| Positive two-hour cycle labels | 22 |
| Negative trainable cycle labels | 15,391 |
| Excluded modelling-view rows | 353 |
| Failure events represented by positive cycles | 3 of 4 |

Local first-write and rerun checks materialized 15,766 rows in each versioned Gold feature-snapshot
table. Identical reruns inserted zero rows and left the existing snapshots unchanged. Generated
Delta storage is intentionally not committed.

## Modelling findings so far

The modelling work is intentionally conservative:

- A June-validation/July-test chronological split was selected from coverage feasibility alone.
  The held-out test period has not been used to choose features or thresholds.
- A training-only robust motor-current deviation baseline exposed substantial temporal or
  operating-regime drift and was retained as a documented benchmark failure.
- Five directional low-current thresholds were rejected because their validation false-alarm burden
  remained operationally weak.
- A pressure-balance feature showed substantial positive/negative overlap and was not promoted.
- Loaded-cycle duration context showed consistent development-period signal and was added through a
  separate immutable version 2 feature snapshot instead of rewriting version 1.

These negative and mixed results are part of the portfolio evidence: the project records why a
candidate was rejected rather than turning limited failure data into an inflated accuracy claim.

## Databricks progression

Completed managed steps:

1. Validate the bundle and packaged wheel on serverless compute.
2. Provision separate Bronze, Silver, and Gold schemas idempotently.
3. Create a deletion-protected managed source Volume.
4. Upload the two contracted inputs without overwrite.
5. Independently verify remote sizes and downloaded hashes.
6. Run the deployed source-preflight job successfully without creating tables.
7. Implement and test an allowlisted Unity Catalog Bronze writer.
8. Define and validate an unscheduled two-task Bronze workflow with a mandatory preflight gate.
9. Deploy only that workflow and verify its empty run history without creating tables.
10. Run the workflow successfully and reconcile 1,516,948 telemetry rows and four failure rows.
11. Redeploy from committed source and prove an identical rerun inserts zero rows.

The managed workflow now owns:

- `<catalog>.<bronze_schema>.telemetry_raw`
- `<catalog>.<bronze_schema>.failure_reports_raw`

The next controlled step is adapting the verified Silver validation and quality outputs to
allowlisted Unity Catalog tables, beginning with a tested catalog-backed persistence boundary.

## Roadmap to the portfolio demonstration

1. **Completed:** materialize and reconcile the two managed Bronze tables, including a zero-insert
   rerun.
2. Adapt the verified Silver accepted, quarantine, and quality outputs to Unity Catalog.
3. Materialize Gold cycles, feature snapshots, horizons, and modelling views in Databricks.
4. Track final candidate training and parameters with MLflow while keeping the test period sealed.
5. Convert cycle scores into alert episodes and evaluate warning lead time and false-alarm burden at
   the failure-event level.
6. Publish dashboard-ready reliability KPIs and a Databricks SQL dashboard for maintenance users.
7. Add a short reproducible demo path and evidence-based résumé bullets.

## Technology

- Python 3.11+
- PySpark / Apache Spark 4.2
- Delta Lake 4.4
- Spark SQL and Databricks SQL
- Databricks Asset Bundles, serverless jobs, Volumes, and Unity Catalog
- pytest and Ruff
- MLflow planned for final experiment tracking

Local Spark/Delta execution is verified in Ubuntu WSL with a supported JDK 21. Native Windows Spark
is not the verified path because its Hadoop layer requires an additional Windows helper binary.

## Quick start

Raw data is not stored in Git. Download MetroPT-3 from UCI and place the official CSV as described
in [data/README.md](data/README.md). The repository already contains the vetted four-row failure
reference and the checksum manifest.

Package checks can run in a normal virtual environment:

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install --upgrade pip
.venv\Scripts\python -m pip install -e ".[dev,spark]"
.venv\Scripts\python -m pytest -m "not spark"
.venv\Scripts\python -m ruff check .
.venv\Scripts\python -m ruff format --check .
```

Spark/Delta integration is run inside WSL with JDK 21:

```bash
python3 -m venv .venv-wsl
.venv-wsl/bin/python -m pip install --upgrade pip
.venv-wsl/bin/python -m pip install -e ".[dev,spark]"
export JAVA_HOME=/path/to/jdk-21
export PYSPARK_PYTHON="$PWD/.venv-wsl/bin/python"
export PYSPARK_SUBMIT_ARGS="--driver-memory 3g pyspark-shell"
.venv-wsl/bin/python -m pytest
```

Core local materialization commands:

```bash
.venv-wsl/bin/python -m railpulse.ingestion.bronze --master "local[4]"
.venv-wsl/bin/python -m railpulse.features.cycle_build --master "local[4]"
.venv-wsl/bin/python -m railpulse.features.feature_snapshot_build --master "local[4]"
.venv-wsl/bin/python -m railpulse.features.feature_snapshot_v2_build --master "local[4]"
```

The remaining profile commands and their expected evidence are linked from
[project status](docs/project_status.md).

## Repository layout

```text
configs/             Environment-independent local defaults
data/                Placement guide and vetted failure reference; raw data ignored
docs/                Contracts, decisions, profiles, project plan, and status
resources/           Databricks jobs and managed infrastructure definitions
src/railpulse/       Reusable ingestion, validation, feature, model, and evaluation code
tests/               Unit and Spark/Delta integration tests
sql/                 Checked-in analytical SQL
```

Reusable transformations belong in Python modules or checked-in SQL, not hidden notebook state.

## Dataset and evaluation guardrails

RailPulse uses the official UCI MetroPT-3 dataset, ID 791, DOI
[10.24432/C5VW3R](https://doi.org/10.24432/C5VW3R), under CC BY 4.0. The retrieved archive, CSV, and
source PDF are identified in [the dataset manifest](docs/dataset_manifest.json). Raw artifacts are
never committed.

The telemetry covers 2020-02-01 through 2020-09-01 at a nominal 10-second cadence with jitter and
material gaps. Failure reports remain separate from telemetry, and unresolved source ambiguities
are preserved.

Evaluation is chronological and failure-event-aware. Future-looking windows, centered features,
random row-level splits, full-dataset normalization, and test-set threshold selection are
prohibited. With only four published failures, conclusions must be presented as case-study evidence
rather than precise estimates of generalization or maintenance impact.

## Key documentation

- [Project status](docs/project_status.md)
- [Project plan](docs/project_plan.md)
- [Architecture decisions](docs/decisions.md)
- [Dataset manifest](docs/dataset_manifest.json)
- [Data contract](docs/data_contract.md)
- [Bronze ingestion](docs/bronze_ingestion.md)
- [Gold cycle build](docs/gold_cycle_build.md)
- [Feature snapshot v1](docs/feature_snapshot_contract.md)
- [Feature snapshot v2](docs/feature_snapshot_v2_contract.md)
- [Evaluation protocol](docs/evaluation_protocol.md)
- [Chronological split evidence](docs/chronological_split_profile.md)
- [Engineering baseline evidence](docs/engineering_baseline_profile.md)
- [Directional baseline evidence](docs/directional_baseline_profile.md)
