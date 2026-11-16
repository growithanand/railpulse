# RailPulse

**A predictive-maintenance data platform for metro air-compressor systems**

RailPulse is an end-to-end data engineering and machine-learning portfolio project built with the
[MetroPT-3 dataset](https://archive.ics.uci.edu/dataset/791/metropt%2B3%2B). It takes more than
1.5 million sensor readings from a metro air compressor and turns them into reliable data products,
model experiments, and a Databricks dashboard.

The compressor supports important pneumatic systems on the train. Instead of expecting a person to
inspect millions of readings, RailPulse asks whether changes in the sensor data can warn a
maintenance team before a recorded failure.

The project reached an important and honest conclusion:

> The data platform worked as designed, but the selected model missed the held-out failure and is
> **not production-ready**.

That result is not hidden or retuned away. The main value of RailPulse is the reproducible pipeline,
careful evaluation, and clear evidence showing why the model should not yet be used for real
maintenance decisions.

## Project status

The full path from raw data to the final dashboard has been implemented and verified. The only
remaining presentation task is a final manual visual review of the unpublished dashboard before a
separate decision is made about publishing it.

| Area | Current state |
| --- | --- |
| Source data | Both official inputs are verified by file size and SHA-256 digital fingerprint |
| Bronze | Raw telemetry and failure records loaded into governed Delta tables |
| Silver | Validation, quarantine, quality metrics, and identifier checks completed |
| Gold | Cycles, failure horizons, point-in-time features, and modelling view completed |
| Databricks Jobs | Dependency-gated, serverless workflows deployed and successfully run |
| MLflow | Two model candidates compared in one parent run with two nested runs |
| Final evaluation | One-time held-out test completed; result frozen and reported honestly |
| Dashboard | Six queries validated; one unpublished AI/BI draft with 13 widgets created |

## How RailPulse works

```text
Official telemetry and failure records
                  |
                  v
      File verification and governed storage
                  |
                  v
      Bronze: preserve the original records
                  |
                  v
   Silver: validate, clean, and record quality issues
                  |
                  v
 Gold: build operating cycles, labels, and past-only features
                  |
                  v
      MLflow model comparison and policy selection
                  |
                  v
        One-time held-out evaluation
                  |
                  v
       Databricks decision-support dashboard
```

### Bronze: preserve the source

Bronze is the raw layer. It stores the source values as they arrived, along with file lineage and
deterministic record identifiers. Keeping this layer close to the source makes the pipeline
auditable and prevents silent corrections from being presented as original data.

### Silver: validate the records

Silver converts values into useful data types and applies explicit quality rules. Valid records go
to accepted tables; invalid records go to quarantine tables with a reason. The workflow checks
parsing, sensor ranges, binary values, duplicates, timestamps, and gaps in the telemetry sequence.

For the official data, Silver accepted all 1,516,948 telemetry rows and all four published failure
records. It found no quarantined records, while still recording 354 material forward gaps as quality
evidence.

### Gold: create analysis-ready products

Gold turns individual sensor readings into information that can be used for analysis and modelling:

- `loaded_cycles` describes 15,766 compressor operating cycles.
- `failure_horizons` records whether a failure begins within two hours of a prediction point.
- `feature_snapshots_v2` contains features calculated only from information available at that time.
- `modeling_view_v2` joins the approved features, labels, split, and lineage into one governed table.

The managed modelling view contains 15,766 rows: 15,272 are trainable and 494 are excluded by
documented rules. Among the trainable rows, 22 are positive two-hour failure labels and 15,250 are
negative.

### Jobs, Delta Lake, and Unity Catalog

Databricks Jobs run the layers in the correct order. For example, Silver starts only after Bronze
succeeds, and Gold starts only after its upstream dependency succeeds. The workflows are
unscheduled, so they do not start compute unexpectedly.

Delta Lake provides reliable, schema-aware tables and safe merge behaviour. RailPulse adds stable
identifiers and reconciliation checks so that an unchanged rerun does not duplicate records. Unity
Catalog organizes the Bronze, Silver, and Gold tables and provides a governed location for the
source files.

## Verified data evidence

| Evidence | Result |
| --- | ---: |
| Telemetry records reconciled through Bronze and Silver | 1,516,948 |
| Published failure records preserved | 4 |
| Quarantined telemetry records | 0 |
| Material forward gaps recorded | 354 |
| Compressor operating cycles | 15,766 |
| Trainable modelling rows | 15,272 |
| Positive two-hour labels | 22 |
| Negative trainable labels | 15,250 |

The important point is not only that the counts match. The managed writers were also rerun with
unchanged inputs and inserted zero duplicate rows, which demonstrates that the pipeline is
idempotent and safe to repeat.

## Model development and final result

RailPulse uses chronological splits: earlier data is used for training, a later period for
validation, and July is held back for the final test. Random row splitting is not used because it
could allow future operating behaviour to influence earlier predictions.

Two logistic-regression candidates were compared on the same data. Logistic regression is a simple,
explainable model that returns a probability rather than a fixed yes-or-no answer.

| Candidate | Validation AUPRC |
| --- | ---: |
| Motor-current features | 0.011954 |
| Motor-current plus cycle context | 0.008632 |

The simpler motor-current candidate was retained because it performed better on validation. AUPRC
summarizes how well a model separates rare positive cases from many negative cases; higher is
better. Because validation contained only five positive rows, this result was treated as a
development decision, not proof of production performance.

Five alert policies were then compared using validation data only. The selected policy required two
consecutive cycles above a validation-derived threshold. It detected the one validation failure,
gave 27 minutes 41 seconds of warning, and produced 33 false alert episodes across 28 operating
days.

After the model and alert policy were frozen, the held-out July period was opened exactly once. The
workflow had no automatic retries and returned aggregate evidence only.

| Metric | Validation | Held-out test |
| --- | ---: | ---: |
| Rows | 4,730 | 4,217 |
| Positive rows | 5 | 13 |
| AUPRC | 0.011954 | 0.001671 |
| Accepted failure events | 1 | 1 |
| Detected failure events | 1 | 0 |
| Missed failure events | 0 | 1 |
| False alert episodes | 33 | 73 |
| False alerts per operating day | 1.1786 | 1.1774 |

The false-alert rate stayed close to 1.18 per day, but the model missed the held-out failure. The
result is therefore frozen as `NOT_PRODUCTION_READY`. July will not be rerun or used for retuning.

## Databricks dashboard

The project includes a source-controlled Databricks AI/BI dashboard for decision support and
portfolio demonstration. Its six read-only datasets show:

- the final readiness decision and held-out metrics;
- validation versus held-out performance;
- managed Silver and Gold product counts;
- telemetry quality evidence;
- daily compressor-cycle activity; and
- monthly accepted-failure summaries.

The current draft has one page and 13 widgets. It is unpublished, unscheduled, and exposes no
row-level probabilities, record identifiers, or failure-report text. The dashboard deliberately
shows `NOT_PRODUCTION_READY` before any favourable validation metric. Its final manual visual review
is still pending because the Databricks console does not support the automated browser control used
during development.

## Design decisions that matter

- **Past-only features:** every model feature uses information available before the prediction time.
- **Time-based evaluation:** train, validation, and test periods follow calendar time.
- **Accepted and quarantined outputs:** quality problems are recorded instead of silently discarded.
- **Deterministic identities:** rerunning unchanged data does not create duplicate rows.
- **A sealed final test:** model and alert choices were frozen before July was opened.
- **Honest reporting:** the failed held-out result is retained instead of being optimized away.
- **Controlled compute:** managed workflows and the dashboard have no automatic schedule.

## Technology

- Python 3.11+
- PySpark 4.2 and Delta Lake 4.4
- Databricks Asset Bundles and serverless Jobs
- Unity Catalog and Databricks SQL
- MLflow experiment tracking
- pytest and Ruff

Spark is used to demonstrate scalable and governed engineering patterns. The dataset can fit on one
machine, so the project does not claim that distributed compute is required for 1.5 million rows.

## Repository structure

```text
configs/             Environment-independent settings
dashboards/          Source-controlled Databricks AI/BI dashboard definition
data/                Data placement guide; raw data is ignored by Git
docs/                Contracts, decisions, run evidence, plan, and project status
resources/           Databricks job and infrastructure definitions
sql/                 Read-only analytical and dashboard queries
src/railpulse/       Reusable pipeline, feature, model, and evaluation code
tests/               Unit and Spark/Delta integration tests
```

Reusable transformations live in Python modules or checked-in SQL rather than hidden notebook state.

## Run the project locally

Raw telemetry is not stored in Git. Download MetroPT-3 from UCI and follow
[the data placement guide](data/README.md). The repository contains the reviewed four-row failure
reference and checksum manifest.

Create a virtual environment and run the non-Spark checks:

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install --upgrade pip
.venv\Scripts\python -m pip install -e ".[dev,spark,tracking]"
.venv\Scripts\python -m pytest -m "not spark"
.venv\Scripts\python -m ruff check .
.venv\Scripts\python -m ruff format --check .
```

The verified local Spark/Delta path uses Ubuntu WSL, Python 3.12, and JDK 21. Native Windows Spark
is not the verified integration-test path because its Hadoop layer requires an additional Windows
helper.

See [Bronze ingestion](docs/bronze_ingestion.md) for the full WSL setup and materialization commands.

## Limitations and next steps

Only four published failures are available, so RailPulse is a case study rather than proof that the
model will generalize to a real fleet. It does not claim to prevent failures, reduce downtime, or
save maintenance cost.

The next modelling experiment should add versioned pressure, temperature, and operating-context
features and reserve a new future evaluation period. It must not change the frozen July result. The
immediate portfolio task is to complete the manual dashboard review and then decide separately
whether to publish it.

## Documentation

- [Current project status](docs/project_status.md)
- [Portfolio demonstration guide](docs/portfolio_demo.md)
- [Architecture decisions](docs/decisions.md)
- [Dataset manifest](docs/dataset_manifest.json)
- [Data contract](docs/data_contract.md)
- [Managed Silver run evidence](docs/managed_silver_first_run.md)
- [Managed modelling-view evidence](docs/managed_modeling_view_v2_first_run.md)
- [Model candidate comparison](docs/managed_model_candidate_comparison_first_run.md)
- [Validation alert-policy selection](docs/development_alert_policy_selection.md)
- [Final sealed-test result](docs/managed_sealed_test_evaluation_result.md)
- [Maintenance dashboard contract](docs/maintenance_dashboard_contract.md)

## Dataset and licence

RailPulse uses the official UCI MetroPT-3 dataset, ID 791, DOI
[10.24432/C5VW3R](https://doi.org/10.24432/C5VW3R), under CC BY 4.0. Raw data and generated Delta
storage are intentionally excluded from Git. Source hashes and provenance are recorded in the
[dataset manifest](docs/dataset_manifest.json).
