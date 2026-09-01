# RailPulse

**Predictive Maintenance and Analytics Platform for Metro Compressor Systems**

RailPulse is an industrial data and machine-learning portfolio project based on the
MetroPT-3 compressor telemetry dataset. Its central question is:

> Can abnormal compressor behaviour be identified early enough to support maintenance
> intervention—ideally at least two hours before a recorded failure—without producing an
> unacceptable number of false alarms?

## Current status

The Phase 1 repository scaffold is implemented and locally verified. Official dataset verification
is the next phase; all data pipelines, models, metrics, alerts, dashboards, and Databricks resources
are still planned. No performance or maintenance-impact claims have been established.

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

This is a production-pattern demonstration. Approximately 1.5 million one-second observations do
not inherently require distributed computing; Spark is used to demonstrate scalable, incremental,
and Databricks-compatible engineering practices.

## Implemented now

- A Python `src` package with clear ingestion, validation, feature, model, evaluation, and
  monitoring boundaries.
- Checked-in TOML defaults loaded through a typed, validated configuration module.
- Deterministic package and configuration tests.
- Data and artifact exclusion rules that allow only `data/README.md` to be versioned under
  `data/`.
- A minimal Databricks Asset Bundle entry point. It has not been deployed or CLI-validated.
- Durable project planning, status, and architecture-decision documentation.

## Planned stack

Python, Apache Spark, PySpark, Spark SQL, Delta Lake, Databricks, MLflow, pytest, Ruff, and GitHub
Actions will be added where each is justified. Structured Streaming will replay historical files;
it will not be described as a live train connection.

Spark, Delta Lake, and Databricks dependency versions are intentionally not pinned yet. The current
machine has Python and Java but no Spark or Databricks CLI, so compatibility will be selected after
the local and target Databricks runtimes are verified.

## Quick start

Python 3.11 or newer is required for the current package-only phase.

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install --upgrade pip
.venv\Scripts\python -m pip install -e ".[dev]"
.venv\Scripts\python -m pytest
.venv\Scripts\python -m ruff check .
.venv\Scripts\python -m ruff format --check .
```

The data pipeline does not exist yet. Do not infer pipeline outputs from the configuration names.

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

## Dataset and evaluation guardrails

The official UCI MetroPT-3 source, license, checksum, schema, units, time range, and published
failure/maintenance intervals must be independently verified before ingestion. Raw telemetry and
failure reports remain logically separate and are never committed.

Evaluation will be chronological and failure-event-aware. Future-looking windows, centered
features, random row-level splits, full-dataset normalization, and test-set threshold selection are
prohibited. Sparse failure events will be reported honestly; inconclusive results are acceptable.

## Documentation

- [Project plan](docs/project_plan.md)
- [Project status](docs/project_status.md)
- [Architecture decisions](docs/decisions.md)
- [Data placement](data/README.md)

Data contracts, a data dictionary, an evaluation protocol, dashboard instructions, a demo script,
and evidence-based résumé bullets will be added only when their supporting phases are implemented.
