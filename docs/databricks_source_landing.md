# Databricks source-landing contract

## Status

`databricks-source-landing-v1` is defined in code and bundle configuration but has not been
deployed. No Volume, directory, or file was created by this increment.

## Purpose

RailPulse needs a governed file boundary between the ignored local source artifacts and managed
Bronze tables. A Unity Catalog managed Volume provides that boundary without treating CSV files as
tables or placing data in a user-specific workspace directory.

The planned development object is:

```text
workspace.railpulse_bronze.source
```

Its runtime root is:

```text
/Volumes/workspace/railpulse_bronze/source
```

Databricks CLI file commands require the same path with the `dbfs:` scheme. Spark and Python jobs
will use the `/Volumes/...` form.

## Versioned source layout

Source artifacts are isolated below the manifest-backed dataset version rather than overwritten in
one mutable directory:

```text
/Volumes/workspace/railpulse_bronze/source/
└── metropt3/
    └── uci-791-aab991a970e5/
        ├── telemetry/
        │   └── MetroPT3(AirCompressor).csv
        └── reference/
            └── metropt3_failure_events.csv
```

The telemetry filename preserves the official archive member name. The failure reference is the
reviewed four-row transcription tracked in `data/reference/`. The official PDF is provenance for
that transcription but is not a runtime ingestion input.

The expected dataset version, filenames, sizes, and SHA-256 values remain authoritative in
`docs/dataset_manifest.json`. Uploading a file does not make it trusted: the first managed Bronze
job must verify its identity before reading or writing any table.

## Code contract

`DatabricksSourceLanding` combines a validated `CatalogNamespace`, a lowercase dataset version, and
a validated Volume name. It produces the qualified Unity Catalog object name, the Volume root, both
source paths, and the CLI form of a Volume path. Unsafe identifiers, traversal-like dataset
versions, and non-Volume CLI paths fail before a job can use them.

The default Volume name is configurable through the bundle variable `source_volume`; the physical
path remains environment-specific while the layout below it is versioned and deterministic.

## Planned workspace mutation

The bundle declares exactly one managed Volume resource with deletion protection:

```text
workspace.railpulse_bronze.source
```

A future reviewed deployment may create that empty Volume. File upload is a separate action and is
not performed by bundle deployment. Before deployment, the bundle plan must be inspected for
unrelated changes. After deployment, the Catalog API and an empty-directory listing must confirm
the object before either source file is uploaded.

Read-only verification on 2026-10-02 found no existing Volume in `workspace.railpulse_bronze`.
Strict bundle validation returned `Validation OK!`, and the plan reported one Volume creation, two
existing job updates, and no deletion. The job updates must remain in the reviewed deployment scope;
no plan has been applied.

Required upload access is `USE CATALOG`, `USE SCHEMA`, and `WRITE VOLUME`. Credentials and OAuth
profiles remain machine-local. Local source paths, workspace URLs, user identities, and deployment
state must not be committed.

## Deferred ingestion boundary

This contract does not create Bronze tables or choose batch versus streaming ingestion. The next
job increment will:

1. accept the two contracted Volume paths as managed parameters;
2. verify file size and SHA-256 against the dataset manifest;
3. preserve raw tokens and source metadata;
4. write fully qualified Bronze tables through the shared catalog namespace;
5. reconcile the first write and an identical rerun.

The 218 MB telemetry file does not require distributed computing. Spark remains appropriate here as
a production-pattern implementation that will share semantics with later incremental replay.

## References

- [Databricks Unity Catalog Volumes](https://docs.databricks.com/aws/en/volumes/)
- [Work with files in Volumes](https://docs.databricks.com/aws/en/volumes/volume-files)
- [Declarative Automation Bundles resources](https://docs.databricks.com/aws/en/dev-tools/bundles/resources)
