# RailPulse data directory

Only this placement guide is versioned. All other contents of `data/` are ignored.

Planned local layout:

```text
data/
├── raw/          Unmodified MetroPT-3 telemetry and separate failure reports
├── processed/    Non-Delta local inspection output, if required
├── delta/        Local Bronze, Silver, and Gold Delta storage
└── checkpoints/  Structured Streaming checkpoints
```

Do not place data in the repository until the official UCI source, license, version, schema, and
failure/maintenance records have been verified in the data-contract phase. Do not commit raw or
processed records. Small deterministic test data belongs in `tests/fixtures/` and must be clearly
synthetic or derived under an explicitly documented license.
