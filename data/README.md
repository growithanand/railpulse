# RailPulse data directory

Only this placement guide and the compact, attributed failure-event reference are versioned. Raw,
processed, Delta, and checkpoint contents remain ignored.

Planned local layout:

```text
data/
├── raw/          Unmodified MetroPT-3 telemetry and separate failure reports
├── reference/    Small licensed source metadata safe to version
├── processed/    Non-Delta local inspection output, if required
├── delta/        Local Bronze, Silver, and Gold Delta storage
└── checkpoints/  Structured Streaming checkpoints
```

The verified source is UCI dataset 791. Place the official archive and extracted members under
`data/raw/source/metropt3-uci-791/`; never commit them. Artifact hashes and provenance are recorded
in `docs/dataset_manifest.json`.

`reference/metropt3_failure_events.csv` is a compact CC BY 4.0 transcription of the official PDF
failure table. It deliberately preserves duplicate report identifiers and ambiguous maintenance
text. Small deterministic test data belongs in `tests/fixtures/` and must be clearly synthetic or
derived under an explicitly documented license.
