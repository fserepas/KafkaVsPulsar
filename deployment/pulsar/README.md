# Pulsar pipeline

Pulsar 4.0.0 runs standalone with the supplied MQTT 4.0.0.10 handler, functions worker enabled and container-resolvable advertised address. A schema-enrichment function is used for every Pulsar run; R12 adds the temperature processing function. The `postgres`/`mysql` profile selects one database.

Sink JSON templates are valid YAML and are rendered with the local `.env` password into an ignored per-trial mount. Their NARs contain their JDBC drivers. The source topic is MQTT bytes, explicitly decoded through the packaged `SensorReadingTempJsonSerde`; enriched/processed output remains schema-aware.

Start with the root README and `docs/EXECUTION.md`; `scripts/experiment.py` deploys and checks the selected function/sink instances.
