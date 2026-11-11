# Runtime dependencies

The deployments require the matching `runtime-assets.zip` archive. The full ZIP includes it in the project root. For a source-only download, place the companion archive in that same folder.

From the project root:

```bash
python3 scripts/import-runtime-assets.py runtime-assets.zip
```

The installer verifies all 109 files against the sizes and SHA-256 checksums in `assets/runtime-manifest.json` before installing them into `runtime/`. The archive includes the Kafka JDBC/MQTT/FromJson connectors and their notices, the JMX exporter, the Pulsar MQTT protocol handler, JDBC sink NARs and supplemental drivers. Build the custom Java application JARs with `./scripts/build.sh`.

The installed runtime files are ignored by Git. Third-party dependencies retain their own licenses; see `THIRD_PARTY_NOTICES.md`.
