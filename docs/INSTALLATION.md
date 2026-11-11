# Installation and dependency baseline

Use JDK 17+, Maven 3.8.6+, Python 3.10+ and Docker with Compose v2. Bash is needed for `.sh` helpers; Windows users can use WSL2/Git Bash or run the equivalent `mvn -B clean verify` command. The runner/importer require only Python's standard library.

From the project root (the folder containing `README.md`):

```bash
cp .env.example .env
python3 scripts/import-runtime-assets.py runtime-assets.zip
mvn -B clean verify
```

The importer verifies every size/SHA-256 against `assets/runtime-manifest.json` before writing assets. It installs into ignored `runtime/` paths. Alternatively use `--url` with the exact companion HTTPS asset URL from the same tagged release. The dependency archive must match `assets/runtime-manifest.json`.

| Module | Shaded JAR |
| --- | --- |
| MQTT generator | `modules/mqtt-load-generator/target/MqttLoadGenerator-1.0-SNAPSHOT.jar` |
| Kafka Streams | `modules/kafka-streams/target/KafkaStreamsTemperatureProcessor-1.0-SNAPSHOT.jar` |
| Pulsar functions | `modules/pulsar-functions/target/PulsarFunction-1.0-SNAPSHOT.jar` |

Dockerfiles reference these exact outputs. The runner builds generator/Streams application images from them as needed. It never runs the legacy compiled KafkaProject/PulsarProject tools from the uploaded archives: pipeline persistence and SQL-based measurement are provided by the included connectors/databases.

## Baseline versions

| Component | Supplied/configured version |
| --- | --- |
| Kafka, ZooKeeper, Connect and Schema Registry images | Confluent Platform 7.7.0 |
| Pulsar image / Functions API | 4.0.0 |
| Pulsar MQTT protocol handler | 4.0.0.10 |
| Kafka Connect JDBC / MQTT | 10.8.0 / 1.7.3 |
| FromJson SMT package | jcustenborder JSON Schema 0.2.5 |
| PostgreSQL / MySQL images | 14-alpine / 8.0 |
| Java compilation target | 17 |
| Kafka Streams library / Confluent SerDe | 3.6.0 / 7.9.0 |
| Jackson / Gson / Paho | 2.16.0 / 2.8.9 / 1.2.5 |
| ksqlDB, Mosquitto, monitoring images | Floating tags in supplied setup; configurable in `.env` |

Pulsar's sink NARs carry their own drivers. The companion preserves additionally supplied driver JARs for provenance, but the Compose stack does not claim that mounting those JARs replaces a NAR's internal classes. The importer preserves binary checksums and third-party notices.

Use the paper's hardware where feasible, record Docker Desktop/Engine resources, and capture actual resolved digests. A current `latest` pull is not a known historical version.
