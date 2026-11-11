# Artifact inventory

| Supplied material | Treatment in this package |
| --- | --- |
| MQTT generator, Kafka Streams and Pulsar Functions Java sources | Unified under `modules/`; built from the root Maven reactor |
| Kafka/Pulsar Compose files | Reorganized under `deployment/`, with documented integration fixes |
| Kafka source and four JDBC sink configurations | Preserved as templates; runner selects one sink and substitutes the local password |
| Four Pulsar JDBC sink configurations | Preserved as JSON/YAML-compatible templates, with documented driver/configuration corrections |
| ksqlDB temperature query | Included under `deployment/kafka/ksqldb/` |
| Database schemas and latency procedures | Included under `sql/`; processed PostgreSQL INTO mismatch corrected |
| Prometheus/Grafana/JMX configuration | Included as configuration; dashboard gains trial-project filtering |
| Required connector/NAR/driver/JMX binaries and included notices | Exact checksum-verified companion `runtime-assets.zip`, installed separately |
| Generated custom application JARs | Rebuilt from source; not committed |
| Legacy KafkaProject/PulsarProject binary-only folders | Not used by the documented R1-R19 pipelines; not committed as source implementations |
| Grafana SQLite databases, downloaded Grafana plugins and old runtime state | Omitted; clean provisioning is supplied |
| Screenshots and unrelated voltage examples | Omitted from the temperature-experiment execution path |
| Manuscript | Used for experiment documentation; its PDF is not republished in the code package |
| Original raw measurements | Not supplied; no historical dataset has been fabricated |

Read `REPRODUCIBILITY.md` for the complete change list and `VALIDATION.md` for what has actually been checked.
