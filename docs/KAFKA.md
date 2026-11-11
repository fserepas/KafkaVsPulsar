# Kafka application and pipeline

The full Kafka pipeline is documented in [execution reference](EXECUTION.md) and [deployment configuration](../deployment/kafka/README.md).

The Java processor reads `temp`, expects a top-level schema/payload JSON envelope, converts Fahrenheit to Celsius, and writes Confluent JSON Schema output to `processed_KafkaStreams_temp`. The included MQTT source connector supplies that envelope.

| Environment variable | Default |
| --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` |
| `SCHEMA_REGISTRY_URL` | `http://schema-registry:8081` |
| `KAFKA_INPUT_TOPIC` | `temp` |
| `KAFKA_OUTPUT_TOPIC` | `processed_KafkaStreams_temp` |
| `KAFKA_APPLICATION_ID` | `temperature_processor` |

The runner uses a fresh broker/DB project and `benchmark-temperature` application ID inside that isolated project. In standalone host execution, override broker/registry endpoints and ensure the broker advertised listener is reachable. Invalid JSON/mapping exceptions are skipped; missing individual fields may become empty strings or 0.0 through Jackson defaults. This is not strict semantic validation.
