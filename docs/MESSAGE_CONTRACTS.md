# Message formats and field mapping

The generator publishes UTF-8 JSON with `message_id` (UUID string), `temperature` (Fahrenheit number), `timestamp` (UTC string without a zone suffix) and `payload` (ASCII filler). Files under `examples/` use artificial timestamps and IDs, not measured results.

Kafka's supplied MQTT source transforms parse these fields, insert a Kafka record timestamp and serialize JSON through Connect's schema-enabled converter. Kafka Streams/ksqlDB therefore see a top-level `payload` object inside a schema/payload envelope. Their output uses Confluent JSON Schema framing; a plain text consumer does not decode that framing automatically.

The Pulsar MQTT endpoint produces unstructured bytes. The explicit JSON input SerDe decodes them to `SensorReadingTemp`; enrichment produces `SensorReadingTempSchema`, and the optional processor produces `SensorReadingProcessedTemp`. These output topics use structured schema handling compatible with the JDBC sink.

| Meaning | Kafka processed field | Pulsar processed field |
| --- | --- | --- |
| Correlation ID | `MESSAGE_ID` | `message_id` |
| Input Fahrenheit | `TEMPERATURE_FAHRENHEIT` | `temperature_fahrenheit` |
| Converted Celsius | `UPDATED_TEMPERATURE_CELSIUS` | `updated_temperature_celsius` |
| Production time | `PRODUCED_TIMESTAMP` | `produced_timestamp` |
| Intermediate metadata | `KAFKA_TIMESTAMP` | `pulsar_timestamp` |
| Processing time | `PROCESSED_TIMESTAMP` | `processed_timestamp` |
| Filler | `PAYLOAD` | `payload` |
| Kafka key | `PRODUCER` | no equivalent producer field populated by the function |

The MQTT generator sends every producer's messages to the same configured topic. Kafka record keys therefore must not be assumed to identify distinct generator devices. See [measurement definitions](MEASUREMENTS.md) for timestamp provenance, precision and latency boundaries.
