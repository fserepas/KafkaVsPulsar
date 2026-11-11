# Kafka pipeline

The base services are ZooKeeper, Kafka, Schema Registry, Mosquitto and Kafka Connect. The `postgres`/`mysql` profiles select one sink database. The `ksqldb` and `streams` profiles select the optional transformation, and the `load` profile provides the one-shot MQTT generator.

Connector templates under `connectors/` are registered by the runner. JSON envelope ingestion uses FromJson, InsertField and TimestampConverter transforms. Only one raw or processed sink is active in a trial. The processed topic is selected explicitly, preserving `processed_KafkaStreams_temp` capitalization.

Start with the root README and `docs/EXECUTION.md`; use `experiments/runs.json` to map a paper run to the profile/configuration.
