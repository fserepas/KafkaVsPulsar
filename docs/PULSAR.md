# Pulsar Functions and pipeline

The complete deployment is described in [execution reference](EXECUTION.md) and [Pulsar deployment](../deployment/pulsar/README.md).

`TempSchemaEnforcerFunction` preserves the input fields and adds message publish-time metadata, falling back to local time when unavailable. `TemperatureProcessorFunction` performs Fahrenheit-to-Celsius conversion and adds a processing timestamp. Both retain the supplied typed function logic and null handling.

The added `SensorReadingTempJsonSerde` decodes UTF-8 JSON from the MQTT handler's raw bytes. The runner supplies it with `--custom-serde-inputs` and lets structured outputs use Pulsar schema handling. It does not introduce an additional processing stage. [Apache Pulsar 4.0 SerDe documentation](https://pulsar.apache.org/docs/4.0.x/functions-develop-serde/) and [Functions CLI reference](https://pulsar.apache.org/docs/4.0.x/functions-cli/) describe these settings.

All Pulsar trials deploy enrichment. R12 also deploys temperature conversion. Sinks read `temp_schema` for raw paths or `processed_temp` for R12; only the selected database sink is active. Check function/sink running counts and errors before publishing.
