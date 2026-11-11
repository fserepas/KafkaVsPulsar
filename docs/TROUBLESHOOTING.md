# Troubleshooting

| Symptom | Check |
| --- | --- |
| Missing runtime files | Install the matching companion `runtime-assets.zip`; inspect size/checksum errors |
| Missing custom JAR | Build with JDK 17+ and `mvn -B clean verify` |
| Compose fails on port binding | Stop the other trial/service using 1883, 5431, 3306, 8080/8081/8083, 9090 or 3000 |
| cAdvisor fails to start | Host devices/mounts, Linux daemon/Desktop VM support, image digest; use `--no-monitoring` only for functional runs |
| `configure` times out | Inspect service logs and database initializer errors before publishing data |
| Connector class/FromJson unavailable | Complete connector runtime assets, plugin discovery logs and package layout |
| MQTT connector licensing error | The supplied package identifies an evaluation license; use appropriate entitlement/configuration |
| Kafka connection succeeds then fails | Advertised listeners must be reachable from client containers; use internal `kafka:9092` there |
| No Kafka processed rows | Correct processor/profile, stable consumer group, exact output topic capitalization and schema-aware sink converter |
| Pulsar schema/input decoding failure | Handler topic mapping, packaged JSON SerDe and function status; raw MQTT bytes are not a typed POJO by themselves |
| Pulsar MySQL JDBC driver error | The NAR uses its bundled MariaDB driver; inspect `jdbc:mariadb` config and class name |
| Generator exits but rows are missing | QoS 0 delivery, swallowed worker futures, broker limits, generator CPU/threads, connector/function errors and drain timeout |
| Empty Prometheus capture | cAdvisor targets, Compose project/service labels, sufficient sample duration and selected monitoring profile |
| Negative latency or unexpected units | UTC interpretation, clock offsets, timestamp provenance, millisecond/microsecond conversions |
| Old rows or offsets on manual rerun | Follow the runner's fresh-project lifecycle; collect/stop before selecting another run |

Preserve `collect` diagnostics even after an incomplete workload. Status RUNNING is a readiness condition, not proof that every message was delivered.
