# Reproducibility and configuration notes

## Evidence used

The experiment mapping comes from the supplied manuscript dated 10 November 2026, Section 4/Tables 4-5. Deployment files, SQL, connector binaries, schemas and monitoring configuration come from the supplied Kafka/Pulsar deployment archives. Application Java sources come from the three previously supplied component projects. The original application JARs have not been substituted for unavailable source code; the current source is built explicitly.

## Integration changes

| Area | Change in this package | Reason |
| --- | --- | --- |
| Repository/build | One Maven reactor, explicit compiler plugin, unified module layout | Build all three Java components consistently |
| Kafka application | Optional environment overrides; original defaults retained | Run inside or outside the deployment network |
| Kafka Compose | Remove globally fixed container names; select one DB/processor by profile | Independent trials and no accidental simultaneous processing |
| Source connector | Retain JSON transforms; remove unverified EOS request settings | QoS 0 and the supplied pipeline do not justify an end-to-end exactly-once claim |
| Processed JDBC sink | Subscribe to exactly the selected output topic | Avoid mixing ksqlDB and Streams results |
| Pulsar address | Advertise `pulsar` instead of `localhost` | Addresses must resolve across the Docker network |
| Pulsar ingress | Add explicit `SensorReadingTempJsonSerde`, using Gson 2.8.9 | Decode MQTT handler bytes into the existing typed function without another stage |
| Pulsar/MySQL config | Use the supplied NAR's `MariadbJdbcAutoSchemaSink` and MariaDB JDBC scheme; remove nonstandard top-level `jar` field | Respect the NAR's bundled driver and supported config |
| PostgreSQL procedures | Remove unused processed `num_of_producers` output/INTO target | Correct SELECT/INTO column mismatch |
| Credentials and time | Local `.env` settings; UTC database/container settings | Keep credentials consistent and timestamp interpretation explicit |
| Monitoring | Add Compose-project filtering and explicit collection scope | Exclude unrelated host workloads and identify what CPU/memory sums represent |
| Runtime dependencies | Checksum-verified companion archive; retain notices | Keep generated/vendor binaries out of Git history without discarding required files |
| Trial lifecycle | Fresh Compose project/database per `up`; count-aware drain check | Avoid stale offsets, schemas, rows and hidden incomplete runs |

The Fahrenheit/Celsius transformation, supplied payload filler logic, original QoS 0 behavior, SQL latency formulas, and connector batch-size setting are retained. The original typed Pulsar function classes are unchanged; the added SerDe makes ingress decoding explicit. These are documented reconstruction fixes, not evidence that historical runs used every reconstructed setting.

## Known limits

- No original raw measurement dataset, run-by-run logs, or historical image digests were supplied.
- Several supplied image tags float. Current pulls may differ from historical executions.
- Paper size labels do not establish exact serialized message sizes for this generator.
- Random UUIDs, temperatures and connection jitter are not seeded; identical event replay is not implemented.
- The generator exit status is insufficient for delivery validation; database UPSERT masks duplicates.
- No repeat count, warm-up interval, exclusions or clock-error bound is invented here.
- PostgreSQL/MySQL percentile estimators and row timestamp behavior differ.
- Pulsar enrichment adds a stage even in baseline runs; CPU, memory and delay boundaries must stay explicit.
- End-to-end Docker execution and paper result regeneration require a Docker-capable host. See `VALIDATION.md` for checks actually performed.

## Recording a reproducible experiment

Run smoke tests for both platforms, the MySQL sinks and all processing paths on the intended host. Record effective image digests, the source version, clocks, workload settings, repetitions and warm-up policy. Preserve each run's raw rows, metrics, connector/function status and errors before analysis.
