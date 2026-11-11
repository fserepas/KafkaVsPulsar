# Execution reference

All commands below run from the repository root. First copy `.env.example` to `.env`, install the matching companion assets and build the Java modules. `python3 scripts/experiment.py --help` documents the operator CLI. `plan` needs neither Docker nor credentials.

## Lifecycle and isolation

| Action | Effect |
| --- | --- |
| `R1 plan` | Print the selected parameters; no infrastructure changes |
| `R1 up [--smoke] [--no-monitoring]` | Create a unique trial project, render the selected Pulsar sink, build app images and start core services |
| `R1 configure` | Wait for DB/broker, deploy the selected source/sink and optional processor, check readiness |
| `R1 run [--drain-timeout 180]` | Publish once, wait for expected row count, record actual start/end/drain state |
| `R1 collect` | Export latency, records, status, logs, image metadata and available monitoring data |
| `R1 down` | Stop that project, retain results and database volumes |

One active trial is tracked in `.state/active.json`. Stop it before selecting another run. Workloads cannot be replayed into an existing trial through the runner; a new `up` creates new topics/offsets/functions and database volumes. Collect before `down`, since broker state and live monitoring disappear when containers stop. If an operation fails, state is retained so `collect` and `down` can still be used.

A smoke trial uses 2 producers x 10 messages and 100 ms delay, marked `smoke_test: true`. Its brief duration is unsuitable for 15-second Prometheus samples or performance inference. Re-run without `--smoke` for the full schedule.

## Direct Compose access

Read `project`, `instance`, platform and database from the active manifest. For a Kafka/PostgreSQL trial:

```bash
export COMPOSE_PROJECT_NAME=THE_PROJECT_FROM_MANIFEST
export RUN_INSTANCE=THE_INSTANCE_FROM_MANIFEST

docker compose --env-file .env \
  -f deployment/kafka/compose.yaml \
  --profile postgres --profile load ps
```

Append `-f deployment/monitoring.yaml --profile monitoring` before the command when monitoring is active. Add `--profile ksqldb` or `--profile streams` for its selected Kafka processing service. For Pulsar, replace the base file with `deployment/pulsar/compose.yaml`. The runner prints its exact Compose commands, including profiles.

Use `logs --tail 100 SERVICE` to inspect failures. `down --volumes` with the exact same project/files deletes the retained synthetic database/monitoring volumes; use it only after exporting the results you intend to keep. Do not run a broad host-wide Docker prune as an experiment reset.

## Kafka connector registration

The source template is `deployment/kafka/connectors/mqtt_source_temp.json`. The runner removes its `name` field from the config body and uses `PUT /connectors/<name>/config`. The JSON converter emits the schema/payload envelope required by both stream processors. The source transform attaches the Kafka record timestamp and formats it to milliseconds.

For a raw trial, select `jdbc_sink_temp_raw_<database>.json`. For R11/R13 select `jdbc_sink_temp_processed_postgres.json`; the runner replaces the regex with the exact selected topic, substitutes the local password in memory and registers it. It waits for connector and task states to be RUNNING.

The ksqlDB query is `deployment/kafka/ksqldb/init.sql`. It is run in headless queries-file mode only for R11. R13 starts the Java Streams container only. The runner waits for a stable consumer group before allowing publication.

## Pulsar deployment

The MQTT endpoint is `tcp://pulsar:1883` inside the trial network, mapping the supplied `persistent://public/default/temp` topic. The broker advertises `pulsar`, making internal addresses resolvable across trial containers.

Representative function command, using the active Compose invocation instead of `docker exec` and assuming its shared JAR mount:

```bash
# Add the active -p/--env-file/profile settings shown by the runner.
docker compose -f deployment/pulsar/compose.yaml exec -T pulsar \
  bin/pulsar-admin functions create \
  --tenant public --namespace default --name temp-schema-fn \
  --jar /pulsar/functions/PulsarFunction-1.0-SNAPSHOT.jar \
  --classname org.example.TempSchemaEnforcerFunction \
  --inputs persistent://public/default/temp \
  --custom-serde-inputs '{"persistent://public/default/temp":"org.example.schema.SensorReadingTempJsonSerde"}' \
  --output persistent://public/default/temp_schema \
  --parallelism 1
```

R12 then deploys `org.example.TemperatureProcessorFunction` from `temp_schema` to `processed_temp`. The selected sink configuration is rendered as JSON (valid YAML) at `.generated/<instance>/pulsar/sink.yaml` and mounted at `/pulsar/conf/benchmark/sink.yaml`. The CLI uses `sinks create --sink-config-file ... --parallelism 1`. Function/sink status must show running instances before a workload starts.

The MySQL endpoint is served by MySQL 8.0; the supplied Pulsar sink NAR uses its own bundled MariaDB JDBC driver. The reconstructed configs use `jdbc:mariadb://mysql:3306/...` and its advertised sink class rather than assuming that a separately mounted MySQL JAR changes NAR classloading.

## Results and local information

Collected rows and service logs may be large, especially at 2.4 million messages. Available disk space should cover both database volumes and text exports. Local resolved Compose/container-inspection files contain environment settings; they are ignored by Git and marked `.local`. Review before sharing a result archive.

## Monitoring compatibility

The supplied cAdvisor/node-exporter approach needs a Linux Docker daemon and host mounts/devices. On Docker Desktop those refer to its Linux VM. If monitoring services cannot start, `--no-monitoring` enables a functional run, but CPU/memory results are then unavailable; do not label it a complete resource experiment. Historical floating tags can also change compatibility. Pin and record working digests rather than assuming `latest` recreates the paper environment.
