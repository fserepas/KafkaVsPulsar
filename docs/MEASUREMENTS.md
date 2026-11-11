# Measurement methodology

The manuscript evaluates producer-to-database latency, intermediate stage delays, CPU utilization and memory. The included SQL implements the timestamp differences. Raw data and actual effective configurations must accompany any new results.

## Timestamps

| Stage | Kafka | Pulsar |
| --- | --- | --- |
| Production | UTC string `timestamp` from generator | Same |
| Intermediate metadata | `InsertField` SMT copies Kafka record timestamp; `TimestampConverter` formats milliseconds into `kafka_timestamp` | Enrichment reads Pulsar message publish-time metadata into `pulsar_timestamp`, or local-time fallback |
| Optional processing | `PROCESSED_TIMESTAMP` from ksqlDB/Streams | `processed_timestamp` from the second function |
| Database | `inserted_at` / `INSERTED_AT` defaults on row insertion | `inserted_at` default on row insertion |

Strings formatted with nine fractional digits have no zone suffix but are generated in UTC. Formatting does not establish nanosecond clock accuracy. PostgreSQL conversions use `timestamp(6)`; MySQL explicitly truncates incoming fractional strings to microseconds. Kafka's intermediate field and Pulsar publish metadata are not necessarily identical broker-arrival boundaries. Capture record timestamp type, clocks and uncertainty before treating intermediate delays as equivalent.

For a record, let `t_p` be production, `t_i` intermediate metadata, `t_x` processing, and `t_d` row insertion time. Compute:

- producer-to-database: `t_d - t_p`;
- producer-to-platform: `t_i - t_p`;
- platform-to-database (raw): `t_d - t_i`;
- platform-to-processing (processed): `t_x - t_i`;
- processing-to-database: `t_d - t_x`.

These are timestamp-based pipeline intervals. In particular, `t_x - t_i` includes waiting and preceding pipeline work, not isolated operator CPU time. The row default is evaluated during insertion and does not measure the later durable commit acknowledgement. PostgreSQL uses `clock_timestamp()`; MySQL uses `CURRENT_TIMESTAMP(6)`, so database timestamp semantics also differ.

## Latency procedures

Each platform/database has an initializer under `sql/<platform>/<database>/init.sql`. Raw trials call `calculate_delays_raw_pipeline()`; processed trials call `calculate_delays_processed_pipeline()`.

PostgreSQL procedures use `percentile_cont` interpolation. MySQL procedures implement nearest-rank percentiles using window ranks and `CEIL(p * count)`. Both report average, median/p50, p90, p95 and p99 in milliseconds, but their quantile estimators differ. Preserve this distinction when comparing small datasets or recomputing results. Normalize from unrounded original metrics, not rounded output tables.

The PostgreSQL processed procedures had an unused `num_of_producers` INTO target without a corresponding SELECT expression. This packaging removes that unused parameter/target; the latency formulas are retained. No producer count is inferred from Kafka keys, which can represent a common MQTT topic.

## Delivery, counts and upserts

The generator attempts `producer_count * messages_per_producer` publications at QoS 0. It has no per-message delivery acknowledgement and does not inspect submitted worker futures. Consequently, even a zero process exit status cannot establish successful delivery.

The runner compares expected attempts with persisted primary-key rows and records a drain timeout. JDBC UPSERT and message-ID primary keys deduplicate database rows, so those rows cannot reveal transport duplicate counts. Preserve connector/function errors and backlog state; do not equate rows with all ingress attempts. The archived `rows` file supports correlation and latency reanalysis, not recovery of duplicates already collapsed by upsert.

## Resource metrics

Prometheus scrapes every 15 seconds. The runner exports project-scoped, pipeline-only series:

| Export | Calculation |
| --- | --- |
| `cpu_user_percent` | Sum of 1-minute rates of cAdvisor user CPU seconds, multiplied by 100 |
| `cpu_total_percent` | Sum of 1-minute rates of cAdvisor total CPU seconds, multiplied by 100 |
| `memory_mib` | Sum of cAdvisor `container_memory_usage_bytes` divided by 1024² |

The pipeline scope includes generator, broker dependencies, schema registry, Connect, selected processor and selected database. It excludes Grafana/Prometheus/exporters and unrelated host containers. CPU 100% corresponds to one fully occupied logical core, so summed values can exceed 100%. Memory uses MiB and includes what cAdvisor reports in `usage_bytes`; it is not identical to process heap or working set.

The supplied dashboard originally used user-CPU time and summed all visible containers. This packaging retains its panel definitions but adds a project selector; the runner separately exports both CPU definitions and a narrower, explicit pipeline scope. Reconcile aggregation boundaries with historical results rather than claiming identical resource totals.

Exports cover publication through collection/drain; a 1-minute rate has smoothing history. Discard/retain ramp-up and warm-up only under a predeclared policy. Short smoke tests cannot support resource percentile estimates at this sampling interval. Empty monitoring data are reported as collection errors, not treated as zero utilization.
