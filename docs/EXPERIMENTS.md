# Experiment definitions

## Source and scope

`experiments/runs.json` maps the manuscript's Section 4, Tables 4-5, to the actual deployment artifacts. It contains 19 configurations grouped into six cases. The companion execution guides supply per-producer delays, counts and concurrency. The manuscript does not specify every operational parameter: repeated-trial count, warm-up duration and exact historical image digests must not be inferred from the scripts.

## TC1: baseline, R1 and R2

Compare producer-to-database latency, CPU and memory with size setting 130, 100 nominal messages/s and PostgreSQL. R1 uses Mosquitto -> Kafka Connect -> Kafka -> JDBC. R2 uses Pulsar's MQTT handler -> schema enrichment -> JDBC. Neither enables optional temperature conversion. Pulsar enrichment remains necessary for structured JDBC records.

```bash
python3 scripts/experiment.py R1 plan
python3 scripts/experiment.py R2 plan
```

For either selected run, execute `up`, `configure`, `run`, `collect`, then `down`. Start the second platform only after stopping the first.

## TC2: database choice, R3 and R4

Keep the baseline workload and replace PostgreSQL with MySQL. Compare R3 with R1 for Kafka and R4 with R2 for Pulsar. The runner starts only the selected database and only its sink connector. Sink batches retain the supplied size setting of 3000; do not change it silently between systems.

## TC3: payload-size setting, R5-R10

Run the Kafka/Pulsar pairs R5/R6 (512), R7/R8 (1024), and R9/R10 (2048). PostgreSQL and 100 nominal messages/s remain fixed. Measure actual JSON byte lengths separately: the generator's fixed-overhead estimate is not exact message sizing. Changing the generator to enforce exact sizes would create a different workload and should be labeled as a follow-up experiment.

## TC4 and TC5: processing, R11-R13

Use PostgreSQL, setting 130 and 100 nominal messages/s. R11 uses ksqlDB, R12 uses Pulsar Functions, R13 uses Kafka Streams. All compute `(F - 32) * 5 / 9`, retain correlation IDs and append processing metadata. Kafka processing outputs use Confluent JSON Schema serialization. Pulsar full processing uses an intermediate topic plus two functions; this has a different stage structure from Kafka's one processing topology.

Do not deploy the raw sink in addition to the selected processed sink: that adds extra database work. The Kafka sink subscribes to exactly one processing output topic; a broad `processed_.*` subscription would mix alternatives if both were active.

## TC6: production rate, R14-R19

At fixed size setting 130 and PostgreSQL, run Pulsar/Kafka pairs R14/R15 (1,000/s), R16/R17 (2,000/s), and R18/R19 (4,000/s). The supplied schedules use 1000 producer threads and 600/1200/2400 messages per producer, with 1000/500/250 ms sleeps. Publish overhead is additional to the sleep, so achieved rates can be lower.

The generator host can become a bottleneck at 1000 threads/connections. Record file descriptor limits, CPU/memory, database row counts, backlog/drain state, and all errors. QoS 0 provides no delivery acknowledgement; unsuccessful trials should remain visible in the dataset.

## Comparing runs

The manuscript's normalized comparison groups use R1 (R1-R4), R5 (R5-R10), R11 (R11-R13), and R14 (R14-R19) as their reference runs. These are analysis baselines, not a reason to skip the other workloads. Preserve raw metrics before normalization. Repeating the configuration does not guarantee identical numerical results on another host or with different images.
