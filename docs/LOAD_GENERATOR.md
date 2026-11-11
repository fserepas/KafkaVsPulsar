# MQTT workload generator

## Behavior

Each producer is one Java worker thread with its own synchronous Paho MQTT client,
random client UUID, and in-memory persistence. It waits a random 0–1499 ms,
connects with a clean session, and publishes its configured number of messages
to the exact same configured topic at MQTT QoS 0. Messages are not retained.
It sleeps between publications, except after its final message, then disconnects.

Each JSON message contains a random `message_id`, a UTC-formatted `timestamp`, a
temperature rounded to two decimal places, and an ASCII `A` filler string. The
temperature is sampled from `[86, 88)` Fahrenheit before rounding; rounding can
produce 88.00. Random seeds and message UUIDs are not configurable.

## Run

```bash
./scripts/run-load.sh \
  --broker=tcp://localhost:1883 \
  --topic=mqtt/temp \
  --producer-count=250 \
  --messages-per-producer=1000 \
  --delay-between-messages=50 \
  --payload-size=512
```

The example attempts 250,000 messages. Its nominal rate is 5,000 messages/s
before accounting for connection ramp-up, publish calls, thread scheduling, and
resource saturation. It is an example workload, not a published result.

| Argument (`--name=value`) | Default | Actual behavior |
| --- | --- | --- |
| `--broker` | required | Paho endpoint, e.g. `tcp://localhost:1883` |
| `--topic` | required | Exact destination; no producer suffix is appended |
| `--producer-count` | 100 | Worker threads; clamped to at least 1 |
| `--messages-per-producer` | 600 | Attempts per worker; clamped to at least 1 |
| `--delay-between-messages` | 1000 | Sleep in ms; clamped to at least 0 |
| `--payload-size` | 130 | Filler sizing parameter; clamped to at least 130 |

Unknown options are silently ignored by the application. There is no application
`--help` option. Use the exact spelling above. Authentication and TLS credential
configuration are not exposed by this CLI.

## Payload size and rate interpretation

The filler contains `max(0, payloadSize - 130)` ASCII characters. The complete
serialized JSON is larger than the filler because of keys, UUID, timestamp,
temperature, punctuation, and quoting. The JSON length can vary. Measure UTF-8
bytes at publication if comparing exact 512/1024/2048-byte workloads. MQTT and
transport framing add further overhead.

For positive delay `d` milliseconds and `P` producers, nominal steady-state rate
is `P * 1000 / d` messages/s, and attempted total is `P * N`. A zero delay means
an unpaced workload; the formula does not apply. Each sleep occurs after the
publish call, so the achieved interval includes publish time.

## Container execution

Replace the network and endpoint placeholders with your actual deployment:

```bash
docker run --rm --network YOUR_NETWORK iot-mqtt-load:local \
  --broker=tcp://YOUR_MQTT_SERVICE:1883 \
  --topic=mqtt/temp \
  --producer-count=2 \
  --messages-per-producer=10 \
  --delay-between-messages=1000 \
  --payload-size=512
```

The included Pulsar deployment uses the supplied MQTT handler and the topic
`persistent://public/default/temp`. The experiment runner sets this mapping
explicitly; verify it in the first smoke test on your Docker host.

## Run success and scaling limits

The application catches invalid configuration and returns from `main`, and
submitted worker futures are not inspected. Consequently, the process exit code
alone does not establish that all workers succeeded. Capture MQTT receiver or
downstream counts and distinct message IDs, and inspect errors.

There is one native thread and connection per producer. Large device counts can
exhaust file descriptors, native memory, broker connection limits, or ephemeral
ports. Benchmark the generator separately and report its CPU and memory use.
