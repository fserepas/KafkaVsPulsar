# Validation performed

Checks completed on 10 November 2026 with Temurin JDK 17.0.21+9, Maven 3.9.9, Python 3 and Docker Compose CLI 2.32.4 on Linux x86-64.

| Check | Outcome |
| --- | --- |
| Maven `clean verify`, all three application modules | Passed; shaded JARs produced |
| Operator/importer tests | 8 passed |
| Paper R1-R19 platform/database/processing/size/rate mapping | Checked against Section 4/Tables 4-5; automated matrix check passed |
| Compose configuration | 38 merged configurations passed: all 19 runs, with and without monitoring |
| Runtime dependencies | 109 supplied files verified by size/SHA-256 and installed successfully |
| JSON/YAML/XML parsing, Python compilation, Bash syntax, relative Markdown links | Passed |
| Isolated Java checks | CLI boundaries/filler sizing, temperature boundary values/null behavior/field preservation, Kafka defaults/overrides, MQTT JSON SerDe round trip passed |
| Existing MQTT Java sources and existing Pulsar function/schema sources | Preserved; the input SerDe is an added class |

The Python tests mock Docker and readiness/status calls. They verify scenario selection, exact Kafka processed sink topic, mandatory/optional Pulsar stages, one-workload-per-trial behavior, rejection of incomplete row counts despite a zero generator exit status, and asset checksum handling. They do not test a real broker or database.

To repeat the operator checks:

```bash
python3 -m unittest discover -s tests -v
python3 scripts/validate-compose.py
mvn -B clean verify
```

The Compose configuration check requires the Docker Compose CLI but not a running daemon. Maven has no Java application unit-test suite; isolated Java checks were run through a temporary local harness. GitHub Actions is configured for the build/operator/Compose checks and has not been executed on a remote repository.

## Not executed in this environment

- Docker image builds, broker/service startup or complete MQTT-to-database smoke runs.
- Actual Kafka Connect/Schema Registry/ksqlDB/Streams data flow.
- Actual Pulsar function/sink deployment, MQTT topic mapping or NAR/database-driver behavior.
- PostgreSQL/MySQL procedure execution against a live database.
- Monitoring compatibility or actual resource samples from cAdvisor.
- Full workloads, raw result regeneration or verification of the manuscript's measured performance values.

No Docker daemon is available here. Validate the documented smoke paths on the intended host before treating the artifact as end-to-end tested. Static configuration checks and isolated transformations do not establish benchmark reproducibility.
