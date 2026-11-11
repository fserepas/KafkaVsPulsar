# Container deployments

`kafka/compose.yaml` and `pulsar/compose.yaml` provide the platform services. `monitoring.yaml` is an optional common overlay; its relative mount paths are resolved against the selected base file under `deployment/kafka/` or `deployment/pulsar/`.

Each database/processor is profile-controlled. The experiment runner selects one database and one optional processor, creates a unique project/network, prepares the chosen connector and waits for readiness. Prefer the [execution guide](../docs/EXECUTION.md) to starting every profile together.

Runtime binaries must first be installed with `scripts/import-runtime-assets.py`. Java JARs must be built from the root POM. The two application Dockerfiles are buildable from the repository root, and are also used by the Compose definitions.
