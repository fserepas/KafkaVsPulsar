# Trial outputs

No original raw benchmark dataset is supplied. Each live runner invocation creates `results/<run>/<UTC-instance>/` with a manifest, generator/service logs, latency summary, persisted rows, status, image metadata and available Prometheus time series. Smoke trials are labeled separately and do not reproduce paper results.

Outputs are ignored by Git. Preserve successful and incomplete trials. Review `.local` container/configuration captures before sharing because they can contain local credentials. Archiving a result should include its source commit and runtime-asset version, workload settings, exact image digests, measurement/analysis definitions and any failures or exclusions.
