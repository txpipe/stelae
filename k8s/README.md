# Kubernetes

Helm-packaged deployment units related to the Stelae protocol:

- [`dolos-publisher/`](dolos-publisher/) runs Stelae Cardano backfill Jobs.
  Existing Dolos deployments stay pinned to the previous 0.1 chart.
- [`registry/`](registry/) runs the zot registry that stores and serves them.

Both charts are instance-agnostic. Deployment-specific values and secrets stay
in the operator's infrastructure repository.
