# Kubernetes

Helm-packaged deployment units related to the Stelae protocol:

- [`dolos-publisher/`](dolos-publisher/) runs Dolos backfill Jobs that publish
  network steles.
- [`registry/`](registry/) runs the zot registry that stores and serves them.

Both charts are instance-agnostic. Deployment-specific values and secrets stay
in the operator's infrastructure repository.
