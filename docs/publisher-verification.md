# Publisher verification

## Local OCI gate

The transport suite uses disposable, authenticated `registry:2` containers and
separate repository names. It covers first push
and pull, subsequent missing-
blob-only upload, layer reuse and verified reuse refusal, forced concurrent and
serial agreement, corrupt and wrong-layer refusal, interruption before manifest
publication, restart with the previous `latest` intact, staging cleanup,
credential refusal, and bounded upload/download memory.

```sh
cargo test --locked -p stelae --all-features --test oci \
  -- --ignored --nocapture
```

The tests are marked ignored because they require Docker, and the dedicated
`Local OCI fault and restart parity` CI job explicitly selects them. A run on
2026-09-13 passed all 16 Docker-backed cases. The same run observed a 49,495,816
byte uncompressed transport fixture with 4,251,854 peak bytes held while
uploading and 1,255,355 while pulling.

Host policy and failure boundaries remain covered by the workspace suite:
dry-run/no-op/require-new and predecessor-gap decisions, transient retry and
cancellation, publish-pending-before-advance, replay finalization after failure,
fail-closed latest restore, explicit genesis initialization, corrupt or missing
input refusal, and layer-journal/checkpoint resume. The transport fault tests
exercise an actual registry; narrower unit tests exercise the orchestration
seams around it.

## Scope and interpretation

All repositories are loopback/disposable and no production registry is named.
Production canary, soak, and mainnet performance remain separate operations and
must not be inferred from these tests.

The former old/new host parity lane compared `stelae-publisher` with the
publisher in Dolos `1ae4e91c18a9e1456a3612af402d7b9b97546d30`. It was retired
when the Dolos v2 configuration changed the snapshot format past what that
publisher produces.
