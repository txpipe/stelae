# Publisher host parity

This is the executable gate for replacing the Dolos-hosted publisher with
`stelae-publisher`. It compares application hosts without changing the Stelae
wire format or the Dolos consumer.

## Pinned identities

- old host: Dolos `1ae4e91c18a9e1456a3612af402d7b9b97546d30`
  (`dolos snapshot publish` and its headless snapshot facade);
- new host base: Stelae `47b38a4`, the merge of publisher-host PR 3;
- protocol/profile: Stelae 0.2.0 and `io.txpipe.dolos.cardano` v1;
- real fixture: `stelae-cardano/tests/fixtures/preview-epoch-0`, identified
  and explained by its README;
- expected first-boundary identity:
  `sha256:6232659be34afdf24f56784b9ba3db3ccee70cd7476708652b8ba0381bea0027`.

The fixture contains 4,958 public Preview blocks. It starts at origin, crosses
the exact slot-86,400 epoch boundary, and ends at slot 99,140. Both hosts replay
the same immutable bodies into separate fresh stores. Replay stops at the
boundary, as the production backfill loop does, so publication observes an
equivalent checkpoint and identical predecessor/history inputs.

## Automated report

Run the real replay, directory/OCI publication, and consumer restore gate with
Docker available:

```sh
STELAE_PARITY_NEW_REVISION="$(git rev-parse HEAD)" \
STELAE_PARITY_REPORT="$(pwd)/target/publisher-parity.json" \
cargo test --locked -p stelae-cardano --test publisher_parity \
  -- --ignored --nocapture
```

The JSON report identifies both hosts and all pins, records pass/fail checks,
and measures replay/publish wall time, process peak RSS, artifact size,
transfer, and scratch use. Directory publication moves zero network bytes and
uses no OCI scratch, so both values are explicitly zero rather than omitted.
The report also records each host's first-push layer and byte transfer counters
from separate repositories in a disposable `registry:2`. CI runs the command
in the `Real publisher parity` job and uploads `publisher-parity-report`.

The test requires all of the following before it writes a passing report:

- identical boundary cursor;
- identical layer descriptors, diffIds, retained epoch-1 dump, and canonical
  inscription bytes;
- identical inscription digest;
- actual old/new publication to separate OCI repositories, including equal
  transfer results, dry-run/no-op policy, and forced reproduction;
- successful directory and OCI restore of the new-host artifact through the
  unchanged Dolos profile consumer;
- reproduction of the same identity from the restored cursor/state/history.

The checked run on 2026-09-13 passed in a debug build. Old/new replay took
2.166/2.013 seconds and directory publication took 1.050/1.025 seconds. Both
directory artifacts were 2,003,124 bytes. Peak process RSS after each host was
416,694,272/421,937,152 bytes. Each OCI host uploaded 1,953,824 layer bytes,
skipped 6,288 bytes already present, and produced a 1,960,112-byte compressed
artifact with the expected identity. These tiny-fixture numbers are regression
evidence only; they make no mainnet throughput or memory claim.

## Local OCI gate

The host parity gate above uses an anonymous loopback registry so both immutable
old-host and new-host code paths publish the real fixture under the same simple
transport policy. The deeper transport suite uses disposable, authenticated
`registry:2` containers and separate repository names. It covers first push
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
input refusal, and layer-journal/checkpoint resume. The real-fixture host test
and transport fault tests both exercise an actual registry; narrower unit tests
exercise the orchestration seams around it.

## Scope and interpretation

All repositories are loopback/disposable and no production registry is named.
The old source is required only by immutable git revision, not by a future
Dolos release branch. The real fixture and genesis inputs live here by content
identity, so removal of the old Dolos command does not remove the evidence.

This gate establishes code-path parity for packaging. Production canary, soak,
and mainnet performance remain separate operations and must not be inferred
from this fixture.
