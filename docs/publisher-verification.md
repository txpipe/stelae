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

## Batched publication

These run in the ordinary workspace suite, with no Docker. The registry is
`stelae_cardano::testing::loopback`, which speaks the distribution API over real
sockets, keeps blobs per repository and holds each upload to its digest.

`stelae-cardano/tests/parity.rs` holds repositories byte for byte, every tag,
manifest and blob, and what each took to upload:

| Case | Where |
| --- | --- |
| Windows 2 and 3 and one window for everything, against one boundary at a time, including a small final batch | `every_window_publishes_what_one_boundary_at_a_time_publishes` |
| One tag, one exact and at most one entity traversal per batch, counted from what staging reports starting | the same test, with `the_reference_holds_every_case_the_comparisons_are_about` showing the count reads a pass per boundary at a window of one |
| Each publication against stores frozen at its own boundary | `every_publication_is_what_stores_frozen_at_its_boundary_publish` |
| Retained dumps, delayed entity seals, entity holes, account rows that change as they settle and inherited layers, each shown to be in the fixture | `the_reference_holds_every_case_the_comparisons_are_about` |
| A source running out mid-batch | `the_end_of_the_source_publishes_what_one_boundary_at_a_time_would` |
| A Byron main block sharing its boundary block's slot | `a_byron_boundary_holds_its_boundary_block_and_not_the_main_block_beside_it` |
| A consumer restoring every publication, reproducing it from the restored stores and replaying on to the next | `a_consumer_restores_every_publication_of_a_batch_and_replays_on`, and for Byron `a_consumer_restores_a_byron_boundary_and_replays_the_main_block_beside_it` |

`stelae-publisher/tests/kills.rs` runs the real binary, kills it with SIGKILL, which
must be what ends it, and reruns it, each time to the same bytes as a run nothing
interrupted, with every blob a manifest names in the repository, `latest` never
skipping a sequence and nothing written under one it reached:

| Killed | Test |
| --- | --- |
| Once a boundary's bundle is written | `a_kill_after_a_capture_converges` |
| Once a family's runs are marked | `a_kill_after_a_staging_marker_converges` |
| With a blob upload the registry holds and never serves, so it doesn't land | `a_kill_during_an_upload_converges` |
| Between a seal's immutable tag and `latest`, which the registry holds and never serves, so the rerun finds half a seal | `a_kill_between_a_seals_two_tags_converges` |
| With `latest` written and its answer not yet sent, which uploads nothing again | `a_kill_before_a_seals_answer_converges_without_another_upload` |
| Between two sequences, with the last journaled | `a_kill_between_two_sequences_converges` |
| With the last `latest` written and its answer not yet sent, the batch still journaled, so the rerun reproduces the last sequence to compare it, journals it and clears the batch, uploading nothing again | `a_kill_as_the_last_seal_lands_converges_without_another_upload` |

`uploads_the_registry_refuses_are_retried_to_the_same_repository` does the same
for a registry answering 503 to every 25th upload, and checks the registry counted
what it refused, and
`an_uninterrupted_run_traverses_once_a_batch` counts traversals from the whole
process's log, so one started on another thread can't hide.

A cleanup stopped part way is `a_cleanup_that_stopped_part_way_finishes` in
`stelae-cardano/tests/batch.rs`: nothing outside the process can stop it between two
of its file removals.

A kill during a block's replay is left out deliberately: Dolos commits a block's
state before its archive entry, so that kill can leave stores the publisher has to
refuse, as [Stores whose state passed the archive](publisher.md#stores-whose-state-passed-the-archive)
describes. `stelae-cardano/tests/batch.rs` covers that refusal, the journal's other
faults, disk pressure and source identity.

Whole-network runs and their results are in
[batched-export-measurement.md](batched-export-measurement.md).

## Scope and interpretation

All repositories are loopback/disposable and no production registry is named.
Production canary, soak, and mainnet performance remain separate operations and
must not be inferred from these tests.

The former old/new host parity lane compared `stelae-publisher` with the
publisher in Dolos `1ae4e91c18a9e1456a3612af402d7b9b97546d30`. It was retired
when the Dolos v2 configuration changed the snapshot format past what that
publisher produces.
