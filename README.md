# Stelae

A deterministic, content-addressed snapshot protocol for large reproducible
datasets, distributed through OCI registries.

A *stele* is a snapshot: a set of compressed layers plus an **inscription** —
a canonical (RFC 8785) JSON document listing the uncompressed digest of every
layer along with the profile, sequence and position that identify what the
snapshot is *of*. Determinism is anchored on the inscription's sha256:
independent parties that hold the same data produce byte-identical
inscriptions, whatever compressed their blobs. The payload is opaque to the
protocol — a **profile** names the layer kinds, scopes and parameters, so any
project shipping large reproducible datasets can put its steles beside anyone
else's in the same registry without colliding or forking the spec.

The protocol was designed for [Dolos](https://github.com/txpipe/dolos), the
Cardano data node, whose `io.txpipe.dolos.cardano` profile remains its first
producer and consumer; it carries no Cardano assumption.

## Crates

| Crate | What it is |
| --- | --- |
| [`stelae`](stelae/) | The wire protocol: framing, inscription, digests, plan/resume, the directory and OCI transports. The normative crate — a third party implements a profile against this and nothing else. |
| [`stelae-driver`](stelae-driver/) | Profile-generic lifecycle machinery: the chained-publish lifecycle, restore budget/checkpoint, preflight, retry, reporting. A profile author's toolkit, never required for wire compatibility. |

The boundary proof lives in the test suite: `stelae/tests/toy_profile.rs`
implements a second, trivial profile against the protocol surface alone.

## Specification

The normative specification is [`SPEC.md`](SPEC.md). The Dolos profile's own
half — its layer kinds, cut point, and pipelines — lives with Dolos, in
[ADR-004](https://github.com/txpipe/dolos/blob/main/adrs/004_stelae_snapshots.md).

## Versioning

The two crates version in lockstep, restarting at `0.x` for the protocol's
own API maturity; `stelae-driver` depends on `stelae` with an exact pin, and
every release is a single tag covering both. Consumers pin a tag, never a
branch. The crates are distributed through this repository as a git
dependency; crates.io names are deferred until a consumer outside TxPipe
needs them (the name `stelae` is taken by an unrelated crate).

`minicbor` is pinned to the 0.26 line deliberately: it is the line
[pallas](https://github.com/txpipe/pallas) pulls in, so the Dolos profile
encodes against the same codec the protocol does. Moving it is a
byte-compatibility question, not a dependency bump.

## License

Apache-2.0.
