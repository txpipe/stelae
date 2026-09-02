# The Stelae Protocol

This document is the normative specification of the Stelae snapshot protocol:
what the `stelae` crate implements, and what a third-party profile is written
against. It is the protocol half of what was authored as [Dolos
ADR-004](https://github.com/txpipe/dolos/blob/main/adrs/004_stelae_snapshots.md),
extracted when these crates left the Dolos workspace. That ADR remains the
decision record; the Dolos profile's own normative half — its layer kinds,
record shapes, cut point and pipelines — is
[`crates/snapshot/PROFILE.md`](https://github.com/txpipe/dolos/blob/main/crates/snapshot/PROFILE.md)
in the Dolos repository, and this document owes both of them nothing
normative. Where a rule needs an example, the Dolos profile
(`io.txpipe.dolos.cardano`) supplies it, as illustration and never as
requirement.

## Overview

A **stele** (*stele*: a standing inscribed slab) is one published artifact set
at one sequence point: a set of compressed **layers** plus an **inscription**
— a canonical JSON document listing the uncompressed digest of every layer
along with the profile, sequence and position that identify what the snapshot
is *of*. The protocol owns framing, the signable document, transport,
attestation and restore planning; a **profile** owns layer kinds, record
shapes, position, parameters, tag rendering and store semantics.

Four properties drive every rule below:

1. **Delta transfer.** Layers are immutable and content-addressed, so a
   publish uploads only what is new and a restore fetches only what it lacks.
2. **Implementation-agnostic formats.** Layer content is deterministic CBOR
   sequences of logical records — never database files.
3. **Determinism.** Two independent parties holding the same data produce
   byte-identical inscriptions, whatever compressed their blobs.
4. **Multi-party attestation.** Identity is the inscription's sha256;
   independent parties reproduce and sign that one digest.

## Why an OCI registry

Registries are content-addressed: pushing skips blobs the registry already
holds (HEAD by digest) and pulling fetches only the layers missing locally,
so immutable layers make delta transfer a property of the transport rather
than a feature of this protocol. The referrers API gives detached signatures
a standard, tooling-compatible home. And registry infrastructure — auth, CDN
distribution, garbage collection, mirroring — is commodity, so the protocol
specifies none of it. Any OCI Distribution v1.1 registry is a valid home for
a stele repository.

## Profiles, naming and media types

Envelope types are protocol-owned and shared by every profile; payload types
are vendor-owned:

| Role | Media type | Owner |
|---|---|---|
| Artifact type (manifest) | `application/vnd.stelae.stele.v1` | protocol |
| Config blob (the inscription) | `application/vnd.stelae.inscription.v1+json` | protocol |
| Signature (referrer artifact) | `application/vnd.stelae.signature.v1` | protocol |
| Layer payloads | `application/vnd.{vendor}.stele.{kind}.v{n}+{codec}` | vendor |

Normative rules for coexistence:

1. Payload media types must carry a vendor slot the publisher controls.
   `vnd.stelae.*` is reserved for envelope types and is never a payload type —
   Stelae defines no payload format.
2. Profile names are reverse-DNS and vendor-owned (the first profile is
   `io.txpipe.dolos.cardano`, version 1). The short token in media types
   follows IANA `vnd.` custom.
3. The protocol never parses layer bodies or a profile's opaque objects. An
   unknown profile name, or a profile major version the client does not
   implement, is a clean refusal — never a partial or misinterpreted restore.
   A layer whose **kind** the client does not implement is deliberately not
   one of those: the client cannot store what it does not model, so it skips
   the layer and reports the skip, and only a `required: true` in that
   layer's `scope` turns the skip back into a refusal naming the kind and the
   scope. The publish side keeps the strict rule in both cases — a publisher
   that cannot build a kind must not chain onto a stele carrying it, since
   the alternatives are dropping the layer from the repository silently or
   attesting bytes it never read. `required` lives in the profile-owned
   `scope` and not in an OCI annotation, so it is signed planning input
   rather than unsigned transport metadata; the protocol carries it and never
   reads it. It is one-way — a kind published as required forever constrains
   readers older than it — so marking one is a specification-level act for
   the profile and rare by construction.
4. One repository per (profile, dataset). Sharing a registry namespace is
   safe: discovery filters on the common `artifactType`, the `profile` field
   discriminates, and tags are rendered by the profile.
5. Signatures are generic and cover the inscription digest, which itself
   binds the profile — so signing and verification tooling is shared across
   vendors.

Rule 3 answers for a kind the reader does not *know*. It says nothing about a
kind the publisher no longer *carries*, and `required: true` cannot be
stretched to cover one: `required` is a property of a layer, and a retired
kind has no layer to put it on. Absence is meaningful in this format — a
profile may define kinds that exist only when they hold content — so a reader
that still models a retired namespace, finds no layer for it, and reports a
clean restore has silently lost a slice of data.

**A profile therefore declares the namespaces it defines, and a retirement is
declared rather than inferred.** The profile's `parameters` carry a schema
map with an entry for every namespace the profile version defines; a
namespace it has retired keeps its entry at revision `0`, which is not a
schema revision and reads as "this version defines no records here". A
restore compares that map against the namespaces it models, before a store is
opened: an entry that is missing or zero for one it models refuses the
restore and names the namespace. The gate is presence, with revision `0`
reading as absence per the sentinel above; a *live* revision's value is
never compared — a revision the reader has not seen describes bytes it can
still parse, and gating on it would make every additive append breaking.
Like `required`, the rule binds forward and not backward. Retiring a
namespace is a specification-level act for the profile.

## Layer format

All layers are zstd-compressed CBOR sequences (RFC 8742). The deterministic
encoding profile is pinned by this spec: shortest-form integers, definite
lengths only, no floats, no tags (RFC 8949 §4.2.1). Every layer starts with a
protocol-defined header record that makes the blob self-describing even when
detached from its registry:

```text
[format_version = 1, profile: tstr, kind: tstr, scope: any]
```

`scope` is opaque to the protocol: the profile decides its shape, and the
protocol carries it, canonicalizes it (in the inscription) and compares it
for equality — never reads it. Content records after the header are entirely
profile-defined.

`diffId` — a layer's identity — is the sha256 of the **uncompressed** CBOR
sequence. Determinism is anchored on uncompressed bytes because compressed
output is only stable for a pinned library version and parameters; the
compression declared in the inscription is transport, and correctness never
depends on it.

## The inscription

The inscription is the canonical, signable document of a stele: the OCI
config blob, canonical JSON per RFC 8785 (JCS). Generic keys plus three
profile-owned opaque objects — `position`, `parameters` and each layer's
`scope`:

```json
{ "schema": 1,
  "profile": {"name": "io.txpipe.dolos.cardano", "version": 1},
  "sequence": 550,
  "position": { "…": "profile-owned" },
  "parameters": { "…": "profile-owned" },
  "compression": {"algo": "zstd", "level": 9},
  "history": [
    {"sequence": 548, "inscriptionDigest": "sha256:…"},
    {"sequence": 549, "inscriptionDigest": "sha256:…"} ],
  "layers": [
    {"kind": "blocks", "mediaType": "application/vnd.dolos.stele.blocks.v1+zstd",
     "diffId": "sha256:…", "records": 21600, "uncompressedSize": 43210000,
     "scope": {"epoch": 0, "startSlot": 0, "endSlot": 21599}} ] }
```

- `schema` is the protocol's own version; verifiers fail closed on anything
  but the versions they implement.
- `sequence` is the protocol's monotonic ordering key. What it counts is the
  profile's decision (the Dolos profile sets it to the epoch).
- `position`, `parameters` and `scope` are canonicalized by JCS like every
  other key, so determinism holds without the protocol interpreting them.
  Verifiers reject unknown *generic* top-level keys, so extension happens
  only inside those three objects.
- Determinism and signing are defined only over this document's sha256.
- Restore verifies registry blob digests (transport integrity) and diffIds
  (canonical identity).

### History

`history` embeds the digest of every previously published inscription, so the
latest signed inscription transitively attests the entire publication history
(~80 bytes per sequence — negligible for a config blob). This makes
attestation outlive blob retention: a stele whose blobs have long been
garbage-collected can still be verified by anyone holding a copy, because the
copy carries its own inscription — check that inscription's digest against
the `history` of the latest signed one, then the layers against its diffIds.
No external trusted storage of attestations is required.

Retention is the publisher's policy, not the protocol's: a publisher keeps a
trailing window of immutable tags, untagged blobs are the registry's garbage
collector's to reclaim, and blobs still referenced by later manifests
survive on their own. `history` is what makes that policy safe — trust
evidence for a reclaimed stele survives in every later inscription.

**History invariant:** `history` contains exactly one entry per published
sequence, contiguous from the dataset's first published sequence (pinned
per dataset alongside the default repository) up to `sequence - 1`, in
strictly ascending order — no gaps, no duplicates. JCS canonicalizes object
keys but preserves array order, so the ordering is normative. Verifiers
reject inscriptions that violate the invariant. This is a reproducibility
requirement as much as a safety one: independent publishers converge on
byte-identical inscriptions only if the publication schedule and history
encoding are canonical. If the list ever outgrows the inscription, the
designated evolution is a sequence-indexed Merkle Mountain Range commitment
(`{root, size}`) — a schema-versioned change that can be built retroactively
from the flat list.

A side-effect of anchoring identity on uncompressed content digests is that
layer *content* can be mirrored over any content-addressed transport — or
re-compressed with a different algorithm — and still be verified against the
same signed inscription via diffIds. Consumption is stricter than
verification: the restore client expects the canonical blobs referenced by
the OCI manifest, so re-encoded mirrors serve archival and verification, not
direct restore.

## OCI layout

- One repository per (profile, dataset). The protocol requires an immutable
  tag per sequence plus a moving `latest`; the profile renders the strings.
- `artifactType: application/vnd.stelae.stele.v1`; layer media types per the
  profile; three annotations per layer, named below.

### The manifest

A stele in a registry is one OCI image manifest, and its shape is closed: a
conforming publisher writes exactly the fields below, and a conforming client
refuses anything else.

- `schemaVersion: 2`; `mediaType: application/vnd.oci.image.manifest.v1+json`;
  `artifactType: application/vnd.stelae.stele.v1`.
- `config` is the inscription's descriptor: `mediaType` is
  `application/vnd.stelae.inscription.v1+json`, `digest` is the sha256 of the
  canonical inscription bytes — the same digest independent parties reproduce
  and sign — and `size` is those bytes' length.
- `layers`: one descriptor per inscription layer, **in inscription order**.
  Each carries the layer's `mediaType` exactly as the inscription states it,
  the compressed blob's `digest` and `size`, and the three annotations below.
- No `subject` and no manifest-level `annotations`.

The manifest bytes are canonical JSON per RFC 8785, through the same
canonicalizer as the inscription, and are pushed verbatim: the protocol has
one answer to "what are the bytes of this JSON document", not two that agree
until they do not.

The per-layer annotation keys are reverse-DNS under `stelae.store`, a domain
TxPipe owns:

| Key | Status | Value |
| --- | --- | --- |
| `store.stelae.layer.diffId` | **normative** | the layer's `diffId`, exactly as the inscription states it |
| `store.stelae.layer.kind` | informational | the layer's profile-defined kind |
| `store.stelae.layer.scope` | informational | the layer's scope object as stringified canonical JSON |

`store.stelae.layer.diffId` is the identity→blob map — the thing a registry
hands over for free and a directory has to rebuild by decompressing every
blob. A client that does not read it cannot fetch a layer; it is the one
annotation a reader must understand. The other two exist so a human or a
generic registry tool can see what a blob covers without fetching the config
blob, and a client may ignore them.

### Manifest–inscription agreement

The manifest and the inscription are two views of one stele — the inscription
holds identity, the manifest holds transport — and a disagreement between
them, in either direction, is a refusal, never a preference.

A publisher refuses to build a manifest — before anything is pushed — when
the inscription describes a layer that was never written, or a layer was
written that the inscription does not describe: a blob nothing attests must
not be published.

A client refuses a manifest — before any blob is fetched — when:

- `artifactType` is missing. This fails closed by choice: a registry that
  strips the OCI 1.1 discovery field has published something a client cannot
  recognize as a stele, and reading it anyway would make the discovery
  contract advisory.
- `artifactType` is present and is not `application/vnd.stelae.stele.v1`.
- the config descriptor's media type is not the inscription's.
- the manifest's layer count differs from the inscription's.
- a layer carries no `store.stelae.layer.diffId` annotation, so nothing says
  which layer it holds.
- a layer's `diffId` annotation disagrees with the inscription's layer *at
  that position*. Positional correspondence is a check of its own: a manifest
  carrying the right blobs in the wrong order passes the map and fails the
  order.
- a layer's media type disagrees with the inscription's at that position.

### The manifest size ceiling

A manifest past **4 MiB** (`stelae::MANIFEST_SIZE_LIMIT`) is refused before
the push. The figure is not a limit the OCI specification imposes; it is the
ceiling registries converge on, and the refusal is measured on the exact
canonical bytes that would have been pushed, so it names the document and its
layer count instead of arriving later as a registry's `413`. A descriptor
with its annotations costs ~350 bytes, so the ceiling falls near 12,000
layers; a profile's layer arithmetic must stay inside it (the Dolos profile's
own accounting lives in its half of the spec).

### What the transport requires of its host

- **A process that opens a registry client must have installed a
  process-default rustls `CryptoProvider` first.** The transport ships no
  crypto backend of its own (`reqwest/rustls-no-provider`): the backend the
  client library would otherwise pick wants `cmake` on every build machine,
  so it stays out of the tree and the choice of provider moves to the
  program. Omitting the install is a panic when the registry client opens,
  not a link error.
- **Authentication is the host's decision, in one of three shapes.** The
  client is opened with credentials its caller supplies — anonymous, a
  bearer token, or an HTTP Basic pair — and never sources them itself. Which
  identity a program authenticates as is that program's credential policy,
  and where it keeps its credentials is that program's deployment: a protocol
  library that read an environment variable would be deciding both on its
  host's behalf, and naming the variable would freeze that decision into a
  published API. **So this specification names no environment variable and no
  configuration key**, and `stelae::oci::Options::auth` is the whole of the
  interface.

  Anonymous remains legitimate and is what a genuinely public repository
  wants. It is not the only legitimate deployment: read access to a
  repository can be free and identity-less and still credentialed.

## Restore planning

A client reads a stele in a fixed order, refusing early: resolve the tag to
a manifest and refuse any manifest–inscription disagreement (above, before
any blob is fetched); verify the inscription's digest, `schema`, profile
name and major version, and signatures; plan; then fetch, verifying each
blob's registry digest (transport integrity) and its `diffId` (canonical
identity).

The protocol's half of a restore is planning and resume; layer selection is
profile-side by necessity (a layer's `scope` is opaque, so only the profile
can read a window out of one), and store writes are the host's.

- No layer kind is mandatory for consumers: the inscription declares what the
  stele contains, and the client selects which layers to fetch and which data
  to source elsewhere. A kind the client does not implement is skipped and
  reported, per rule 3 above.
- The progress file (`stelae::plan::RestoreProgress`) records the inscription
  digest and the diffIds of completed layers, at a path the host chooses. Its
  resume rule is content-based: a layer is done when its `diffId` is
  recorded, which is a fact about bytes and not about the stele they were
  published in. *Which* layers may be skipped on resume at all is the
  profile's decision.
- Remaining-transfer accounting (`stelae::plan::Remaining`) is derived from
  the compressed sizes of the layers still to fetch — excluding layers
  already completed per the progress file — so resumed and deduplicated
  restores report correct totals.

## Signatures

Signatures are Ed25519 over the inscription digest, pushed as OCI referrer
artifacts (`application/vnd.stelae.signature.v1`, cosign-compatible envelope
where convenient), enforced k-of-n against a client-configured trusted-key
set. **Status: specified, not yet implemented** — no signing or verification
code exists in this repository yet; nothing else in this document is
aspirational.

## Compatibility

Three version axes move independently and must be kept coherent:

1. **Protocol schema** — the inscription's `schema` and the envelope media
   types' `.v1`. Verifiers fail closed on versions they do not implement.
2. **Profile version** — `profile.version`; a major the client does not
   implement is a clean refusal.
3. **Payload media-type versions** — the `.v{n}` in each payload type,
   vendor-owned. Within a version, a profile is expected to treat `.v{n}` as
   a contract on record content rather than an exact-byte pin, with the
   profile's schema-revision map (in `parameters`) pinning the bytes; the
   details are each profile's to specify.

## Implementation notes

The `stelae` crate is the protocol implementation this document is normative
for; `stelae-driver` layers profile-generic lifecycle machinery (chained
publishing, restore budget/checkpoint, preflight, retry) on top of it and
adds no wire surface. The boundary proof is `stelae/tests/toy_profile.rs`: a
second, trivial profile implemented against the protocol surface alone.
