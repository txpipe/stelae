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

**Status: frozen, reopened by decision 0080.** Protocol version 1, as this
document describes it, is frozen with Dolos v2, and so is the Dolos profile at
version 1
([`crates/snapshot/PROFILE.md`](https://github.com/txpipe/dolos/blob/main/crates/snapshot/PROFILE.md)).
Decision 0080 changes the inscription in place before that release, at the
same versions and with no compatibility path: its layer list becomes the
`diff` and `tip` arrays, its `history` becomes the `previous` link, a
sequence is published under a diff manifest and a tip manifest, and a reader
resolves a snapshot by walking the links. An
implementation of either form refuses the other's documents. Changes after it
follow the [compatibility](#compatibility) rules: the inscription schema and
the envelope media types stay fail-closed, so any change to them takes a
protocol version bump; only payload record content may grow within a version,
and only as that section describes.

## Overview

A Stelae **snapshot** is one published artifact set at one sequence point: a
set of compressed **layers** plus an **inscription** — a canonical JSON
document listing the uncompressed digest of each of its own layers, a link to
the inscription before it, and the profile, sequence and position that
identify what the snapshot is *of*. The
protocol owns framing, the signable document, transport,
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
a snapshot repository.

## Profiles, naming and media types

Envelope types are protocol-owned and shared by every profile; payload types
are vendor-owned:

| Role | Media type | Owner |
|---|---|---|
| Artifact type (a snapshot's manifest) | `application/vnd.stelae.snapshot.v1` | protocol |
| Config blob (the inscription) | `application/vnd.stelae.inscription.v1+json` | protocol |
| Signature (referrer artifact) | `application/vnd.stelae.signature.v1` | protocol |
| Layer payloads | `application/vnd.{vendor}.stelae.{kind}.v{n}+{codec}` | vendor |

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
   that cannot build a kind must not chain onto a snapshot carrying it, since
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

The inscription is the canonical, signable document of a snapshot: the OCI
config blob, canonical JSON per RFC 8785 (JCS). Generic keys plus three
profile-owned opaque objects — `position`, `parameters` and each layer's
`scope`. Its two layer arrays and its link are decision 0080's:

```json
{ "schema": 1,
  "profile": {"name": "io.txpipe.dolos.cardano", "version": 1},
  "sequence": 550,
  "previous": {"sequence": 549, "inscriptionDigest": "sha256:…"},
  "position": { "…": "profile-owned" },
  "parameters": { "…": "profile-owned" },
  "compression": {"algo": "zstd", "level": 9},
  "diff": [
    {"kind": "log-epochs", "mediaType": "application/vnd.dolos.stelae.log-epochs.v1+zstd",
     "diffId": "sha256:…", "records": 2, "uncompressedSize": 1840,
     "scope": { "…": "profile-owned" }} ],
  "tip": [
    {"kind": "state-utxos", "mediaType": "application/vnd.dolos.stelae.state-utxos.v1+zstd",
     "diffId": "sha256:…", "records": 812004, "uncompressedSize": 98123456,
     "scope": { "…": "profile-owned" }} ] }
```

- `schema` is the protocol's own version; verifiers fail closed on anything
  but the versions they implement.
- `sequence` is the protocol's monotonic ordering key. What it counts is the
  profile's decision (the Dolos profile sets it to the epoch).
- `previous` links to the inscription before this one; see
  [the link](#the-link).
- `position`, `parameters` and `scope` are canonicalized by JCS like every
  other key, so determinism holds without the protocol interpreting them.
  Verifiers reject unknown *generic* keys at the top level and inside the
  generic objects, so extension happens only inside those three objects.
- Determinism and signing are defined only over this document's sha256.
- Restore verifies registry blob digests (transport integrity) and diffIds
  (canonical identity).

### Diff and tip

An inscription lists only its own layers, in two arrays, and both are always
present:

- `diff` holds the layers every later snapshot of the lineage inherits.
- `tip` holds the layers that belong to this snapshot alone. It may be empty.

The layer set of the snapshot at sequence N is the `diff` of every
inscription of its lineage up to N, plus the `tip` of N. A diff only adds,
and a tip is not part of it. Every later snapshot is made of a lineage's
`diff` layers, so they are kept for as long as the lineage is; no later
snapshot uses a `tip`, so which tips are kept is the registry's policy.

The profile assigns each layer to one array. A layer goes to `diff` once its
content is final; until then, each snapshot carries the layer's current form
in `tip`. A kind alone does not decide the array. JCS preserves array order,
so the order within each array is normative, and the profile fixes it.

**Layer invariant:** a `(kind, scope)` pair appears at most once across
`diff` and `tip` together, scopes compared by their canonical JSON. Verifiers
reject an inscription that lists one twice.

**Append-only:** no inscription replaces or amends a layer of an earlier one.
A correction to published data starts a new lineage at a new repository path.

### The link

`previous` names the inscription this one follows, by its `sequence` and its
`inscriptionDigest`. That inscription carries its own link, so the latest
signed inscription transitively attests every inscription of its lineage, at
a cost that does not grow with the chain. Attestation outlives blob retention:
a snapshot whose blobs have been reclaimed can still be verified by anyone
holding a copy of it — follow the links back from the latest signed
inscription, through the inscriptions in between, to that sequence, compare
the digest, then check the layers against their diffIds. No external trusted
storage of attestations is required.

**Link invariant:** `previous` is omitted from the first inscription of a
lineage, and is never `null`. A lineage may start at any sequence; its first
inscription then lists in `diff` everything final up to its cut. When
`previous` is present, its `sequence` is lower than this inscription's.
Verifiers reject an inscription that violates this.

A publisher holds itself to the stricter rule: a new inscription links to the
repository's latest snapshot, at `sequence - 1`, and a publish that would
leave a gap or repeat a sequence is refused, naming both sequences. The
protocol does not hold a verifier to that rule, because how a lineage is
numbered is its publisher's schedule. Independent publishers converge on
byte-identical inscriptions only if they link to the same predecessor: the
link is part of the identity.

A side-effect of anchoring identity on uncompressed content digests is that
layer *content* can be mirrored over any content-addressed transport — or
re-compressed with a different algorithm — and still be verified against the
same signed inscription via diffIds. Consumption is stricter than
verification: the restore client expects the canonical blobs referenced by
the OCI manifest, so re-encoded mirrors serve archival and verification, not
direct restore.

### What a publish writes

A publish uploads and lists only the layers its profile assigns to its own
sequence, each in the array the profile names for it at that sequence. It
never re-lists a layer of an earlier inscription: that layer stays where the
inscription that first listed it put it, and a later snapshot reaches it by
following the links. So a publish's work and its documents grow with what one
sequence adds, not with the length of the chain (decision 0080).

A publish that resumes after an interruption may name a blob that an earlier
attempt at the same sequence already uploaded, once the registry confirms it
still holds that blob. It is reusing its own layer, not an earlier
snapshot's.

### Resolving the chain

Restore and verification read a snapshot whole, so they walk its chain. From
the inscription at N, a reader follows `previous` to the inscription that has
none, and refuses the snapshot at the first link that fails one of these, in
order:

1. the transport holds a document for the link;
2. that document is not an inscription the walk already holds, which would
   loop;
3. its digest is the link's `inscriptionDigest`;
4. its `sequence` is the link's, so sequences strictly descend;
5. it is an inscription of the profile being read, at a major version the
   reader implements.

The walk ends at the first inscription without a link, at whatever sequence
that is: a lineage may start late.

The layer set is then the `diff` of every inscription of the chain in
ascending sequence, then the `tip` of N, in each array's order. A `(kind,
scope)` pair listed twice in that set is refused, naming both sequences. An
earlier inscription's `tip` is not part of the set, so a layer that a tip
carried while it was not final may appear later in a `diff`, final.

Every layer belongs to the inscription that lists it. A reader interprets it
under that inscription's `parameters` (its schema map included) and fetches
it under that inscription's `compression`, never under the head's.

## OCI layout

- One repository per (profile, dataset). Every manifest in it carries
  `artifactType: application/vnd.stelae.snapshot.v1`, the envelope that names
  a Stelae snapshot, the profile's layer media types, and three annotations
  per layer, named below.
- A sequence is published under two manifests that carry the same
  inscription as their config (decision 0080). The **diff manifest** lists the
  inscription's `diff`, and the **tip manifest** lists its `tip`. A sequence
  whose `tip` is empty has no tip manifest. The diff manifest is never
  deleted. The tip manifest, and the blobs only it names, may be, as the
  registry's policy.

### Tags

| Tag | Names | Written |
| --- | --- | --- |
| `<tag>` | the sequence's diff manifest | once, by the sequence's publish |
| `<tag>.tip` | the sequence's tip manifest | once, by the same publish, when `tip` is not empty |
| `latest` | the newest sequence's diff manifest | moved by every publish |

`<tag>` is the tag the profile renders for the sequence. The Cardano profiles
render `epoch-N`, with no zero padding and no profile or lineage name, so
their tags are `epoch-N`, `epoch-N.tip` and `latest`.

The protocol owns the `.tip` suffix and appends it. A profile renders only the
sequence's tag. A rendered or moving tag that ends in `.tip` is refused, and a
rendered tag must leave room for the suffix within OCI's 128 characters. A
reader asking for a tag that ends in `.tip` is refused too, because such a tag
names one array of a snapshot and not the snapshot.

A publish writes, in this order:

1. the layer blobs;
2. the inscription, as the config blob of both manifests;
3. the tip manifest, under `<tag>.tip`;
4. the diff manifest, under `<tag>`;
5. `latest`, moved to the diff manifest.

So an existing `<tag>` implies its tip was written, and `latest` never names a
sequence whose blobs or tip were not up. Each step writes the same bytes when
it is repeated, so a publish that stopped part way is finished by sealing or
publishing the same sequence again.

A reader resolves `<tag>`, or `latest`, to the diff manifest and reads the
inscription it names: the head. When the head's `tip` is not empty, the
reader also reads the tip manifest, under the tag the profile renders for the
head's `sequence`, plus `.tip`. To read the snapshot whole it then walks the
chain ([resolving the chain](#resolving-the-chain)): for each link, the diff
manifest under the tag the profile renders for the link's `sequence`, and the
inscription that manifest names, each manifest held to its inscription's
`diff`. It reads no tip manifest but the head's. The identity→blob maps and
compressed sizes of every manifest read make up the snapshot's.

Tags are predictable, so a reader need not wait on one link to ask for the
next: it may read the sequences below a link together, and drop those the
chain does not reach, absent or not.

Three failures stay distinct:

- **an absent head**: the repository holds no manifest under the tag asked
  for. This alone is absence;
- **a gone tip**: the head's tip manifest is missing beside its diff
  manifest. The diff stays, every later snapshot is built on it, and only a
  restore at this sequence is lost;
- **a document missing inside the chain**: a link names a sequence whose
  diff manifest or inscription the repository does not hold. The head is
  published, and later snapshots may be, but none whose chain passes the gap
  can be resolved.

### The manifest

Each manifest of a sequence is an OCI image manifest, and its shape is
closed: a conforming publisher writes exactly the fields below, and a
conforming client refuses anything else.

- `schemaVersion: 2`; `mediaType: application/vnd.oci.image.manifest.v1+json`;
  `artifactType: application/vnd.stelae.snapshot.v1`.
- `config` is the inscription's descriptor, the same in both manifests:
  `mediaType` is `application/vnd.stelae.inscription.v1+json`, `digest` is the
  sha256 of the canonical inscription bytes — the same digest independent
  parties reproduce and sign — and `size` is those bytes' length.
- `layers`: one descriptor per layer of the one array the manifest lists,
  **in that array's order**. The diff manifest lists every `diff` layer, and
  the tip manifest every `tip` layer. Each descriptor carries the layer's
  `mediaType` exactly as the inscription states it, the compressed blob's
  `digest` and `size`, and the three annotations below.
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
hands over for free. A directory has no manifest to carry it, so a publish
writes the same map to a `blobs.json` sidecar beside `inscription.json`; it
is transport and not identity — unsigned, outside the inscription digest, and
fully recoverable from the blobs — so a snapshot without one still restores, by
decompressing every blob to rebuild it. A client that does not read it cannot
fetch a layer; it is the one annotation a reader must understand. The other
two exist so a human or a generic registry tool can see what a blob covers
without fetching the config blob, and a client may ignore them.

### Manifest–inscription agreement

The manifests and the inscription are views of one snapshot — the
inscription holds identity, the manifests hold transport — and a disagreement
between them, in either direction, is a refusal, never a preference.
Agreement is per array: the diff manifest is held to `diff` and the tip
manifest to `tip`.

A publisher refuses to build the manifests — before anything is pushed — when
the inscription describes a layer that was never written, or a layer was
written that neither array describes: a blob nothing attests must not be
published.

A client refuses a manifest — before any of its layer blobs is fetched — when:

- `artifactType` is missing. This fails closed by choice: a registry that
  strips the OCI 1.1 discovery field has published something a client cannot
  recognize as a snapshot, and reading it anyway would make the discovery
  contract advisory.
- `artifactType` is present and is not `application/vnd.stelae.snapshot.v1`.
  The envelope identifier releases before v2 wrote is one such value: it has
  no alias and no migration path, and a manifest bearing it is refused like
  any foreign type.
- the config descriptor's media type is not the inscription's.
- the config descriptor's digest is not the inscription's. A tip manifest
  under a sequence's `.tip` tag that names another inscription is not that
  sequence's tip.
- the manifest's layer count differs from its array's.
- a layer carries no `store.stelae.layer.diffId` annotation, so nothing says
  which layer it holds.
- a layer's `diffId` annotation disagrees with its array's layer *at that
  position*. Positional correspondence is a check of its own: a manifest
  carrying the right blobs in the wrong order passes the map and fails the
  order.
- a layer's media type disagrees with its array's at that position.

### The manifest size ceiling

A manifest past **4 MiB** (`stelae::MANIFEST_SIZE_LIMIT`) is refused before
the push, and each manifest of a sequence is held to it on its own, before
either is pushed. The figure is not a limit the OCI specification imposes; it
is the ceiling registries converge on, and the refusal is measured on the
exact canonical bytes that would have been pushed, so it names the document
and its layer count instead of arriving later as a registry's `413`. A
manifest lists one array of one inscription, so its size depends on what one
sequence adds and not on the length of the chain. A descriptor
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

A client reads a snapshot in a fixed order, refusing early: resolve the tag to
the head's diff manifest and, when its `tip` is not empty, its tip manifest;
walk the chain to its first inscription
([resolving the chain](#resolving-the-chain)), reading each linked sequence's
diff manifest and inscription; refuse any manifest–inscription disagreement
(above, before any layer blob is fetched); verify each inscription's digest
(the head's against its config descriptor, each linked one against its link
as well), `schema`, profile name and major version, and signatures; plan over
the layer set; then fetch, verifying each blob's registry digest (transport
integrity) and its `diffId` (canonical identity).

The protocol's half of a restore is planning and resume; layer selection is
profile-side by necessity (a layer's `scope` is opaque, so only the profile
can read a window out of one), and store writes are the host's.

- No layer kind is mandatory for consumers: the chain's inscriptions declare
  what the snapshot contains, and the client selects which layers of the set
  to fetch, in the order it chooses, and which data to source elsewhere. A
  kind the client does not implement is skipped and reported, per rule 3
  above.
- The schema map a restore checks (see
  [profiles, naming and media types](#profiles-naming-and-media-types)) is per
  inscription: each inscription's `parameters` describe the layers it lists,
  so a client checks the map of every inscription whose layers it reads.
- The progress file (`stelae::plan::RestoreProgress`) records the inscription
  digest and the diffIds of completed layers, at a path the host chooses. Its
  resume rule is content-based: a layer is done when its `diffId` is
  recorded, which is a fact about bytes and not about the snapshot they were
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
   details are each profile's to specify. Within a payload version, writers
   may append numbered optional fields and readers skip fields they do not
   know; field indexes are never renumbered or repurposed; anything else —
   an added enum variant included — is a breaking change that takes a
   version bump.

These additive rules apply to payload record content only. The inscription
and the envelope types are not extensible within protocol version 1:
verifiers reject unknown generic top-level inscription keys, and extension
happens only inside `position`, `parameters` and `scope`.

## Implementation notes

The `stelae` crate is the protocol implementation this document is normative
for; `stelae-driver` layers profile-generic lifecycle machinery (chained
publishing, restore budget/checkpoint, preflight, retry) on top of it and
adds no wire surface. The boundary proof is `stelae/tests/toy_profile.rs`: a
second, trivial profile implemented against the protocol surface alone.
