# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### 🚀 Features

- `SteleReader::stream_layers` streams the layers a caller names, in that order, opening each `LayerReader` when it is taken. A registry `Stele` pulls up to `ahead` blobs ahead of the caller as tasks on the transport's runtime, and dropping the stream cancels them; other readers, and a depth of 0 or 1, read one layer at a time as `stream_layer` does. Each read retry's `Event::Retry` reason now starts with the blob or manifest it retries.
- [**breaking**] Retry failed and truncated registry reads with a longer backoff: manifest, config and layer `GET`s get `Options::read_attempts` (default 6) with waits from `Options::read_delay` (2 s) doubling up to 60 s, also retry a body that fails mid-transfer, a blob short of its descriptor and a digest mismatch, and report exhaustion as `Error::RetriesExhausted`. That message and each `Event::Retry` reason spell out the failure's whole cause chain. Publish round trips keep `Options::attempts`.

## [0.5.2] - 2026-11-04

### 🚀 Features

- Support opt-in direct S3 uploads to Zot blob storage (#18)

## [0.5.1] - 2026-11-03

### 🚀 Features

- [**breaking**] Pin the frozen Dolos v2 profile at e39a38a3 (#15)
- [**breaking**] Name the OCI envelope a snapshot (#17)
- [**breaking**] Refuse the `.stele.` infix and freeze SPEC at protocol v1 (#16)

### 🐛 Bug Fixes

- *(deps)* Move the dolos pin to c5e4f77e and clear the cargo deny advisories (#14)

### Release

- V0.5.1

## [0.5.0] - 2026-11-01

### 🚀 Features

- Mount the publisher's /data from an optional persistent claim (#10)
- `stelae-publisher digest | verify | inspect` replace `dolos snapshot` (#13)

### 🐛 Bug Fixes

- *(deps)* Move to zstd 0.14 and update tokio (#12)

### Release

- V0.5.0

## [0.4.0] - 2026-10-30

### 🚀 Features

- [**breaking**] Name payload media types `.stelae.`, accepting `.stele.` for one release (#8)
- [**breaking**] Move the publisher onto the Dolos v2 configuration (#9)

### Release

- V0.4.0

## [0.3.1] - 2026-10-21

### ⚙️ Miscellaneous Tasks

- Raise rustls trust components and move the publisher base to cc-debian13 (#7)

### Release

- V0.3.1

## [0.3.0] - 2026-09-14

### 🚀 Features

- Add Kubernetes deployment units (#2)
- Add Stelae-owned Cardano publisher host (#3)

### 🔧 Continuous Integration

- Package the Stelae publisher image and deployment chart (#5)

### ⚙️ Miscellaneous Tasks

- Prove Stelae publisher host parity (#4)
- Pin cleaned Dolos publisher graph (#6)

### Release

- V0.3.0

## [0.2.0] - 2026-09-02

### 📚 Documentation

- The registry gate lives here by responsibility, not leftovers

### ⚡ Performance

- *(dir)* A directory states its blob map instead of rebuilding it (#1)

### ⚙️ Miscellaneous Tasks

- Adopt the dolos release flow (cargo-release + git-cliff)

### Release

- V0.2.0

### Spec

- Stand alone — why-OCI, retention policy, the client read order
- State the schemas gate precisely — presence, with zero as absence

## [0.1.0] - 2026-09-02

### 🚀 Features

- *(stelae)* Setup a walking skeleton for the Stelae protocol (#1147)
- *(stelae)* Read layers as a stream, bounded by one record (#1148)
- *(stelae)* Write layers as a stream, bounded by one record (#1167)
- *(stelae)* Push and pull a stele to an OCI registry (#1170)
- *(snapshot)* Publish a stele into a registry, and only what is new (#1173)
- *(snapshot)* Restore from a registry, and survive being interrupted (#1176)
- *(stelae)* Authenticate a registry with basic credentials (#1184)
- *(stelae)* Give a publisher digest, verify and inspect capabilities (#1188)
- *(stelae)* Run preflight rules before publish / restore (#1192)
- *(stelae)* Support resume of interrupted publish (#1200)
- *(stelae)* Introduce restore / publish progress reports (#1210)
- *(stelae)* Make an unknown layer kind skippable at restore, strict at publish (#1242)
- *(snapshot)* Retain state dumps at configured epochs (#1246)

### 🐛 Bug Fixes

- *(stelae)* A registry client should not need a C toolchain (#1180)
- *(stelae)* Hold a publisher to the record ceiling its reader enforces (#1182)
- *(stelae)* Specify the manifest third parties read, and prove it in CI (#1183)
- *(stelae)* A blob that already exists is a dedup, not a rename (#1187)
- *(stelae)* Improve error messages on staging dir preflight checks (#1202)
- *(stelae)* Retry a read the OS interrupted instead of failing the layer (#1254)

### 🚜 Refactor

- *(stelae)* Improve DRY on layer opening prologue (#1201)
- *(stelae)* Move the dolos-free residue out of the profile (#1285)
- *(stelae)* Make the chained-publish lifecycle profile-generic (#1286)
- *(stelae)* Make the snapshot and restore commands parse and render (#1288)
- *(stelae)* Close out the repo-split leftovers (#1290)

### ⚡ Performance

- *(stelae)* Move a publish's layer round trips concurrently (#1260)
- *(snapshot)* Produce a publish's layers from a pool of workers (#1272)
- *(stelae)* Send 4 MiB upload chunks instead of 1 MiB (#1273)
- *(stelae)* Push a layer that fits as one request (#1277)

### 🧪 Testing

- *(stelae)* Point the oci harness at a deployed registry (#1185)

### ⚙️ Miscellaneous Tasks

- Stand the extracted crates up as their own workspace

<!-- generated by git-cliff -->
