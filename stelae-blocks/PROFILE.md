# The `io.txpipe.cardano.blocks` profile

Raw Cardano blocks, one layer per epoch, for any client. A layer carries each
block's slot, header hash and body as the node stores it, and nothing a
particular node implementation derives from them. Any node can produce it and
any client can read it.

This document is the profile's normative half; the protocol's is
[`SPEC.md`](../SPEC.md). The profile was adopted by decision 0080, which also
gives the layout below that every Cardano profile shares. The crate
[`stelae-blocks`](src/lib.rs) implements it against `stelae` and
`stelae-driver` alone.

## Identity

| Field | Value |
| --- | --- |
| Profile name | `io.txpipe.cardano.blocks` |
| Profile version | `1` |
| Layer kinds | `blocks` |
| Media type | `application/vnd.txpipe.stelae.blocks.v1+zstd` |
| Repository | `cardano/<network>/blocks` |
| `parameters` | `{}` |
| Compression | `zstd`, level 9 |

The vendor token is `txpipe`. Compression is transport policy: a layer's
identity is the digest of its uncompressed bytes.

## Numbering and the cut

Every Cardano profile numbers its snapshots the same way (decision 0080,
point 7):

- Sequence N is the cut at the boundary that opens epoch N. The snapshot at N
  holds what came before that boundary: the blocks of epochs 0 to N−1.
- Its tag is `epoch-N`, with no zero padding. `latest` follows the newest
  `epoch-N`.
- `position` is `{"networkMagic": m, "epoch": N}`, with `epoch` equal to the
  sequence.

There is no snapshot at sequence 0: the cut that opens epoch 0 holds nothing.
A published lineage starts at `epoch-1`.

## Diff and tip

Every layer goes in the inscription's `diff`. A blocks snapshot has no tip:
a closed epoch's blocks are final, so nothing in this profile is true only at
the cut.

- A snapshot that extends a lineage adds one layer, for epoch N−1.
- The first snapshot of a lineage lists one layer for each epoch before its
  cut. A lineage may start at any sequence, which keeps an export of a single
  boundary usable for tests and local use.

## The `blocks` layer

One layer covers one whole epoch (decision 0080, point 9). An epoch is a
whole number of the node's immutable files, so a layer's records are exactly
the blocks of those files, and a file's bodies can be checked against a
digest of that file.

### Scope

| Where | Form |
| --- | --- |
| Layer header | `[network_magic, epoch, start_slot, end_slot]` |
| Inscription descriptor | `{"epoch": e, "startSlot": s, "endSlot": t}` |

Both slot bounds are inclusive and are the epoch's own first and last slot,
not those of its first and last block. The descriptor leaves the network out:
the inscription's `position` names it.

### Records

```
[slot: uint, hash: bytes(32), body: bytes]
```

- `slot` is the block's slot, inside the scope's bounds.
- `hash` is the block's header hash.
- `body` is the block as the node stores it, verbatim. It is never decoded and
  re-encoded.

Records run in ascending slot. Blocks that share a slot keep the order the
chain gave them: a Byron epoch boundary block shares its slot with the block
after it. A reader refuses a slot that goes backwards or falls outside the
scope.

An epoch with no blocks is a layer holding its header record alone.

## Decided, not yet built

Decision 0080 also gives this profile a `digests` layer per epoch: the three
digests Mithril's Cardano DB v2 certifies for each of the epoch's immutable
files. With it, a publisher hashes each file's bodies and refuses to publish
when the result is not the certified digest. Neither the kind nor the check
exists yet, and no snapshot of this profile carries a `digests` layer.
