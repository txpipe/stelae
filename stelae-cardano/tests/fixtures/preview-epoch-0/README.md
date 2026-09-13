# Preview epoch-zero replay fixture

This fixture is the first 4,958 public Cardano Preview blocks, from origin
through the first epoch transition. It is enough to make both publisher hosts
execute real Byron replay and the epoch-boundary state transition without a
network dependency.

The blocks were acquired from `relay.cnode-m1.demeter.run:3002` on 2026-09-13
with Dolos revision `1ae4e91c18a9e1456a3612af402d7b9b97546d30`. The bounded
capture was interrupted after its first 1,000-block request and resumed with a
4,000-block request; intersection overlap left 4,958 canonical blocks.
`000000.segment` is the Dolos v4 flat-file
format: one independently checksummed zstd frame per raw block, using the
dictionary pinned by that Dolos revision. No store indexes or mutable state are
part of the fixture.

`byron.json`, `shelley.json`, `alonzo.json`, and `conway.json` are the public
Preview genesis inputs supplied by that same revision's `dolos init`. The test
loads the equivalent embedded genesis from the pinned dependency and verifies
the file identities recorded below, so deleting the old publisher command in a
later Dolos release does not delete either the fixture or its inputs.

Identities:

- segment sha256:
  `728c5aa9c7c78c4221bdee765f3cd109b630cb00266ab824f6db2bfa43276b4d`
- blocks: 4,958
- first: slot 0, block 0,
  `268ae601af8f9214804735910a3301881fbe0eec9936db7d1fb9fc39e93d1e37`
- boundary: slot 86,400, block 4,320,
  `4a9761ddc291b0c352d1712b624759132936a07b3e02d1d3bdaaf17b9abfe683`
- final: slot 99,140, block 4,957,
  `da6efb517e112a8439820d505286964bcc65ddd073b560a8f04d1c0902f1125f`
- old host: Dolos `1ae4e91c18a9e1456a3612af402d7b9b97546d30`
- protocol/profile: Stelae 0.2.0, `io.txpipe.dolos.cardano` v1
- first-publication predecessor/history: none / empty

The fixture contains only public chain protocol data and public network
configuration. It contains no credentials, registry state, or production data.
