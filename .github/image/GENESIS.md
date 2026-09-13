# Bundled Cardano genesis

The mainnet, preprod and preview JSON files in `genesis/` are copied verbatim
from `txpipe/dolos` revision
`1ae4e91c18a9e1456a3612af402d7b9b97546d30`, under
`.github/image/genesis/`.

The same bytes are embedded by `dolos-cardano` behind its `include-genesis`
feature. Its `include::{mainnet,preprod,preview}` modules expose `load()` and
`save()`. We vendor the files here to preserve the publisher's configured
`/etc/genesis/<network>/` paths without fetching them during image assembly.
These network inputs do not need updating when the Dolos code pin changes.
