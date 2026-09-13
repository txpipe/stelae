# syntax=docker/dockerfile:1.7.0@sha256:dbbd5e059e8a07ff7ea6233b213b36aa516b4c53c645f1817a4dd18b83cbea56

# The runtime is multi-architecture and immutable. `cc-debian12` supplies the
# glibc/libgcc runtime and CA bundle needed by the Rust binary, but no shell and
# no Dolos executable.
FROM gcr.io/distroless/cc-debian12@sha256:e5d81ddde149641e2a9ba55be4545bc125c67de07508b03ba4c22e6eb0ded5aa

ARG TARGETARCH
ARG BUILD_CREATED
ARG BUILD_REVISION
ARG BUILD_VERSION

COPY --chmod=0755 bin/stelae-publisher-Linux-${TARGETARCH} /usr/local/bin/stelae-publisher

# Keep the genesis paths used by the existing publisher configuration. Every
# public input is fetched from the Dolos revision pinned by stelae-cardano and
# independently checksum-locked so the image build cannot accept drift.
ADD --checksum=sha256:4f28b3b437b2c4f6ee26cc70964b3a5f1a274b0b3909c31535091c00316c13aa https://raw.githubusercontent.com/txpipe/dolos/1ae4e91c18a9e1456a3612af402d7b9b97546d30/.github/image/genesis/mainnet/byron.json /etc/genesis/mainnet/byron.json
ADD --checksum=sha256:59cd3932c6dd792bc5020ca3336064a8faabde4e4a8dc7d143ff4df6eec36961 https://raw.githubusercontent.com/txpipe/dolos/1ae4e91c18a9e1456a3612af402d7b9b97546d30/.github/image/genesis/mainnet/shelley.json /etc/genesis/mainnet/shelley.json
ADD --checksum=sha256:3511a116de9496a84f956ac50c90dbe92e95d0fbee7b2d7134fb4d407efb9d0e https://raw.githubusercontent.com/txpipe/dolos/1ae4e91c18a9e1456a3612af402d7b9b97546d30/.github/image/genesis/mainnet/alonzo.json /etc/genesis/mainnet/alonzo.json
ADD --checksum=sha256:ae9d00e4dd9a60814f541005e061ceeb93248712345cb7bbf6a15b887143acdb https://raw.githubusercontent.com/txpipe/dolos/1ae4e91c18a9e1456a3612af402d7b9b97546d30/.github/image/genesis/mainnet/conway.json /etc/genesis/mainnet/conway.json

ADD --checksum=sha256:d88fbffdf78daaccfaddf504e95840c73ce527c06fa4140aefb55d3f91c00cef https://raw.githubusercontent.com/txpipe/dolos/1ae4e91c18a9e1456a3612af402d7b9b97546d30/.github/image/genesis/preprod/byron.json /etc/genesis/preprod/byron.json
ADD --checksum=sha256:4b9d32c09159c2948e4386ba1f59db5a249a89b43b84dfd8368f465e650095de https://raw.githubusercontent.com/txpipe/dolos/1ae4e91c18a9e1456a3612af402d7b9b97546d30/.github/image/genesis/preprod/shelley.json /etc/genesis/preprod/shelley.json
ADD --checksum=sha256:7333bfafe311589fa09e8bf59a47ec0d85a1959f00748cc0800591d2c7646408 https://raw.githubusercontent.com/txpipe/dolos/1ae4e91c18a9e1456a3612af402d7b9b97546d30/.github/image/genesis/preprod/alonzo.json /etc/genesis/preprod/alonzo.json
ADD --checksum=sha256:dbd8fbf16aafaf97a0894cb69db477fb658b3fec744ef89e8bd06e77aa482753 https://raw.githubusercontent.com/txpipe/dolos/1ae4e91c18a9e1456a3612af402d7b9b97546d30/.github/image/genesis/preprod/conway.json /etc/genesis/preprod/conway.json

ADD --checksum=sha256:101650cc8ccbd020f6ceae29bcee2aed5cbdb0bebaa26288d132a9dc3299d9cd https://raw.githubusercontent.com/txpipe/dolos/1ae4e91c18a9e1456a3612af402d7b9b97546d30/.github/image/genesis/preview/byron.json /etc/genesis/preview/byron.json
ADD --checksum=sha256:c5ccb45161676718a8c08b1362ec1ef2cee516fd123aecafacf0f3e4625a746a https://raw.githubusercontent.com/txpipe/dolos/1ae4e91c18a9e1456a3612af402d7b9b97546d30/.github/image/genesis/preview/shelley.json /etc/genesis/preview/shelley.json
ADD --checksum=sha256:3511a116de9496a84f956ac50c90dbe92e95d0fbee7b2d7134fb4d407efb9d0e https://raw.githubusercontent.com/txpipe/dolos/1ae4e91c18a9e1456a3612af402d7b9b97546d30/.github/image/genesis/preview/alonzo.json /etc/genesis/preview/alonzo.json
ADD --checksum=sha256:51ae5b86ae59f872462f22c5034b37d8f8b8561bb7a7ad5c6d9b45502ca65b9e https://raw.githubusercontent.com/txpipe/dolos/1ae4e91c18a9e1456a3612af402d7b9b97546d30/.github/image/genesis/preview/conway.json /etc/genesis/preview/conway.json

LABEL org.opencontainers.image.created="${BUILD_CREATED}" \
      org.opencontainers.image.description="Stelae-owned Cardano snapshot publisher" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.revision="${BUILD_REVISION}" \
      org.opencontainers.image.source="https://github.com/txpipe/stelae" \
      org.opencontainers.image.title="stelae-publisher" \
      org.opencontainers.image.version="${BUILD_VERSION}"

USER 0:0
ENTRYPOINT ["/usr/local/bin/stelae-publisher"]
