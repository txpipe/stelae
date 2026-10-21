# syntax=docker/dockerfile:1.7.0@sha256:dbbd5e059e8a07ff7ea6233b213b36aa516b4c53c645f1817a4dd18b83cbea56

# Debian 13 glibc/libgcc and CA roots, without a shell or Dolos executable.
FROM gcr.io/distroless/cc-debian13@sha256:b2e43dc72acf58a146ed6feaf350806ae3bf4243eb5c68062d06bc13228ca2bd

ARG TARGETARCH
COPY --chmod=0755 bin/stelae-publisher-Linux-${TARGETARCH} /usr/local/bin/stelae-publisher
COPY genesis/ /etc/genesis/
COPY --chown=65532:65532 data/ /data/

USER 65532:65532
ENTRYPOINT ["/usr/local/bin/stelae-publisher"]
