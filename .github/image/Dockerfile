# syntax=docker/dockerfile:1.7.0@sha256:dbbd5e059e8a07ff7ea6233b213b36aa516b4c53c645f1817a4dd18b83cbea56

# glibc/libgcc and CA roots, without a shell or Dolos executable.
FROM gcr.io/distroless/cc-debian12@sha256:e5d81ddde149641e2a9ba55be4545bc125c67de07508b03ba4c22e6eb0ded5aa

ARG TARGETARCH
COPY --chmod=0755 bin/stelae-publisher-Linux-${TARGETARCH} /usr/local/bin/stelae-publisher
COPY genesis/ /etc/genesis/
COPY --chown=65532:65532 data/ /data/

USER 65532:65532
ENTRYPOINT ["/usr/local/bin/stelae-publisher"]
