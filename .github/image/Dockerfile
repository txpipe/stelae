# syntax=docker/dockerfile:1

# glibc/libgcc and CA roots, without a shell or Dolos executable.
FROM gcr.io/distroless/cc-debian12@sha256:e5d81ddde149641e2a9ba55be4545bc125c67de07508b03ba4c22e6eb0ded5aa

ARG TARGETARCH
COPY --chmod=0755 bin/stelae-publisher-Linux-${TARGETARCH} /usr/local/bin/stelae-publisher
COPY genesis/ /etc/genesis/

ENTRYPOINT ["/usr/local/bin/stelae-publisher"]
