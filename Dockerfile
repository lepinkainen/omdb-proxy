# syntax=docker/dockerfile:1

# --- build ---------------------------------------------------------------
# --platform=$BUILDPLATFORM pins this stage to the machine doing the building
# rather than the target architecture. Go cross-compiles natively, so the
# arm64 image is produced by an amd64 runner at full speed with no QEMU
# emulation in the loop.
FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# TARGETOS/TARGETARCH are supplied by buildx for each requested platform;
# they default to the build host's own values for a plain `docker build`.
ARG TARGETOS=linux
ARG TARGETARCH

# CGO_ENABLED=0 is required: modernc.org/sqlite is pure Go, and the final
# image has no C toolchain to link against anyway. It is also what makes the
# cross-compile above a plain environment-variable change.
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -o /out/omdb-proxy ./cmd/omdb-proxy

# --- certs -----------------------------------------------------------------
# The golang:alpine build image doesn't install ca-certificates by
# default, so pull a known-good bundle from a minimal image that does.
FROM --platform=$BUILDPLATFORM alpine:3.20 AS certs
RUN apk add --no-cache ca-certificates

# --- runtime ---------------------------------------------------------------
# distroless/static has no shell and no package manager, which is exactly
# right for a static Go binary — smaller attack surface, nothing to patch.
FROM gcr.io/distroless/static-debian12

# The upstream call to omdbapi.com is HTTPS, and distroless/static ships
# no CA bundle of its own; without this, every upstream fetch fails at
# the TLS handshake with "x509: certificate signed by unknown authority".
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY --from=build /out/omdb-proxy /omdb-proxy

VOLUME ["/data"]
EXPOSE 8090

ENTRYPOINT ["/omdb-proxy"]
