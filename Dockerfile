# syntax=docker/dockerfile:1

# --- build ---------------------------------------------------------------
FROM golang:1.27-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 is required: modernc.org/sqlite is pure Go, and the final
# image has no C toolchain to link against anyway.
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/omdb-proxy ./cmd/omdb-proxy

# --- certs -----------------------------------------------------------------
# The golang:alpine build image doesn't install ca-certificates by
# default, so pull a known-good bundle from a minimal image that does.
FROM alpine:3.20 AS certs
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
