# Deployment and build pipeline

Background for the image build and the VPS deployment. The compose files,
Dockerfile, and workflows stay terse; the reasoning lives here.

## Two compose files

`compose.yaml` builds from the checkout and is for local work. It keeps a
named volume and runs as root — fine for a throwaway dev cache.

`compose.prod.yaml` pulls `ghcr.io/lepinkainen/omdb-proxy:latest` and needs no
source tree: that file plus a `.env` is the entire deployment. It lives in
`~/docker/omdb-proxy/` on the VPS, matching the one-directory-per-app layout
used for every container there.

Upgrades are `docker compose -f compose.prod.yaml pull` then `up -d`.

## Bind mount, not a named volume

Production mounts `./data:/data`, so the cache DB sits at
`~/docker/omdb-proxy/data/cache.db`.

The cache is the product — a fresh one refills at OMDb's own daily limit per
day, so it must survive upgrades, and it is worth being able to inspect. A host
path can be opened with `sqlite3 data/cache.db` and is picked up by whatever
backs up `~/docker`. A named volume needs a helper container for both:
`docker run --rm -v omdb-proxy-data:/data -v $PWD:/out alpine cp ...`.

## Non-root user

The service runs as `${PUID:-1000}:${PGID:-1000}`, overridable from `.env`.

A root container would leave root-owned `cache.db`, `cache.db-wal`, and
`cache.db-shm` on the host, which defeats the point of choosing an inspectable
path. `distroless/static-debian12` has no `/etc/passwd` entry for an arbitrary
UID, and does not need one: nothing in the proxy resolves a user name, and
SQLite only needs write permission on `/data` — write permission on the
*directory*, not just the file, because WAL mode creates the two sidecars
alongside it.

Consequence worth remembering: `data/` must exist before the first `up`.
Docker creates a missing bind-mount source owned by root, and the non-root
container then cannot write to it. The failure looks like a startup error
opening the database, not a permissions message from Docker.

## Cross-compilation instead of emulation

The Docker build stage is pinned with `FROM --platform=$BUILDPLATFORM` and
takes `GOOS`/`GOARCH` from buildx's `$TARGETOS`/`$TARGETARCH`. Go cross-compiles
natively, so the arm64 image is produced by the amd64 runner at full speed.

Dropping the `--platform` pin still produces a correct image, but silently
switches the arm64 build to QEMU emulation and turns a ~2 minute job into a
long one. `CGO_ENABLED=0` (already required by pure-Go `modernc.org/sqlite`) is
what keeps the cross-compile a matter of two environment variables.

The `certs` stage is pinned the same way: the CA bundle it copies is
architecture-independent.

## Workflows

`ci.yml` runs gofmt, `go vet`, `go build`, and `go test -race` on pushes and
pull requests. The `-race` flag is not optional here: the singleflight
deduplication and quota-reservation tests are the ones that would catch a
regression in cache-fill behaviour, and they only fail reliably under it.

`docker.yml` builds `linux/amd64` and `linux/arm64` and pushes to GHCR on
`main` and on `v*` tags. Published tags on `ghcr.io/lepinkainen/omdb-proxy`:

| Tag | Points at |
| --- | --- |
| `latest` | the most recent build of `main` |
| `main` | the same thing, named by branch |
| `sha-<full-sha>` | one exact commit, for pinning or rolling back |
| `1.2.3`, `1.2` | a pushed `v1.2.3` git tag |

Pull requests build without pushing, because a fork's `GITHUB_TOKEN` has no
write access to the registry — the build still proves the Dockerfile works.

The package inherits repository visibility on first publish. If the repo goes
private, either make the package public from its GitHub page or log the VPS in
with a PAT carrying `read:packages`:

```bash
echo "$GITHUB_TOKEN" | docker login ghcr.io -u lepinkainen --password-stdin
```
