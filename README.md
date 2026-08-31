# omdb-proxy

A self-hosted caching proxy for [OMDb](https://www.omdbapi.com/) that sits between a handful of personal projects and the real API, so they can share one key and one cache instead of each burning through their own daily quota on the same movies.

## Why

OMDb's free tier allows **1000 requests/day**. Several small personal projects, each with its own OMDb client, end up looking up largely the same set of movies — the same well-known titles show up in more than one library — and each pays its own 1000/day for data that barely changes: a movie released in 2005 has the same rating, plot, and cast today as it did last week.

`omdb-proxy` holds **one** upstream API key and **one** shared cache. Every project talks to the proxy exactly as if it were OMDb itself — same query parameters, same JSON shape, same status codes — so switching a client over is a one-line change, and the proxy only spends real quota on an actual miss. With N projects behind it, a shared library that would cost `N × (library size)` requests spread across `N` separate 1000/day budgets instead costs `1 × (library size)` total, against **one** 1000/day key: a cold fill of a ~2000-movie library that used to take two days *per project* now takes about two days, once, for every project combined.

## Quick start

On a server, pull the published image — no source checkout needed, just the two files:

```bash
mkdir -p ~/docker/omdb-proxy/data && cd ~/docker/omdb-proxy

curl -O https://raw.githubusercontent.com/lepinkainen/omdb-proxy/main/compose.prod.yaml
curl -o .env https://raw.githubusercontent.com/lepinkainen/omdb-proxy/main/.env.example
# edit .env, set OMDB_API_KEY

docker compose -f compose.prod.yaml up -d
```

Create `data/` yourself, as above: Docker would create a missing bind-mount
source owned by root, which the non-root container can't write to.

Upgrading is `docker compose -f compose.prod.yaml pull` then `up -d`.

The cache DB is bind-mounted at `./data/cache.db`, so it survives upgrades and
`sqlite3 data/cache.db` works directly — worth keeping, since a fresh cache
refills at whatever OMDb's daily limit allows. The container runs as
`${PUID:-1000}:${PGID:-1000}`, so those files stay owned by you; set `PUID`
and `PGID` in `.env` if your account isn't `1000:1000`.

From a source checkout, `compose.yaml` builds the image locally instead:

```bash
cp .env.example .env
# edit .env, set OMDB_API_KEY

docker compose up -d
```

or without Docker:

```bash
export OMDB_API_KEY=your-real-key
go run ./cmd/omdb-proxy
```

It listens on `:8090` by default and stores its cache at `/data/cache.db` (or `./cache.db`-relative-to-whatever if you override `DB_PATH` outside a container).

## Pointing a client at it

Any OMDb client that lets you override the base URL can be pointed at this proxy with no other code changes — the query parameters and response shape are identical. For example, [moviepicker](../moviepicker) already has an `omdb.WithBaseURL` option:

```go
client := omdb.NewClient(
    apiKey, // any non-empty string; ignored unless PROXY_TOKEN is set
    omdb.WithBaseURL("http://omdb-proxy.local:8090"),
)
```

The client still sends an `apikey` query parameter, but the proxy discards it and substitutes its own upstream key — unless `PROXY_TOKEN` is configured, in which case the client's `apikey` (or an `Authorization: Bearer` header) has to match that token instead. See [Configuration](#configuration).

## Configuration

All configuration is environment variables; there is no config file.

| Variable | Default | Notes |
|---|---|---|
| `OMDB_API_KEY` | *(required)* | The proxy's own upstream key. The process refuses to start without it. Never logged, never echoed in a response. |
| `ADDR` | `:8090` | Listen address. |
| `DB_PATH` | `/data/cache.db` | SQLite database path. |
| `UPSTREAM_URL` | `https://www.omdbapi.com` | Overridable so tests and staging can point elsewhere. |
| `PROXY_TOKEN` | *(unset)* | If set, clients must present it as `apikey` or `Authorization: Bearer <token>`. **Left unset, this is an open proxy** — fine on a trusted LAN, not something to expose to the internet. |
| `NOTFOUND_TTL` | `168h` | Go duration string. TTL for `Response:"False"` misses. |

## Endpoints

- `GET /` with OMDb's usual query parameters (`i`, `t`, `s`, ...) — the proxy itself. Responses carry `X-Cache: HIT | MISS | STALE`.
- `GET /` with **no query string at all** — a human-readable HTML dashboard: quota remaining, cache row counts, recent entries. Ungated even when `PROXY_TOKEN` is set.
- `GET /stats` — JSON: quota used and remaining, cached row counts, and the hit/miss/stale counters since process start.
- `GET /healthz` — plain `ok`. Doesn't touch the database, so a slow or momentarily locked DB doesn't make the container look unhealthy.

## Logs

One `slog` line per request on stderr; `cache=MISS` marks the requests that actually spent upstream quota. No client key or proxy token ever reaches the log. See [`ai-docs/caching.md`](ai-docs/caching.md) for sample lines and log levels.

## Development

```bash
go build ./...
go vet ./...
go test ./...
```

Tests are hermetic — nothing talks to the real `omdbapi.com`.

## Background docs

- [`ai-docs/caching.md`](ai-docs/caching.md) — wire compatibility, caching policy and its traps, the cache key, and the SQLite schema.
- [`ai-docs/deployment.md`](ai-docs/deployment.md) — image build, GHCR workflows, compose files, volume and user choices.
