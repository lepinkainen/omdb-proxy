# omdb-proxy

A self-hosted caching proxy for [OMDb](https://www.omdbapi.com/) that sits between a handful of personal projects and the real API, so they can share one key and one cache instead of each burning through their own daily quota on the same movies.

## Why

OMDb's free tier allows **1000 requests/day**. Several small personal projects (each with its own OMDb client) end up looking up largely the same set of movies — the same well-known titles show up in more than one library. Run independently, each project pays its own 1000/day for data that barely changes: a movie released in 2005 has the same rating, plot, and cast today as it did last week.

`omdb-proxy` holds **one** upstream API key and **one** shared cache. Every project talks to the proxy exactly as if it were OMDb itself — same query parameters, same JSON shape, same status codes — so switching a client over is a one-line change. The proxy answers from cache whenever it can and only spends real quota on an actual miss.

### The quota arithmetic

With N projects each independently polling OMDb, the shared library costs `N × (library size)` requests to fully warm, spread across `N` separate 1000/day budgets. Behind this proxy, the same library costs `1 × (library size)` requests total, spread across **one** 900/day budget (see `DAILY_BUDGET` below) — because the second, third, and Nth project to ask about a given movie gets a cache hit instead of a fresh request. A cold fill of a ~2000-movie library that used to take two days *per project* now takes about two days, once, for every project combined.

## Wire compatibility

Requests arrive at `GET /` with OMDb's own query parameters — `i`, `t`, `s`, `y`, `type`, `plot`, `page`, `r`, `v`, `apikey` — and get OMDb's own response shape back, byte-for-byte. The proxy never parses a response into a struct and re-serialises it: different consumer projects want different fields (`Plot`, `Poster`, `Genre`, `Runtime`, ...), so the full body is cached and returned verbatim. The upstream `Content-Type` is preserved too.

One addition: every response carries an `X-Cache: HIT | MISS | STALE` header, purely for debugging. Extra headers don't break OMDb clients.

## Running it

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

Any OMDb client that lets you override the base URL can be pointed at this proxy with no other code changes — the query parameters and response shape are identical.

For example, [moviepicker](../moviepicker) already has an `omdb.WithBaseURL` option (added so tests can point the client at an `httptest` server):

```go
client := omdb.NewClient(
    apiKey, // any non-empty string; ignored unless PROXY_TOKEN is set
    omdb.WithBaseURL("http://omdb-proxy.local:8090"),
)
```

That's the whole migration. The client still sends an `apikey` query parameter (OMDb's protocol requires one), but the proxy discards it and substitutes its own upstream key — unless `PROXY_TOKEN` is configured, in which case the client's `apikey` (or an `Authorization: Bearer` header) has to match that token instead. See [Configuration](#configuration) below.

## Caching policy

Expiry is derived from the release year in the response body wherever there is one, and is deliberately skewed towards "cache forever" — the whole point of this proxy is to stop re-fetching movies whose metadata essentially never changes.

| Response | Expiry |
|---|---|
| Successful lookup, released more than a year ago | Permanent (`expires_at` is `NULL`) |
| Successful lookup, released within the last year | 7 days |
| Successful lookup, year unknown or in the future | 24 hours |
| Not-found miss (`Response:"False"`, ordinary error) | `NOTFOUND_TTL`, default 7 days — see below |
| Search query (`s=...`) | 30 days, regardless of whether it found anything |
| Quota-exceeded response | **Never cached** — see [Traps](#traps-this-proxy-handles-so-you-dont-have-to) |

**Why not-found misses get a finite TTL, not a permanent one:** a `Response:"False"` miss has two indistinguishable causes. Either the title genuinely has no OMDb entry and never will, or it simply hasn't been added to OMDb's catalogue yet and will show up in a few weeks. The response gives no way to tell these apart. Caching permanently is right for the first case and permanently wrong for the second. A finite TTL retries the "not yet added" case cheaply while still collapsing the "never will exist" case from "every project, every run" down to one request per TTL window for the whole fleet.

This is a deliberate change from the per-project caches this proxy replaces, several of which cached misses permanently. That was the correct trade when each project spent its own 1000/day — retrying a genuine miss was pure waste against a budget nobody else could touch. With one shared key and one shared cache, a periodic retry now costs a single request *across every consumer combined*, so the insurance against a late catalogue addition became nearly free. Tune it with `NOTFOUND_TTL` if a week feels wrong for your usage pattern.

Year parsing is best-effort: if the body is XML (`r=xml`) or the `Year` field can't be read as a four-digit year, the response falls back to the "unknown year" bucket (24h) rather than erroring.

## Traps this proxy handles so you don't have to

These are the parts of OMDb's actual behaviour that are easy to get wrong and expensive to get wrong:

1. **OMDb signals failure in the response body, and the paired HTTP status is inconsistent.** An ordinary miss is HTTP 200 with `{"Response":"False","Error":"Incorrect IMDb ID."}`. An exhausted quota is HTTP 401 with the *same shape*, `{"Response":"False","Error":"Request limit reached!"}`. The proxy always decodes the body before trusting the status code, and detects a quota error by matching `request limit` case-insensitively against the `Error` field rather than pinning OMDb's exact wording.
2. **A quota-exceeded response is never cached.** It's a fact about the proxy's key on that particular day, not a fact about the movie. Caching it would poison that cache entry permanently.
3. **A decoded quota error trips a local circuit breaker for the rest of the UTC day.** The daily budget counter is only ever a prediction of OMDb's real counter, and the two can disagree — the cache DB gets recreated, the budget gets raised, or something else spends requests against the same key. A quota error from upstream is ground truth that the prediction was wrong, so the proxy immediately marks the day's budget as fully spent rather than waiting for its own counter to catch up. Without this, every later cache miss that day would make its own doomed upstream call to rediscover the same exhausted key — and if a stale entry happens to exist for one of those misses, the consumer gets a perfectly good `STALE` response and never even sees the quota error to react to, so nothing else would stop the loop.
4. **Not-found misses are cached** (with the finite TTL above), so a typo'd id or a genuinely absent title isn't retried on every single pass.
5. **Upstream failures serve stale data instead of failing.** If the upstream request errors, times out, or the daily budget is already spent, and an expired cache entry exists for that query, the proxy returns the stale body with `X-Cache: STALE` rather than an error. Consumers never see a hard failure just because the proxy is temporarily out of quota.
6. **A miss with no stale fallback and an exhausted budget returns OMDb's own quota body verbatim** — `{"Response":"False","Error":"Request limit reached!"}`, HTTP 401 — without making an upstream call to get it. Existing OMDb clients already know how to recognise this shape and abort their enrichment pass cleanly; that's exactly the behaviour this proxy wants to preserve.
7. **Concurrent identical requests collapse into one upstream call**, via `golang.org/x/sync/singleflight`. If three projects all start a sync at the same moment and all immediately ask about the same movie, only one of them actually reaches OMDb.

## Cache key

The cache key is the SHA-256 of a canonicalised query string: `apikey` is dropped entirely, parameter names are lowercased, `i` and `type` values are lowercased (IMDb ids and the type filter are case-insensitive upstream), `t` (title) is lowercased with internal whitespace collapsed (OMDb title matching is case-insensitive and forgiving of stray spacing), every value is trimmed, and the parameters are sorted before re-encoding. `?i=TT0137523&apikey=a` and `?apikey=b&i=tt0137523` land on the same row. The canonical string itself is stored alongside the row for debugging.

## Storage

SQLite via `modernc.org/sqlite` — pure Go, no cgo, so the Docker image builds with `CGO_ENABLED=0` and needs no C toolchain. Schema:

```sql
CREATE TABLE IF NOT EXISTS responses (
    cache_key   TEXT PRIMARY KEY,   -- sha256 of the canonical query
    query       TEXT NOT NULL,      -- canonical query, for inspection
    body        BLOB NOT NULL,      -- raw upstream bytes, verbatim
    content_type TEXT NOT NULL,
    status      INTEGER NOT NULL,   -- upstream HTTP status
    found       INTEGER NOT NULL,   -- 1 = Response:"True", 0 = miss
    fetched_at  TEXT NOT NULL,      -- RFC3339 UTC
    expires_at  TEXT                -- RFC3339 UTC; NULL = permanent
);

CREATE TABLE IF NOT EXISTS quota (
    day  TEXT PRIMARY KEY,          -- YYYY-MM-DD in UTC
    used INTEGER NOT NULL
);
```

WAL mode and a busy timeout are enabled at startup. The quota counter increments once per request actually sent upstream, keyed by UTC day, because OMDb's own limit resets at UTC midnight.

## Configuration

All configuration is environment variables; there is no config file.

| Variable | Default | Notes |
|---|---|---|
| `OMDB_API_KEY` | *(required)* | The proxy's own upstream key. The process refuses to start without it. Never logged, never echoed in a response. |
| `ADDR` | `:8090` | Listen address. |
| `DB_PATH` | `/data/cache.db` | SQLite database path. |
| `DAILY_BUDGET` | `900` | Upstream requests allowed per UTC day. Deliberately below OMDb's real 1000, to leave headroom for a second run the same day. |
| `UPSTREAM_URL` | `https://www.omdbapi.com` | Overridable so tests and staging can point elsewhere. |
| `PROXY_TOKEN` | *(unset)* | If set, clients must present it as `apikey` or `Authorization: Bearer <token>`. **Left unset, this is an open proxy** — fine on a trusted LAN, not something to expose to the internet. |
| `NOTFOUND_TTL` | `168h` | Go duration string. TTL for `Response:"False"` misses. |

## Admin endpoints

OMDb only ever uses `/`, so these don't collide with anything a client might send:

- `GET /healthz` — plain `ok`. Doesn't touch the database, so a slow or momentarily locked DB doesn't make the container look unhealthy.
- `GET /stats` — JSON: quota used today, remaining budget, total cached rows, permanent vs. expiring row counts, and cache hit/miss/stale counters accumulated since the process started.

## Development

```bash
go build ./...
go vet ./...
go test ./...
```

Tests are hermetic — nothing talks to the real `omdbapi.com`. Fake upstreams are `httptest.Server` instances, and the cache uses temp-file SQLite databases created per test.
