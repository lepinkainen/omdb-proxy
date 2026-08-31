# Caching, wire compatibility, and storage

Background for how the proxy stays wire-compatible with OMDb, how it decides
what to cache and for how long, and how that cache is stored. The README
stays short; the reasoning lives here.

## Wire compatibility

Requests arrive at `GET /` with OMDb's own query parameters — `i`, `t`, `s`, `y`, `type`, `plot`, `page`, `r`, `v`, `apikey` — and get OMDb's own response shape back, byte-for-byte. The proxy never parses a response into a struct and re-serialises it: different consumer projects want different fields (`Plot`, `Poster`, `Genre`, `Runtime`, ...), so the full body is cached and returned verbatim. The upstream `Content-Type` is preserved too.

One addition: every response carries an `X-Cache: HIT | MISS | STALE` header, purely for debugging. Extra headers don't break OMDb clients.

## Caching policy

Expiry is derived from the release year in the response body wherever there is one, and is deliberately skewed towards "cache forever" — the whole point of this proxy is to stop re-fetching movies whose metadata essentially never changes.

| Response | Expiry |
|---|---|
| Successful lookup, released more than a year ago | Permanent (`expires_at` is `NULL`) |
| Successful lookup, released within the last year | 7 days |
| Successful lookup, year unknown or in the future | 24 hours |
| Not-found miss (`Response:"False"`, ordinary error) | `NOTFOUND_TTL`, default 7 days — see below |
| Search query (`s=...`) | 30 days, regardless of whether it found anything |
| Quota-exceeded response | **Never cached** — see [Traps](#traps) |

**Why not-found misses get a finite TTL, not a permanent one:** a `Response:"False"` miss has two indistinguishable causes. Either the title genuinely has no OMDb entry and never will, or it simply hasn't been added to OMDb's catalogue yet and will show up in a few weeks. The response gives no way to tell these apart. Caching permanently is right for the first case and permanently wrong for the second. A finite TTL retries the "not yet added" case cheaply while still collapsing the "never will exist" case from "every project, every run" down to one request per TTL window for the whole fleet.

This is a deliberate change from the per-project caches this proxy replaces, several of which cached misses permanently. That was the correct trade when each project spent its own 1000/day — retrying a genuine miss was pure waste against a budget nobody else could touch. With one shared key and one shared cache, a periodic retry now costs a single request *across every consumer combined*, so the insurance against a late catalogue addition became nearly free. Tune it with `NOTFOUND_TTL` if a week feels wrong for your usage pattern.

Year parsing is best-effort: if the body is XML (`r=xml`) or the `Year` field can't be read as a four-digit year, the response falls back to the "unknown year" bucket (24h) rather than erroring.

## Traps

These are the parts of OMDb's actual behaviour that are easy to get wrong and expensive to get wrong:

1. **OMDb signals failure in the response body, and the paired HTTP status is inconsistent.** An ordinary miss is HTTP 200 with `{"Response":"False","Error":"Incorrect IMDb ID."}`. An exhausted quota is HTTP 401 with the *same shape*, `{"Response":"False","Error":"Request limit reached!"}`. The proxy always decodes the body before trusting the status code, and detects a quota error by matching `request limit` case-insensitively against the `Error` field rather than pinning OMDb's exact wording.
2. **A quota-exceeded response is never cached.** It's a fact about the proxy's key on that particular day, not a fact about the movie. Caching it would poison that cache entry permanently.
3. **A decoded quota error arms a circuit breaker that probes back.** The daily budget counter is only ever a prediction of OMDb's real counter, and the two can disagree — the cache DB gets recreated, the budget gets raised, or something else spends requests against the same key. A quota error from upstream is ground truth that the prediction was wrong, so the proxy stops calling upstream immediately rather than waiting for its own counter to catch up. Without this, every later cache miss that day would make its own doomed upstream call to rediscover the same exhausted key — and if a stale entry happens to exist for one of those misses, the consumer gets a perfectly good `STALE` response and never even sees the quota error to react to, so nothing else would stop the loop. The breaker is not latched to the calendar: after `QUOTA_PROBE_INTERVAL` (default 15m) one request is let through as a probe, and a probe that comes back normal clears the breaker *and* resets the day's counter to zero. OMDb neither exposes remaining quota nor documents when its day rolls over, and its boundary is not UTC midnight — so a key that starts answering again is the only signal available that a fresh upstream day has begun. An earlier design that latched until the next UTC midnight cost a live deployment a full day: a miss just after UTC midnight hit a key OMDb still considered spent, and the proxy sat idle for 24 hours holding a key that recovered within a few. The probe is the difference between recovering in fifteen minutes and forfeiting a day.
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
    day          TEXT PRIMARY KEY,  -- YYYY-MM-DD in UTC
    used         INTEGER NOT NULL,  -- requests actually spent upstream
    exhausted_at TEXT               -- RFC3339 UTC; NULL = breaker closed
);
```

WAL mode and a busy timeout are enabled at startup. `used` increments once per request actually sent upstream and is a real measurement — nothing ever forges it to trip the breaker, because that would make a forfeited day indistinguishable from a spent one. `exhausted_at` is when upstream last refused the key, or last handed out a probe; it is the breaker.

The day key is UTC, but the accounting is not really the calendar's: `MarkRecovered` zeroes `used` whenever a probe finds the key working again, so a day effectively begins whenever OMDb's own quota day does. That indirection exists because OMDb documents no reset time and its boundary is not UTC midnight. `Open` adds `exhausted_at` to a pre-existing two-column table on startup — `CREATE TABLE IF NOT EXISTS` would otherwise leave a deployed database on the old shape and break every quota read.

## Logs

One `slog` line per request on stderr, plus startup and shutdown:

```
level=INFO msg=request query="i=tt0137523" cache=MISS status=200 duration_ms=214
level=INFO msg=request query="i=tt0137523" cache=HIT status=200 duration_ms=0
```

`cache` mirrors the `X-Cache` header, so `cache=MISS` marks the requests that
actually spent upstream quota. A bare `GET /` — the HTML dashboard rather than
a proxy request — logs `cache=INDEX` with an empty query. The query is
otherwise the canonical one, with `apikey` stripped — no client key or proxy
token ever reaches the log. Rejected tokens
log at WARN, and cache-write or quota-marker failures at ERROR.

`docker compose -f compose.prod.yaml logs -f` on the VPS.
