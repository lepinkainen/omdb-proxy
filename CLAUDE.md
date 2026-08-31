# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A self-hosted caching proxy in front of `omdbapi.com`, wire-compatible with it. Several personal projects each used to call OMDb with their own key; OMDb's free tier allows 1000 requests/day, so the same films were refetched by every project and a full refresh took days. This proxy holds **one** upstream key and **one** shared cache, so N consumer projects cost the quota of 1.

Two facts shape nearly every decision here:

- **The proxy is transparent.** A consumer switches to it by changing only its base URL. Anything that makes a response differ from what OMDb itself would have sent — a custom error shape, a rewritten body, a locally-invented rejection — breaks that contract and is almost always the wrong change. The one deliberate exception is the additive `X-Cache` header.
- **The cache is the product.** Aggressiveness is the point. A change that shortens a TTL, adds a refetch, or drops an entry needs to justify itself in requests-per-day, because the budget is the scarce resource.

## Commands

```bash
go build ./...
go vet ./...
go test ./...
go test ./... -race                                   # singleflight and quota tests need this
gofmt -l .                                            # must print nothing
go test ./internal/proxy/ -run TestQuotaResponseIsNotCached -v   # single test

OMDB_API_KEY=xxx DB_PATH=/tmp/scratch.db ADDR=:8090 go run ./cmd/omdb-proxy

docker compose up --build
curl 'localhost:8090/?i=tt0137523' -i                 # check the X-Cache header
curl localhost:8090/stats
```

`OMDB_API_KEY` is the only required variable; startup fails fast without it. Everything else defaults: `ADDR=:8090`, `DB_PATH=/data/cache.db`, `DAILY_BUDGET=900`, `UPSTREAM_URL=https://www.omdbapi.com`, `NOTFOUND_TTL=168h`, `QUOTA_PROBE_INTERVAL=15m`, `PROXY_TOKEN` unset.

There is no `.env` loader in this repo — the process reads the real environment, and `compose.yaml` feeds it via `env_file`.

## Architecture

```
cmd/omdb-proxy    config from environment, wiring, graceful shutdown
internal/cache    SQLite store: responses table, quota ledger, stats
internal/proxy    HTTP handler, canonicalisation, expiry policy, upstream client
```

**Request path.** `ServeHTTP` optionally checks `PROXY_TOKEN`, canonicalises the query, and tries a fast-path cache read. A fresh hit returns immediately. Otherwise the request enters `singleflight.Group.Do` keyed by the cache key, and `resolve` re-checks the cache (another goroutine may have just filled it), reserves quota, fetches upstream, decides an expiry, and stores the result.

A store error on the *fast path* is deliberately non-fatal — it falls through to `resolve`, which re-reads the cache and will surface a persistent error there.

**Bodies are stored and returned verbatim.** `Entry.Body` is the raw upstream bytes. Never parse a response into a struct and re-encode it: different consumers need different fields (`Plot`, `Poster`, `Genre`, `Runtime`), and only the full body serves all of them. `parseEnvelope` is a *side* read for cache policy only — it extracts `Response`, `Error`, and `Year` and nothing else.

**Single SQLite connection is deliberate.** `Open` sets `db.SetMaxOpenConns(1)`. See the `internal/cache` package doc: it makes PRAGMA setup a one-time affair and makes `TryReserveQuota` race-free without an application-level lock, because `database/sql` serialises callers on the one connection. Raising the connection count silently breaks quota atomicity.

**The index page (`internal/proxy/index.go`, `index.html`) is a third path out of `ServeHTTP`, gated on a completely empty `r.URL.RawQuery` at `/`.** It renders quota remaining, cache row counts, and the 25 most recent entries via `store.Stats` and `store.Recent`, using `html/template` so a hostile canonical query (already stripped of `apikey`) can't inject markup. No external assets — it has to render on a bare VPS with no internet.

## Invariants

These are the expensive ones. Each corresponds to a test; if a test here looks wrong, suspect the change before the test.

**Decode the body before trusting the status code.** OMDb signals failure in the body with an inconsistent status: an ordinary miss is HTTP 200 with `{"Response":"False","Error":"Incorrect IMDb ID."}`, while an exhausted quota is HTTP 401 with the same body shape. Reversing this ordering makes every quota response look like a permanent "this movie does not exist".

**Never cache a quota error.** It is a fact about the proxy's key today, not about the movie. Caching one poisons the entry — permanently, if the expiry policy happened to classify it as an old film.

**A quota error arms a circuit breaker that probes rather than latches.** `resolve` calls `store.MarkExhausted` before falling back, which records *when* upstream refused us. `TryReserveQuota` then refuses every caller until `QUOTA_PROBE_INTERVAL` (default 15m) has passed, at which point exactly one caller is granted a probe — the grant pushes `exhausted_at` forward to now, so a burst of misses cannot become a burst of doomed calls. A probe that comes back normal calls `MarkRecovered`; a probe that comes back exhausted re-arms.

**"Normal" for a probe means HTTP 200 *and* a recognisable OMDb envelope**, not merely the absence of a quota error. `recognisedEnvelope` exists for this and is deliberately stricter than `parseEnvelope`, which degrades to a substring scan so it never misses a quota error. Recovery hands the proxy a whole fresh budget, so the bar is proof OMDb served us: a 502 HTML page from a CDN and an `Invalid API key!` both lack a quota error while saying nothing about the quota day, and either would resume full-rate traffic into a key that is still refused. An ordinary `Movie not found!` does qualify — it is a served answer.

The local `DAILY_BUDGET` counter is only a *prediction* of OMDb's counter, and the two drift whenever the cache DB is recreated, the budget is raised, or the key is used elsewhere. An upstream quota error is ground truth that the prediction was wrong. This matters most alongside stale serving: when a stale entry exists the consumer receives `STALE` and carries on requesting, so it never sees the quota signal that would make it stop — without the breaker, the proxy would spend the whole run on doomed upstream calls.

**A recovered probe starts a new accounting epoch, because it is the only observable start of an upstream quota day.** OMDb publishes no way to read remaining quota and does not document when its day rolls over — [issue #335](https://github.com/omdbapi/OMDb-API/issues/335) asks and never got an answer — and its boundary is demonstrably *not* UTC midnight. So the ledger is a single `quota_epoch` row (`used`, `epoch_started_at`, `exhausted_at`), not a table keyed by UTC date. `MarkRecovered` anchors `epoch_started_at` to the moment a refused key answered again and restarts `used` at 1 (the probe itself was served out of the new upstream day, and over-counting is the safe direction against a limit we cannot read).

Two failures came from keying this to the calendar, and both are covered by tests. Latching the breaker until the next UTC midnight cost a live deployment a full day: the first miss after UTC midnight hit a key OMDb still considered spent, latched the breaker, and forfeited the following 24 hours while the key worked again within a few. Resetting a UTC-date-keyed counter at midnight was the mirror image: an epoch anchored to OMDb's 06:00 rollover would be handed a *second* full budget at our midnight, inside one upstream day, and spend it into a real refusal. `TestUTCMidnightDoesNotResetTheCounter` and `TestBudgetSurvivesUTCMidnightAfterRecovery` pin that.

Absent an observed rollover the epoch rolls itself one `QuotaEpochLength` (24h) on, in whole steps rather than jumping to now — the step preserves the hour, so once a probe pins the epoch to OMDb's actual rollover, every later epoch rolls at that hour without rediscovering it by hitting the wall. An epoch rollover deliberately leaves `exhausted_at` armed: the rollover is the proxy's own estimate, the breaker is something upstream actually said, and only a probe may overrule it.

This all assumes the proxy is the sole spender of its key; if something else shares it, the new epoch is optimistic and the next probe simply rediscovers exhaustion.

**`MarkExhausted` must never touch `used`.** Tripping the breaker by forging the counter (the old `used = MAX(used, excluded.used)` upsert) left a forfeited day byte-identical to a legitimately spent one, so neither `/stats` nor the dashboard could answer "did we spend it, or did OMDb cut us off?". `used` is now a real measurement and the breaker is a separate column; `exhausted_at` only ever moves forward (`MAX`), so a stale caller cannot wind it back and hand out an early probe.

**Zero remaining has two causes and the operator-facing surfaces must distinguish them.** `quota_exhausted_upstream` in `/stats` and the dashboard's two different notes exist because "we spent our own 900" and "OMDb refused the key" look identical from outside and want opposite reactions. The `Warn` log line in `resolve` is the only other place an armed breaker is observable.

**Quota exhaustion is signalled with OMDb's own body.** On a miss with no stale entry and no budget, the proxy returns `{"Response":"False","Error":"Request limit reached!"}` with HTTP 401, byte-identical to upstream. Consumers already recognise this and abort their enrichment pass cleanly. Do not replace it with a proxy-specific error.

**Serve stale rather than fail.** An expired entry plus a failing upstream, an exhausted budget, or an upstream quota error all return the stale body with `X-Cache: STALE`. A consumer should never break because the proxy ran out of quota. A true miss with nothing stale and a dead upstream is the only case that errors.

**Quota detection matches `request limit` case-insensitively on a substring.** OMDb owns the exact wording. Pinning the literal string would silently stop detecting exhaustion and start caching it as a real miss.

**A bare `GET /` (empty `RawQuery`) is the index page, never a proxy request — and that check has to run before the `PROXY_TOKEN` gate.** OMDb itself only ever answers a query-less `/` with an error, so no real consumer can depend on that exact request, which is what makes it a safe discriminator; any query carrying an actual parameter still goes through the normal proxy path untouched. (`/?` has an empty `RawQuery` too and also renders the index — the same parameter-less request by another spelling.) Putting the check after `authorised(r)` looks like the "more secure" order but is a trap: a client presents `PROXY_TOKEN` as `?apikey=...`, which makes the query non-empty, so a gated index would never be reachable by a browser in the first place — it's ungated on purpose, like `/healthz` and `/stats`.

## Cache key

`canonicalQuery` drops `apikey` entirely (two projects with different keys must share a row), lowercases parameter names, lowercases `i` and `type`, lowercases and whitespace-collapses `t`, trims all values, drops empties, and relies on `url.Values.Encode` sorting by key. `cacheKeyFor` is the SHA-256 of that string. The canonical string itself is stored in `responses.query` for inspection.

The client's key never leaves the process: `fetchUpstream` reparses the canonical query and sets the proxy's own `apikey` on it.

## Expiry policy

Decided by `expiryFor` (`internal/proxy/expiry.go`), whose doc comments carry the reasoning. Summary:

| Case | Expiry |
| --- | --- |
| Search (`s=`), any outcome | 30 days |
| Miss (`Response:"False"`) | `NOTFOUND_TTL`, default 7 days |
| Hit, release year earlier than the current year | permanent (`expires_at` NULL) |
| Hit, release year equals the current year | 7 days |
| Hit, year unparseable or in the future | 24 hours |

Two things a future reader is likely to "simplify" into a bug:

- **The not-found TTL is finite on purpose.** A miss has two indistinguishable causes: the title has no OMDb entry and never will, or it simply has not been added yet. Permanent caching is right for the first and permanently wrong for the second. This inverts the rule used by the per-project caches this proxy replaces — correct when each project paid its own 1000/day, wrong now that a weekly retry costs one request for the entire fleet.
- **The year split is calendar-year granularity**, because OMDb only ever reports a year. It slightly under-caches titles released late last year and over-caches ones released early this year, and is deliberately biased towards permanent once a title is unambiguously old.

## Testing

Tests must be hermetic — nothing may contact `omdbapi.com`. Use `httptest` servers as the fake upstream, temp-file SQLite databases, and `Config.Now` to pin the clock rather than racing the calendar (expiry assertions depend on the current year).

Quota, breaker, and singleflight tests assert exact upstream call counts via an `atomic.Int32` in the fake handler. When a change alters how many upstream calls a scenario makes, that count is the assertion that will catch it.

## Background documents

`ai-docs/` holds LLM-facing background that would bloat the files it describes. Keep it that way: config files stay terse and point here, rather than carrying paragraphs of rationale in comments.

- `ai-docs/deployment.md` — image build, GHCR workflows, compose files, volume and user choices.
- `ai-docs/caching.md` — wire compatibility, caching policy and its traps, the cache key, and the SQLite schema.

## Conventions

- Errors wrap with `github.com/cockroachdb/errors`; sentinels are checked with `errors.Is`.
- The API key is never logged, never included in an error message, never echoed in a response. This is why the per-request log line in `ServeHTTP` logs the *canonical* query and never `r.URL.RawQuery`: the raw query holds the client's own key, and the proxy token itself when `PROXY_TOKEN` is set. `TestRequestLoggingNeverLeaksKeys` guards it.
- Doc comments on exported symbols, and comments explain *why*. The invariants above are only defensible with their reasoning attached — several are one plausible-looking simplification away from costing 900 requests a day.
- `/stats` counters (hit/miss/stale) live in memory and reset on restart, which is the intuitive reading of "since process start". Quota and cache contents live in SQLite and do not.

## Docker

Multi-stage, `CGO_ENABLED=0` (the driver is pure-Go `modernc.org/sqlite`), final stage `gcr.io/distroless/static-debian12`. A separate `alpine` stage exists solely to supply `ca-certificates.crt`: `golang:alpine` does not install it and `distroless/static` ships none, so without that copy every upstream fetch dies at the TLS handshake with `x509: certificate signed by unknown authority`. The cache lives on the `/data` volume; losing it means refilling at `DAILY_BUDGET` per day.

CI publishes multi-arch images to `ghcr.io/lepinkainen/omdb-proxy`; `compose.prod.yaml` runs one on the VPS, `compose.yaml` builds locally. `ai-docs/deployment.md` carries the reasoning behind the cross-compile pin, the bind mount, and the non-root user — read it before changing any of those.

## Consumers

Any project that already builds its OMDb URL from a configurable base can point at this proxy unchanged. For the `moviepicker` repo specifically, `internal/omdb` already has a `WithBaseURL` option — it is currently test-only, so wiring it to an environment variable is the whole integration.

Consumers keep their own local caches. That is not redundant: the proxy saves quota across projects, while a local cache saves a network round trip and lets a project run when the proxy is down.
