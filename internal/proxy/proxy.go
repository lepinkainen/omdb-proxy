// Package proxy implements the OMDb-compatible HTTP handler: cache
// lookup, upstream forwarding with the proxy's own key, single-flighted
// fetches, and the expiry/quota policy that makes the cache aggressive
// without ever poisoning it with a quota error.
package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"golang.org/x/sync/singleflight"

	"github.com/lepinkainen/omdb-proxy/internal/cache"
)

// quotaBody is OMDb's own response shape for an exhausted key, byte for
// byte. Trap #5: when our budget is spent and there's no stale entry to
// fall back to, we hand this back verbatim (not a proxy-specific error),
// because every consumer of this proxy already knows how to recognise
// and abort cleanly on it.
const quotaBody = `{"Response":"False","Error":"Request limit reached!"}`

const quotaContentType = "application/json"

// invalidKeyBody mirrors OMDb's shape for a rejected key, used only for
// the optional PROXY_TOKEN gate — never sent upstream, never confused
// with a real quota response.
const invalidKeyBody = `{"Response":"False","Error":"Invalid API key!"}`

// Config configures a Handler. See the field comments for the
// environment variables each one maps to in cmd/omdb-proxy.
type Config struct {
	// UpstreamURL is the OMDb-compatible base URL to forward misses to.
	UpstreamURL string
	// APIKey is the proxy's own OMDb key, substituted for whatever
	// apikey (if any) the client sent.
	APIKey string
	// DailyBudget caps upstream requests per UTC day.
	DailyBudget int
	// ProxyToken, if non-empty, must be presented by clients as either
	// the apikey query parameter or an Authorization: Bearer header.
	// Left empty, the proxy accepts any (or no) client key.
	ProxyToken string
	// HTTPClient is used for upstream requests. Defaults to a client
	// with a 10s timeout when nil.
	HTTPClient *http.Client
	// Now returns the current time. Defaults to time.Now; overridden in
	// tests so expiry decisions can be pinned to a fixed clock.
	Now func() time.Time
	// NotFoundTTL is the expiry applied to Response:"False" misses.
	// Defaults to DefaultNotFoundTTL (see its doc comment for why this
	// is finite rather than permanent) when zero.
	NotFoundTTL time.Duration
}

// Stats is the in-memory, process-lifetime counters reported by
// /stats. They deliberately live in memory rather than the database:
// they exist purely for operator debugging, and resetting them on
// restart is the intuitive behaviour ("since process start" is what a
// human wants when nothing else has changed).
type Stats struct {
	Hits   int64
	Misses int64
	Stales int64
}

// Handler serves the OMDb-compatible endpoint plus the admin endpoints.
type Handler struct {
	store       *cache.Store
	upstreamURL string
	apiKey      string
	budget      int
	proxyToken  string
	httpClient  *http.Client
	now         func() time.Time
	notFoundTTL time.Duration

	group singleflight.Group

	hits, misses, stales atomic.Int64
}

// New builds a Handler backed by store, per cfg.
func New(store *cache.Store, cfg Config) (*Handler, error) {
	if cfg.APIKey == "" {
		return nil, errors.New("proxy: APIKey must not be empty")
	}
	if cfg.UpstreamURL == "" {
		return nil, errors.New("proxy: UpstreamURL must not be empty")
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	notFoundTTL := cfg.NotFoundTTL
	if notFoundTTL == 0 {
		notFoundTTL = DefaultNotFoundTTL
	}

	return &Handler{
		store:       store,
		upstreamURL: strings.TrimRight(cfg.UpstreamURL, "/"),
		apiKey:      cfg.APIKey,
		budget:      cfg.DailyBudget,
		proxyToken:  cfg.ProxyToken,
		httpClient:  httpClient,
		now:         now,
		notFoundTTL: notFoundTTL,
	}, nil
}

// Stats returns a snapshot of the process-lifetime hit/miss/stale
// counters.
func (h *Handler) Stats() Stats {
	return Stats{
		Hits:   h.hits.Load(),
		Misses: h.misses.Load(),
		Stales: h.stales.Load(),
	}
}

// ServeHTTP implements the OMDb-compatible endpoint at "/".
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !h.authorised(r) {
		writeBody(w, http.StatusUnauthorized, quotaContentType, []byte(invalidKeyBody), "")
		return
	}

	params := r.URL.Query()
	canonical := canonicalQuery(params)
	cacheKey := cacheKeyFor(canonical)
	isSearch := params.Get("s") != ""

	ctx := r.Context()

	if entry, err := h.store.Get(ctx, cacheKey); err == nil && entry != nil && !entry.Expired(h.now()) {
		h.hits.Add(1)
		writeEntry(w, entry, "HIT")
		return
	}
	// A store error on the fast-path lookup is not fatal: fall through
	// to the single-flighted path below, which re-reads the cache
	// anyway and will surface the same error there if it persists.

	result, err, _ := h.group.Do(cacheKey, func() (any, error) {
		return h.resolve(ctx, cacheKey, canonical, isSearch)
	})
	if err != nil {
		http.Error(w, "omdb-proxy: internal error", http.StatusInternalServerError)
		return
	}

	res := result.(resolution)
	switch res.cacheStatus {
	case "HIT":
		h.hits.Add(1)
	case "STALE":
		h.stales.Add(1)
	default:
		h.misses.Add(1)
	}
	writeBody(w, res.status, res.contentType, res.body, res.cacheStatus)
}

// resolution is what a single (possibly single-flighted) cache
// resolution produces for one cache key.
type resolution struct {
	body        []byte
	contentType string
	status      int
	cacheStatus string // HIT, MISS, or STALE
}

// resolve performs the "no fresh cache entry" path: re-check the cache
// (another goroutine may have just filled it), then either fetch
// upstream or fall back to a stale entry / the quota body. It is always
// called through singleflight so that concurrent requests for the same
// movie collapse into a single upstream call.
func (h *Handler) resolve(ctx context.Context, cacheKey, canonical string, isSearch bool) (resolution, error) {
	entry, err := h.store.Get(ctx, cacheKey)
	if err != nil {
		return resolution{}, errors.Wrap(err, "read cache entry")
	}

	now := h.now()
	if entry != nil && !entry.Expired(now) {
		// Filled by a previous singleflight call (or a racing request)
		// while we were waiting for the group lock.
		return resolution{body: entry.Body, contentType: entry.ContentType, status: entry.Status, cacheStatus: "HIT"}, nil
	}

	day := now.UTC().Format("2006-01-02")
	reserved, _, err := h.store.TryReserveQuota(ctx, day, h.budget)
	if err != nil {
		return resolution{}, errors.Wrap(err, "reserve quota")
	}
	if !reserved {
		return h.budgetExhausted(entry)
	}

	body, contentType, status, upstreamErr := h.fetchUpstream(ctx, canonical)
	if upstreamErr != nil {
		// Upstream is down or timed out. Trap #4: consumers must never
		// see this as a hard failure while we still have something to
		// serve.
		return h.staleOrError(entry, upstreamErr)
	}

	found, quotaError, year, yearOK := parseEnvelope(body, contentType)
	if quotaError {
		// Trap #2: this is a property of our key's daily allowance,
		// not of the movie. Never let it land in the cache table,
		// where it would look like a permanent fact about this query.

		// Trip the local circuit breaker before anything else. Our own
		// budget counter is a prediction of OMDb's counter; a decoded
		// quota error is ground truth that the prediction was wrong,
		// and treating it as ground truth *now* is what stops every
		// remaining cache miss today from spending its own doomed
		// upstream call to rediscover the same fact. This is most
		// important exactly here, in combination with staleOrQuota
		// below: when a stale entry exists, the caller is about to get
		// a perfectly good STALE response and will never see this
		// quota error itself, so this is the only place left that can
		// record it. A failure to persist the marker doesn't change
		// the response we're about to serve, and this handler has no
		// logger to report it to, so we deliberately swallow it —
		// worst case, some other request records the exhaustion for
		// us before the day is out.
		_ = h.store.MarkExhausted(ctx, day, h.budget)

		return h.staleOrQuota(entry, body, contentType, status)
	}

	expiresAt := expiryFor(now, isSearch, found, year, yearOK, h.notFoundTTL)
	putErr := h.store.Put(ctx, cache.Entry{
		CacheKey:    cacheKey,
		Query:       canonical,
		Body:        body,
		ContentType: contentType,
		Status:      status,
		Found:       found,
		FetchedAt:   now,
		ExpiresAt:   expiresAt,
	})
	if putErr != nil {
		// We already have a good response to hand back; a failure to
		// persist it just means we'll pay for it again next time.
		return resolution{body: body, contentType: contentType, status: status, cacheStatus: "MISS"}, nil
	}

	return resolution{body: body, contentType: contentType, status: status, cacheStatus: "MISS"}, nil
}

// budgetExhausted handles the case where our own daily budget check
// failed before we even tried upstream: serve the stale entry if one
// exists (trap #4), otherwise hand back OMDb's own quota body verbatim
// (trap #5) without spending an upstream call to get it.
func (h *Handler) budgetExhausted(entry *cache.Entry) (resolution, error) {
	if entry != nil {
		return resolution{body: entry.Body, contentType: entry.ContentType, status: entry.Status, cacheStatus: "STALE"}, nil
	}
	return resolution{body: []byte(quotaBody), contentType: quotaContentType, status: http.StatusUnauthorized, cacheStatus: "MISS"}, nil
}

// staleOrError handles an upstream transport failure (network error,
// timeout, non-2xx that we can't even read a body from).
func (h *Handler) staleOrError(entry *cache.Entry, upstreamErr error) (resolution, error) {
	if entry != nil {
		return resolution{body: entry.Body, contentType: entry.ContentType, status: entry.Status, cacheStatus: "STALE"}, nil
	}
	return resolution{}, upstreamErr
}

// staleOrQuota handles an upstream response that turned out to be a
// quota error once we decoded the body. If we have something stale to
// serve, do that; otherwise hand back exactly what upstream told us
// (still never cached).
func (h *Handler) staleOrQuota(entry *cache.Entry, body []byte, contentType string, status int) (resolution, error) {
	if entry != nil {
		return resolution{body: entry.Body, contentType: entry.ContentType, status: entry.Status, cacheStatus: "STALE"}, nil
	}
	return resolution{body: body, contentType: contentType, status: status, cacheStatus: "MISS"}, nil
}

// fetchUpstream forwards the canonical query to the real OMDb (or a
// test double), substituting the proxy's own apikey for whatever the
// client sent — the client's key, if any, was already stripped out of
// the canonical query and never leaves this process.
func (h *Handler) fetchUpstream(ctx context.Context, canonical string) (body []byte, contentType string, status int, err error) {
	values, parseErr := url.ParseQuery(canonical)
	if parseErr != nil {
		return nil, "", 0, errors.Wrap(parseErr, "parse canonical query")
	}
	values.Set("apikey", h.apiKey)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.upstreamURL+"/?"+values.Encode(), nil)
	if err != nil {
		return nil, "", 0, errors.Wrap(err, "build upstream request")
	}

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, "", 0, errors.Wrap(err, "upstream request failed")
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", 0, errors.Wrap(err, "read upstream body")
	}

	return data, resp.Header.Get("Content-Type"), resp.StatusCode, nil
}

// authorised checks the optional PROXY_TOKEN gate. With no token
// configured, every request is accepted — see the README for why that
// default is deliberately an open proxy meant for a trusted LAN.
func (h *Handler) authorised(r *http.Request) bool {
	if h.proxyToken == "" {
		return true
	}

	if key := r.URL.Query().Get("apikey"); key == h.proxyToken {
		return true
	}

	if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
		return strings.TrimPrefix(auth, "Bearer ") == h.proxyToken
	}

	return false
}

// writeEntry writes a cache.Entry to the client as-is, tagging the
// response with the given X-Cache status.
func writeEntry(w http.ResponseWriter, e *cache.Entry, cacheStatus string) {
	writeBody(w, e.Status, e.ContentType, e.Body, cacheStatus)
}

// writeBody writes a raw response body to the client unmodified. This
// is the only place that produces bytes on the wire, which is what
// guarantees byte-for-byte fidelity with whatever OMDb (or the cache)
// actually returned.
func writeBody(w http.ResponseWriter, status int, contentType string, body []byte, cacheStatus string) {
	if contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	if cacheStatus != "" {
		w.Header().Set("X-Cache", cacheStatus)
	}
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// Healthz answers GET /healthz with a plain "ok". It intentionally
// doesn't touch the database — a slow or locked DB shouldn't make the
// container look unhealthy and get killed mid-request.
func (h *Handler) Healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// statsResponse is the JSON shape returned by GET /stats.
type statsResponse struct {
	QuotaUsedToday   int   `json:"quota_used_today"`
	QuotaBudget      int   `json:"quota_budget"`
	QuotaRemaining   int   `json:"quota_remaining"`
	TotalCachedRows  int   `json:"total_cached_rows"`
	PermanentRows    int   `json:"permanent_rows"`
	ExpiringRows     int   `json:"expiring_rows"`
	CacheHits        int64 `json:"cache_hits"`
	CacheMisses      int64 `json:"cache_misses"`
	CacheStaleServes int64 `json:"cache_stale_serves"`
}

// StatsHandler answers GET /stats with a snapshot of quota, cache
// contents, and hit/miss/stale counters.
func (h *Handler) StatsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	day := h.now().UTC().Format("2006-01-02")

	used, err := h.store.QuotaUsed(ctx, day)
	if err != nil {
		http.Error(w, "omdb-proxy: failed to read quota", http.StatusInternalServerError)
		return
	}

	cacheStats, err := h.store.Stats(ctx)
	if err != nil {
		http.Error(w, "omdb-proxy: failed to read cache stats", http.StatusInternalServerError)
		return
	}

	stats := h.Stats()
	remaining := h.budget - used
	if remaining < 0 {
		remaining = 0
	}

	resp := statsResponse{
		QuotaUsedToday:   used,
		QuotaBudget:      h.budget,
		QuotaRemaining:   remaining,
		TotalCachedRows:  cacheStats.TotalRows,
		PermanentRows:    cacheStats.PermanentRows,
		ExpiringRows:     cacheStats.ExpiringRows,
		CacheHits:        stats.Hits,
		CacheMisses:      stats.Misses,
		CacheStaleServes: stats.Stales,
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(resp); err != nil {
		http.Error(w, "omdb-proxy: failed to encode stats", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())
}
