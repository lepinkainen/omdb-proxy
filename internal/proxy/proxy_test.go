package proxy_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lepinkainen/omdb-proxy/internal/cache"
	"github.com/lepinkainen/omdb-proxy/internal/proxy"
)

const proxyAPIKey = "proxy-owns-this-key"

// newTestHandler wires a Handler to a temp-file SQLite cache and a fake
// upstream, with a fixed clock so expiry assertions don't race the
// calendar. Nothing here ever talks to the real omdbapi.com.
func newTestHandler(t *testing.T, upstreamURL string, budget int) (*proxy.Handler, *cache.Store) {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "cache.db")
	store, err := cache.Open(dbPath)
	if err != nil {
		t.Fatalf("cache.Open: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	fixedNow := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)

	h, err := proxy.New(store, proxy.Config{
		UpstreamURL: upstreamURL,
		APIKey:      proxyAPIKey,
		DailyBudget: budget,
		HTTPClient:  &http.Client{Timeout: 2 * time.Second},
		Now:         func() time.Time { return fixedNow },
	})
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}
	return h, store
}

func doRequest(t *testing.T, h *proxy.Handler, rawQuery string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/?"+rawQuery, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// foundMovieJSON, releaseYear is baked in so tests can control which
// expiry bucket a response falls into.
func foundMovieJSON(year string) string {
	return fmt.Sprintf(`{"Title":"The Matrix","Year":"%s","imdbID":"tt0137523","Response":"True"}`, year)
}

const notFoundJSON = `{"Response":"False","Error":"Movie not found!"}`
const quotaJSON = `{"Response":"False","Error":"Request limit reached!"}`

func TestCacheMissThenHit(t *testing.T) {
	var calls int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, foundMovieJSON("1999"))
	}))
	defer upstream.Close()

	h, _ := newTestHandler(t, upstream.URL, 900)

	first := doRequest(t, h, "i=tt0137523&apikey=client-key")
	if first.Code != http.StatusOK {
		t.Fatalf("first request status = %d, want 200", first.Code)
	}
	if got := first.Header().Get("X-Cache"); got != "MISS" {
		t.Errorf("first request X-Cache = %q, want MISS", got)
	}

	second := doRequest(t, h, "i=tt0137523&apikey=client-key")
	if got := second.Header().Get("X-Cache"); got != "HIT" {
		t.Errorf("second request X-Cache = %q, want HIT", got)
	}
	if second.Body.String() != first.Body.String() {
		t.Errorf("second body = %q, want identical to first %q", second.Body.String(), first.Body.String())
	}

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("upstream calls = %d, want 1 (second request should be served from cache)", got)
	}
}

func TestCanonicalizationSharesCacheEntry(t *testing.T) {
	var calls int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, foundMovieJSON("1999"))
	}))
	defer upstream.Close()

	h, _ := newTestHandler(t, upstream.URL, 900)

	doRequest(t, h, "i=TT0137523&apikey=a")
	doRequest(t, h, "apikey=b&i=tt0137523")

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("upstream calls = %d, want 1 (both queries should canonicalise to the same cache entry)", got)
	}
}

func TestClientAPIKeyNeverForwardedUpstream(t *testing.T) {
	var receivedKey string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedKey = r.URL.Query().Get("apikey")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, foundMovieJSON("1999"))
	}))
	defer upstream.Close()

	h, _ := newTestHandler(t, upstream.URL, 900)
	doRequest(t, h, "i=tt0137523&apikey=some-clients-own-key")

	if receivedKey != proxyAPIKey {
		t.Errorf("upstream received apikey = %q, want the proxy's own key %q", receivedKey, proxyAPIKey)
	}
}

func TestQuotaResponseIsNotCached(t *testing.T) {
	var calls int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		w.Header().Set("Content-Type", "application/json")
		if n == 1 {
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, quotaJSON)
			return
		}
		fmt.Fprint(w, foundMovieJSON("1999"))
	}))
	defer upstream.Close()

	h, store := newTestHandler(t, upstream.URL, 900)

	first := doRequest(t, h, "i=tt0137523&apikey=client-key")
	if first.Code != http.StatusUnauthorized {
		t.Errorf("first request status = %d, want 401", first.Code)
	}
	if first.Body.String() != quotaJSON {
		t.Errorf("first request body = %q, want upstream's quota body verbatim", first.Body.String())
	}

	canonical := "i=tt0137523"
	key := sha256Hex(canonical)
	entry, err := store.Get(t.Context(), key)
	if err != nil {
		t.Fatalf("store.Get: %v", err)
	}
	if entry != nil {
		t.Fatalf("a quota response must never be cached, but found an entry: %+v", entry)
	}

	// A second request for the same query must not retry upstream: the
	// first quota error already tripped the local circuit breaker
	// (MarkExhausted) for the rest of the day, so this is served from
	// the budget-exhausted path instead of spending another doomed
	// upstream call. This replaces the old expectation that a retry
	// would reach upstream again — that unconditional-retry behaviour
	// is exactly the bug the circuit breaker exists to close; see
	// TestQuotaErrorTripsBreakerForOtherQueries and
	// TestQuotaErrorTripsBreakerServesStaleForOtherQuery below for the
	// breaker's cross-query behaviour.
	second := doRequest(t, h, "i=tt0137523&apikey=client-key")
	if second.Code != http.StatusUnauthorized {
		t.Errorf("second request status = %d, want 401 (breaker tripped, no retry)", second.Code)
	}
	if second.Body.String() != quotaJSON {
		t.Errorf("second request body = %q, want the quota body", second.Body.String())
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("upstream calls = %d, want 1 (the circuit breaker must block the retry)", got)
	}
}

// TestQuotaErrorTripsBreakerForOtherQueries is the core regression test for
// the defect this proxy used to have: a quota error decoded for one movie
// must stop upstream calls for every other cache miss for the rest of the
// day too, not just for the query that triggered it. Before MarkExhausted
// existed, each of these misses would have spent its own doomed upstream
// call rediscovering the same exhausted key.
func TestQuotaErrorTripsBreakerForOtherQueries(t *testing.T) {
	var calls int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, quotaJSON)
	}))
	defer upstream.Close()

	h, _ := newTestHandler(t, upstream.URL, 900)

	first := doRequest(t, h, "i=tt0137523&apikey=client-key")
	if first.Code != http.StatusUnauthorized {
		t.Fatalf("first request status = %d, want 401", first.Code)
	}

	// A completely different, uncached query must not reach upstream
	// either: the first response's quota error tripped the breaker for
	// the whole day, not just for that one movie.
	second := doRequest(t, h, "i=tt9999999&apikey=client-key")
	if second.Code != http.StatusUnauthorized {
		t.Errorf("second request status = %d, want 401", second.Code)
	}
	if second.Body.String() != quotaJSON {
		t.Errorf("second request body = %q, want the quota body", second.Body.String())
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("upstream calls = %d, want 1 (breaker must stop the second query from trying upstream)", got)
	}
}

// TestQuotaErrorTripsBreakerServesStaleForOtherQuery covers the scenario
// the bug report called out specifically: when a stale entry exists for
// the next query, staleOrQuota hands the consumer a perfectly good STALE
// response, so the consumer never sees a quota error to react to. Without
// the breaker, nothing would ever stop the loop of doomed upstream calls
// hiding behind those STALE responses.
func TestQuotaErrorTripsBreakerServesStaleForOtherQuery(t *testing.T) {
	var calls int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, quotaJSON)
	}))
	defer upstream.Close()

	h, store := newTestHandler(t, upstream.URL, 900)

	// Seed a stale (expired) entry for a second, different query.
	staleBody := foundMovieJSON("1999")
	canonical := "i=tt-stale"
	key := sha256Hex(canonical)
	past := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := store.Put(t.Context(), cache.Entry{
		CacheKey:    key,
		Query:       canonical,
		Body:        []byte(staleBody),
		ContentType: "application/json",
		Status:      http.StatusOK,
		Found:       true,
		FetchedAt:   past,
		ExpiresAt:   &past,
	}); err != nil {
		t.Fatalf("seed stale entry: %v", err)
	}

	first := doRequest(t, h, "i=tt0137523&apikey=client-key")
	if first.Code != http.StatusUnauthorized {
		t.Fatalf("first request status = %d, want 401", first.Code)
	}

	second := doRequest(t, h, "i=tt-stale&apikey=client-key")
	if got := second.Header().Get("X-Cache"); got != "STALE" {
		t.Errorf("second request X-Cache = %q, want STALE", got)
	}
	if second.Body.String() != staleBody {
		t.Errorf("second request body = %q, want the stale cached body %q", second.Body.String(), staleBody)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("upstream calls = %d, want 1 (breaker must serve the stale entry without a second upstream call)", got)
	}
}

func TestNotFoundMissIsCached(t *testing.T) {
	var calls int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, notFoundJSON)
	}))
	defer upstream.Close()

	h, _ := newTestHandler(t, upstream.URL, 900)

	first := doRequest(t, h, "i=tt9999999&apikey=client-key")
	if first.Body.String() != notFoundJSON {
		t.Errorf("first request body = %q, want %q", first.Body.String(), notFoundJSON)
	}

	second := doRequest(t, h, "i=tt9999999&apikey=client-key")
	if got := second.Header().Get("X-Cache"); got != "HIT" {
		t.Errorf("second request X-Cache = %q, want HIT (not-found misses must be cached)", got)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("upstream calls = %d, want 1", got)
	}
}

func TestBudgetExhaustedWithNoCacheReturnsQuotaBodyWithoutCallingUpstream(t *testing.T) {
	var calls int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		t.Error("upstream must not be called when the daily budget is already exhausted")
	}))
	defer upstream.Close()

	h, _ := newTestHandler(t, upstream.URL, 0) // DAILY_BUDGET=0

	resp := doRequest(t, h, "i=tt0137523&apikey=client-key")
	if resp.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.Code)
	}
	if resp.Body.String() != quotaJSON {
		t.Errorf("body = %q, want OMDb's own quota body %q", resp.Body.String(), quotaJSON)
	}
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Errorf("upstream calls = %d, want 0", got)
	}
}

func TestStaleServedWhenUpstreamFails(t *testing.T) {
	// A server that is immediately closed makes every request fail
	// with a connection error — a stand-in for upstream being down.
	deadUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadUpstream.Close()

	h, store := newTestHandler(t, deadUpstream.URL, 900)

	staleBody := foundMovieJSON("1999")
	canonical := "i=tt0137523"
	key := sha256Hex(canonical)
	past := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := store.Put(t.Context(), cache.Entry{
		CacheKey:    key,
		Query:       canonical,
		Body:        []byte(staleBody),
		ContentType: "application/json",
		Status:      http.StatusOK,
		Found:       true,
		FetchedAt:   past,
		ExpiresAt:   &past, // already expired
	}); err != nil {
		t.Fatalf("seed stale entry: %v", err)
	}

	resp := doRequest(t, h, "i=tt0137523&apikey=client-key")
	if got := resp.Header().Get("X-Cache"); got != "STALE" {
		t.Errorf("X-Cache = %q, want STALE", got)
	}
	if resp.Body.String() != staleBody {
		t.Errorf("body = %q, want the stale cached body %q", resp.Body.String(), staleBody)
	}
	if resp.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 (the stale entry's original status)", resp.Code)
	}
}

func TestExpiryPolicyOldMovieIsPermanentRecentMovieIsFinite(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("i") {
		case "tt-old":
			fmt.Fprint(w, foundMovieJSON("1990"))
		case "tt-recent":
			fmt.Fprint(w, foundMovieJSON("2026")) // matches the fixed test clock's year
		}
	}))
	defer upstream.Close()

	h, store := newTestHandler(t, upstream.URL, 900)

	doRequest(t, h, "i=tt-old&apikey=client-key")
	doRequest(t, h, "i=tt-recent&apikey=client-key")

	oldEntry, err := store.Get(t.Context(), sha256Hex("i=tt-old"))
	if err != nil || oldEntry == nil {
		t.Fatalf("Get(old): entry=%v err=%v", oldEntry, err)
	}
	if oldEntry.ExpiresAt != nil {
		t.Errorf("old movie ExpiresAt = %v, want nil (permanent)", oldEntry.ExpiresAt)
	}

	recentEntry, err := store.Get(t.Context(), sha256Hex("i=tt-recent"))
	if err != nil || recentEntry == nil {
		t.Fatalf("Get(recent): entry=%v err=%v", recentEntry, err)
	}
	if recentEntry.ExpiresAt == nil {
		t.Errorf("recent movie ExpiresAt = nil, want a finite expiry")
	}
}

func TestConcurrentIdenticalRequestsCollapseToOneUpstreamCall(t *testing.T) {
	var calls int32
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		<-release // hold every concurrent caller open until we say go
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, foundMovieJSON("1999"))
	}))
	defer upstream.Close()

	h, _ := newTestHandler(t, upstream.URL, 900)

	const n = 20
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			doRequest(t, h, "i=tt0137523&apikey=client-key")
		}()
	}

	// Give every goroutine a chance to reach the upstream handler (or
	// pile up behind singleflight) before releasing the response.
	time.Sleep(100 * time.Millisecond)
	close(release)
	wg.Wait()

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("upstream calls = %d, want exactly 1 for %d concurrent identical requests", got, n)
	}
}

func TestResponseBodyIsByteIdenticalToUpstream(t *testing.T) {
	// Deliberately odd formatting (extra spaces, unusual field order)
	// to prove the proxy never re-encodes.
	const raw = `{ "Response" : "True",   "Year":"1999", "Title": "The Matrix" }`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, raw)
	}))
	defer upstream.Close()

	h, _ := newTestHandler(t, upstream.URL, 900)

	resp := doRequest(t, h, "i=tt0137523&apikey=client-key")
	if resp.Body.String() != raw {
		t.Errorf("body = %q, want byte-identical upstream body %q", resp.Body.String(), raw)
	}
}

func TestProxyTokenGate(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, foundMovieJSON("1999"))
	}))
	defer upstream.Close()

	dbPath := filepath.Join(t.TempDir(), "cache.db")
	store, err := cache.Open(dbPath)
	if err != nil {
		t.Fatalf("cache.Open: %v", err)
	}
	defer store.Close()

	h, err := proxy.New(store, proxy.Config{
		UpstreamURL: upstream.URL,
		APIKey:      proxyAPIKey,
		DailyBudget: 900,
		ProxyToken:  "secret-token",
	})
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}

	rejected := doRequest(t, h, "i=tt0137523&apikey=wrong-token")
	if rejected.Code != http.StatusUnauthorized {
		t.Errorf("wrong token: status = %d, want 401", rejected.Code)
	}

	req := httptest.NewRequest(http.MethodGet, "/?i=tt0137523", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("bearer token: status = %d, want 200", rec.Code)
	}
}

func TestHealthzAndStats(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, foundMovieJSON("1999"))
	}))
	defer upstream.Close()

	h, _ := newTestHandler(t, upstream.URL, 900)

	healthRec := httptest.NewRecorder()
	h.Healthz(healthRec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if healthRec.Body.String() != "ok" {
		t.Errorf("healthz body = %q, want %q", healthRec.Body.String(), "ok")
	}

	doRequest(t, h, "i=tt0137523&apikey=client-key")

	statsRec := httptest.NewRecorder()
	h.StatsHandler(statsRec, httptest.NewRequest(http.MethodGet, "/stats", nil))
	if statsRec.Code != http.StatusOK {
		t.Fatalf("stats status = %d, want 200", statsRec.Code)
	}
	if statsRec.Body.Len() == 0 {
		t.Fatal("stats body is empty")
	}
}

// sha256Hex mirrors the cache-key hashing done inside the proxy package
// (canonicalQuery + cacheKeyFor), duplicated here rather than exported
// so the test can independently verify the on-disk key without
// weakening the package's public API just for testability.
func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// TestRequestLoggingNeverLeaksKeys is the guard on the one thing a
// request log must never do here. The raw query carries the client's own
// apikey, and when PROXY_TOKEN is set it carries the proxy token itself,
// so the handler logs the canonical query — which has dropped it — and
// never r.URL.RawQuery.
func TestRequestLoggingNeverLeaksKeys(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, foundMovieJSON("1999"))
	}))
	defer upstream.Close()

	store, err := cache.Open(filepath.Join(t.TempDir(), "cache.db"))
	if err != nil {
		t.Fatalf("cache.Open: %v", err)
	}
	defer store.Close()

	var logs bytes.Buffer
	h, err := proxy.New(store, proxy.Config{
		UpstreamURL: upstream.URL,
		APIKey:      proxyAPIKey,
		DailyBudget: 900,
		ProxyToken:  "secret-token",
		Logger:      slog.New(slog.NewTextHandler(&logs, nil)),
	})
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}

	doRequest(t, h, "i=tt0137523&apikey=secret-token")
	doRequest(t, h, "i=tt0137523&apikey=secret-token")
	doRequest(t, h, "i=tt0137523&apikey=wrong-token")

	// A dead upstream is the path that produces an error log line, and
	// net/http's *url.Error carries the outgoing URL — with the proxy's
	// apikey on it — in its message.
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	dead.Close()

	deadStore, err := cache.Open(filepath.Join(t.TempDir(), "dead.db"))
	if err != nil {
		t.Fatalf("cache.Open: %v", err)
	}
	defer deadStore.Close()

	deadHandler, err := proxy.New(deadStore, proxy.Config{
		UpstreamURL: dead.URL,
		APIKey:      proxyAPIKey,
		DailyBudget: 900,
		Logger:      slog.New(slog.NewTextHandler(&logs, nil)),
	})
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}
	doRequest(t, deadHandler, "i=tt0137523")

	out := logs.String()
	if !strings.Contains(out, "request failed") {
		t.Errorf("want the failed upstream request logged, got:\n%s", out)
	}
	// A stack trace here would mean the error was logged as a value
	// rather than a string, which is also what would drag the URL along.
	if strings.Contains(out, "stack trace") {
		t.Errorf("log output contains a stack trace:\n%s", out)
	}
	for _, secret := range []string{"secret-token", "wrong-token", proxyAPIKey} {
		if strings.Contains(out, secret) {
			t.Errorf("log output contains %q:\n%s", secret, out)
		}
	}

	if got := strings.Count(out, `msg=request `); got != 2 {
		t.Errorf("request log lines = %d, want 2\n%s", got, out)
	}
	if !strings.Contains(out, "cache=MISS") || !strings.Contains(out, "cache=HIT") {
		t.Errorf("want one MISS and one HIT logged, got:\n%s", out)
	}
	if !strings.Contains(out, `query="i=tt0137523"`) {
		t.Errorf("want the canonical query logged, got:\n%s", out)
	}
	if !strings.Contains(out, "request rejected") {
		t.Errorf("want the rejected request logged, got:\n%s", out)
	}
}
