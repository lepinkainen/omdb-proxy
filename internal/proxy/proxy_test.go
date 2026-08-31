package proxy_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
func newTestHandler(t *testing.T, upstreamURL string) (*proxy.Handler, *cache.Store) {
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
		HTTPClient:  &http.Client{Timeout: 2 * time.Second},
		Now:         func() time.Time { return fixedNow },
	})
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}
	return h, store
}

// newTestHandlerWithClock is newTestHandler with a caller-controlled
// clock and probe interval, for the breaker tests: they need to cross
// an interval boundary, and sleeping through a real one would make the
// suite unusable.
func newTestHandlerWithClock(t *testing.T, upstreamURL string, now *atomic.Pointer[time.Time]) (*proxy.Handler, *cache.Store) {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "cache.db")
	store, err := cache.Open(dbPath)
	if err != nil {
		t.Fatalf("cache.Open: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	h, err := proxy.New(store, proxy.Config{
		UpstreamURL: upstreamURL,
		APIKey:      proxyAPIKey,
		HTTPClient:  &http.Client{Timeout: 2 * time.Second},
		Now:         func() time.Time { return *now.Load() },
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

	h, _ := newTestHandler(t, upstream.URL)

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

	h, _ := newTestHandler(t, upstream.URL)

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

	h, _ := newTestHandler(t, upstream.URL)
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

	// A movable clock, because recovery is only credited to a request
	// issued strictly after the refusal it clears. Stored timestamps are
	// second-granular, so a retry in the same second as the refusal is
	// deliberately not treated as proof of a rollover.
	refusedAt := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	clock := &atomic.Pointer[time.Time]{}
	clock.Store(&refusedAt)

	h, store := newTestHandlerWithClock(t, upstream.URL, clock)

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

	// A second request for the same query tries upstream again, and this
	// time OMDb answers — which is exactly how the proxy learns the
	// quota day has rolled over. Nothing was cached to stop it trying.
	retriedAt := refusedAt.Add(time.Minute)
	clock.Store(&retriedAt)
	second := doRequest(t, h, "i=tt0137523&apikey=client-key")
	if second.Code != http.StatusOK {
		t.Errorf("second request status = %d, want 200", second.Code)
	}
	if second.Body.String() != foundMovieJSON("1999") {
		t.Errorf("second request body = %q, want the movie", second.Body.String())
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("upstream calls = %d, want 2 (the retry is what discovers the reset)", got)
	}

	// The served answer is cached, unlike the quota error before it.
	entry, err = store.Get(t.Context(), key)
	if err != nil {
		t.Fatalf("store.Get: %v", err)
	}
	if entry == nil {
		t.Fatal("the served response was not cached")
	}

	quota, err := store.Quota(t.Context(), retriedAt)
	if err != nil {
		t.Fatalf("store.Quota: %v", err)
	}
	if quota.ExhaustedAt != nil {
		t.Errorf("ExhaustedAt = %v, want nil once OMDb answered again", quota.ExhaustedAt)
	}
	if quota.Used != 1 {
		t.Errorf("Used = %d, want 1: the answering request is the first of the new quota day", quota.Used)
	}
}

// TestEveryMissRetriesWhileUpstreamRefuses is the deliberate inverse of
// what this test used to assert. An earlier design recorded a quota
// error and then refused to call upstream again for the rest of the day,
// which meant the proxy sat idle for hours holding a key that had
// started working again.
//
// OMDb publishes no way to ask how much quota is left and documents no
// reset time, so a request it actually answers is the only evidence its
// day has rolled over. Refusing to make that request is refusing to ever
// find out. The doomed calls in between are the accepted price: upstream
// is already refusing, so they cost latency, not quota.
func TestEveryMissRetriesWhileUpstreamRefuses(t *testing.T) {
	var calls int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, quotaJSON)
	}))
	defer upstream.Close()

	h, _ := newTestHandler(t, upstream.URL)

	const misses = 5
	for i := 0; i < misses; i++ {
		rec := doRequest(t, h, fmt.Sprintf("i=tt000000%d&apikey=client-key", i))
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("request %d status = %d, want 401", i, rec.Code)
		}
		if rec.Body.String() != quotaJSON {
			t.Errorf("request %d body = %q, want upstream's quota body verbatim", i, rec.Body.String())
		}
	}

	if got := atomic.LoadInt32(&calls); got != misses {
		t.Errorf("upstream calls = %d, want %d (one per cache miss)", got, misses)
	}
}

// TestQuotaErrorServesStaleForOtherQuery: when a stale entry exists,
// the consumer gets a perfectly good STALE response rather than an
// error, and never sees the quota error at all. That is the point of
// serving stale — a consumer must never break because the proxy's key
// is spent.
func TestQuotaErrorServesStaleForOtherQuery(t *testing.T) {
	var calls int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, quotaJSON)
	}))
	defer upstream.Close()

	h, store := newTestHandler(t, upstream.URL)

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
	// The second request did try upstream — every miss does — and was
	// refused again; what matters is that the consumer got the stale
	// body rather than an error, and never saw the quota response.
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("upstream calls = %d, want 2 (each miss tries; the stale body is what the consumer sees)", got)
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

	h, _ := newTestHandler(t, upstream.URL)

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

func TestStaleServedWhenUpstreamFails(t *testing.T) {
	// A server that is immediately closed makes every request fail
	// with a connection error — a stand-in for upstream being down.
	deadUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadUpstream.Close()

	h, store := newTestHandler(t, deadUpstream.URL)

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

	h, store := newTestHandler(t, upstream.URL)

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

	h, _ := newTestHandler(t, upstream.URL)

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

	h, _ := newTestHandler(t, upstream.URL)

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

	h, _ := newTestHandler(t, upstream.URL)

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

// TestBareRootServesIndexWithoutTouchingUpstream is the test that catches
// the index accidentally falling through into resolve: a bare GET / with
// no query string must render the HTML dashboard entirely from the
// cache/quota store, never by treating "" as a canonical query and
// asking upstream about it.
func TestBareRootServesIndexWithoutTouchingUpstream(t *testing.T) {
	var calls int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, foundMovieJSON("1999"))
	}))
	defer upstream.Close()

	h, _ := newTestHandler(t, upstream.URL)

	// Seed a cache entry so the recent-entries table has something
	// recognisable to assert on.
	doRequest(t, h, "i=tt0137523&apikey=client-key")
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("setup: upstream calls = %d, want 1", got)
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("Content-Type = %q, want a text/html prefix", ct)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "upstream requests served since") {
		t.Errorf("index body does not report the upstream spend:\n%s", body)
	}
	if !strings.Contains(body, "i=tt0137523") {
		t.Errorf("index body does not mention the seeded cached query:\n%s", body)
	}

	// The critical assertion: rendering the index must not have made a
	// second upstream call.
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("upstream calls = %d, want 1 (bare GET / must never reach upstream)", got)
	}
}

// TestQueryStringRequestStillBehavesAsProxy locks in that the index
// guard only fires on a completely empty raw query — any real proxy
// request, even one that happens to target the same path, keeps the
// existing MISS/upstream-call/byte-identical behaviour.
func TestQueryStringRequestStillBehavesAsProxy(t *testing.T) {
	var calls int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, foundMovieJSON("1999"))
	}))
	defer upstream.Close()

	h, _ := newTestHandler(t, upstream.URL)

	resp := doRequest(t, h, "i=tt0137523&apikey=client-key")
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("upstream calls = %d, want 1", got)
	}
	if got := resp.Header().Get("X-Cache"); got != "MISS" {
		t.Errorf("X-Cache = %q, want MISS", got)
	}
	if resp.Body.String() != foundMovieJSON("1999") {
		t.Errorf("body = %q, want the upstream body verbatim", resp.Body.String())
	}
}

// TestIndexIsUngatedByProxyToken locks in the deliberate decision that
// the index page is reachable even when PROXY_TOKEN is set — a gated
// index would be unreachable from a plain browser, since the token is
// presented as ?apikey=..., which makes the query non-empty and routes
// back into the (still-gated) proxy path.
func TestIndexIsUngatedByProxyToken(t *testing.T) {
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

	h, err := proxy.New(store, proxy.Config{
		UpstreamURL: upstream.URL,
		APIKey:      proxyAPIKey,
		ProxyToken:  "secret-token",
	})
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}

	indexReq := httptest.NewRequest(http.MethodGet, "/", nil)
	indexRec := httptest.NewRecorder()
	h.ServeHTTP(indexRec, indexReq)
	if indexRec.Code != http.StatusOK {
		t.Errorf("bare GET / with PROXY_TOKEN set: status = %d, want 200 (index is ungated)", indexRec.Code)
	}
	if ct := indexRec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("bare GET / Content-Type = %q, want a text/html prefix", ct)
	}

	// A real proxy request without the token must still be rejected.
	rejected := doRequest(t, h, "i=tt0137523")
	if rejected.Code != http.StatusUnauthorized {
		t.Errorf("proxy request without token: status = %d, want 401", rejected.Code)
	}
}

// TestIndexNeverLeaksAPIKeys guards the one thing the index page must
// never do: display a key. It seeds a request that carried a client
// apikey (which canonicalQuery strips before the query ever reaches the
// cache) and asserts neither that client key nor the proxy's own
// upstream key appears anywhere in the rendered HTML.
func TestIndexNeverLeaksAPIKeys(t *testing.T) {
	const clientKey = "clients-own-secret-key"

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, foundMovieJSON("1999"))
	}))
	defer upstream.Close()

	h, _ := newTestHandler(t, upstream.URL)
	doRequest(t, h, "i=tt0137523&apikey="+clientKey)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	body := rec.Body.String()
	if strings.Contains(body, clientKey) {
		t.Errorf("index body contains the client's apikey %q:\n%s", clientKey, body)
	}
	if strings.Contains(body, proxyAPIKey) {
		t.Errorf("index body contains the proxy's own upstream key %q:\n%s", proxyAPIKey, body)
	}
}

// TestIndexGuardPathAndQueryEdges pins the two edges of the index guard's
// condition, both of which are easy to get wrong when someone "tidies" it.
//
// "/?" is an empty RawQuery as far as net/url is concerned, so it renders
// the index — correct, because it is the same parameter-less request by
// another spelling. A non-root path, meanwhile, must never be diverted:
// the mux hands every unmatched path to this handler, and OMDb-style
// clients that build a base URL with a trailing path segment must keep
// being proxied.
func TestIndexGuardPathAndQueryEdges(t *testing.T) {
	var calls int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, foundMovieJSON("1999"))
	}))
	defer upstream.Close()

	h, _ := newTestHandler(t, upstream.URL)

	forcedQuery := httptest.NewRequest(http.MethodGet, "/?", nil)
	forcedRec := httptest.NewRecorder()
	h.ServeHTTP(forcedRec, forcedQuery)
	if ct := forcedRec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf(`GET "/?" Content-Type = %q, want a text/html prefix (empty RawQuery is still the index)`, ct)
	}
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Errorf(`GET "/?" made %d upstream calls, want 0`, got)
	}

	nested := httptest.NewRequest(http.MethodGet, "/somepath?i=tt0137523", nil)
	nestedRec := httptest.NewRecorder()
	h.ServeHTTP(nestedRec, nested)
	if got := nestedRec.Header().Get("X-Cache"); got != "MISS" {
		t.Errorf("non-root path X-Cache = %q, want MISS (must be proxied, not diverted to the index)", got)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("non-root path made %d upstream calls in total, want 1", got)
	}
}

// TestStatsReportsUpstreamRefusal covers the reporting half: an
// operator looking at /stats has to be able to tell "OMDb is refusing
// this key" from "everything is fine", since the proxy keeps serving
// cached and stale responses either way and looks identical from
// outside.
func TestStatsReportsUpstreamRefusal(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, quotaJSON)
	}))
	defer upstream.Close()

	h, _ := newTestHandler(t, upstream.URL)

	var before map[string]any
	decodeStats(t, h, &before)
	if before["quota_exhausted_upstream"] != false {
		t.Errorf("quota_exhausted_upstream = %v before any refusal, want false", before["quota_exhausted_upstream"])
	}
	if before["quota_counting_since"] != "2026-08-30T12:00:00Z" {
		t.Errorf("quota_counting_since = %v, want the current instant on an unspent ledger", before["quota_counting_since"])
	}
	if _, ok := before["quota_budget"]; ok {
		t.Error("quota_budget is still reported, want it gone: there is no local budget")
	}
	if _, ok := before["quota_remaining"]; ok {
		t.Error("quota_remaining is still reported, want it gone: there is no local budget")
	}

	doRequest(t, h, "i=tt0137523&apikey=client-key")

	var after map[string]any
	decodeStats(t, h, &after)
	if after["quota_exhausted_upstream"] != true {
		t.Errorf("quota_exhausted_upstream = %v after a quota error, want true", after["quota_exhausted_upstream"])
	}
	if after["quota_exhausted_at"] != "2026-08-30T12:00:00Z" {
		t.Errorf("quota_exhausted_at = %v, want when upstream refused us", after["quota_exhausted_at"])
	}
	// Being refused is not spending.
	if after["quota_used"] != float64(0) {
		t.Errorf("quota_used = %v, want 0: a refused request served nothing", after["quota_used"])
	}
}

func decodeStats(t *testing.T, h *proxy.Handler, into any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	rec := httptest.NewRecorder()
	h.StatsHandler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("/stats status = %d, want 200", rec.Code)
	}
	if err := json.Unmarshal(rec.Body.Bytes(), into); err != nil {
		t.Fatalf("decode /stats: %v", err)
	}
}

// TestUnrecognisedResponseIsNotTreatedAsRecovery covers the ways a
// request can come back without a transport error and still prove
// nothing: an HTML error page from something between us and OMDb, and
// OMDb rejecting the key outright. Neither says a new quota day has
// begun, so neither may clear the refusal or restart the count.
func TestUnrecognisedResponseIsNotTreatedAsRecovery(t *testing.T) {
	for _, tc := range []struct {
		name        string
		status      int
		contentType string
		body        string
	}{
		{"gateway error page", http.StatusBadGateway, "text/html", "<html><body>502 Bad Gateway</body></html>"},
		{"html body with a 200", http.StatusOK, "text/html", "<html><body>hello</body></html>"},
		{"invalid api key", http.StatusUnauthorized, "application/json", `{"Response":"False","Error":"Invalid API key!"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var (
				calls     int32
				exhausted atomic.Bool
			)
			exhausted.Store(true)

			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&calls, 1)
				if exhausted.Load() {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusUnauthorized)
					fmt.Fprint(w, quotaJSON)
					return
				}
				w.Header().Set("Content-Type", tc.contentType)
				w.WriteHeader(tc.status)
				fmt.Fprint(w, tc.body)
			}))
			defer upstream.Close()

			refusedAt := time.Date(2026, 8, 30, 1, 0, 0, 0, time.UTC)
			clock := &atomic.Pointer[time.Time]{}
			clock.Store(&refusedAt)

			h, store := newTestHandlerWithClock(t, upstream.URL, clock)

			// Record a real refusal first.
			doRequest(t, h, "i=tt0137523&apikey=client-key")
			if got := atomic.LoadInt32(&calls); got != 1 {
				t.Fatalf("upstream calls = %d, want 1", got)
			}

			// A later miss gets something that is not an OMDb answer.
			exhausted.Store(false)
			later := refusedAt.Add(15 * time.Minute)
			clock.Store(&later)
			doRequest(t, h, "i=tt9999999&apikey=client-key")
			if got := atomic.LoadInt32(&calls); got != 2 {
				t.Fatalf("upstream calls = %d, want 2 (the retry)", got)
			}

			quota, err := store.Quota(t.Context(), *clock.Load())
			if err != nil {
				t.Fatalf("store.Quota: %v", err)
			}
			if quota.ExhaustedAt == nil {
				t.Fatal("the refusal was forgotten, want it still recorded")
			}
			if !quota.ExhaustedAt.Equal(refusedAt) {
				t.Errorf("ExhaustedAt = %v, want %v (unchanged: nothing new was learned)", quota.ExhaustedAt, refusedAt)
			}
			if quota.Used != 0 {
				t.Errorf("Used = %d, want 0: an unrecognised response served nothing and must not restart the count", quota.Used)
			}
			if !quota.CountingSince.Equal(refusedAt) {
				t.Errorf("CountingSince = %v, want %v (unmoved)", quota.CountingSince, refusedAt)
			}
		})
	}
}

// TestOrdinaryMissCountsAsRecovery pins the other side of that bar: a
// plain "Movie not found!" is a 200 with a real OMDb envelope, which is
// proof the key was served and therefore proof of a new quota day.
func TestOrdinaryMissCountsAsRecovery(t *testing.T) {
	var (
		calls     int32
		exhausted atomic.Bool
	)
	exhausted.Store(true)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.Header().Set("Content-Type", "application/json")
		if exhausted.Load() {
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, quotaJSON)
			return
		}
		fmt.Fprint(w, notFoundJSON)
	}))
	defer upstream.Close()

	const probeInterval = 15 * time.Minute
	start := time.Date(2026, 8, 30, 1, 0, 0, 0, time.UTC)
	clock := &atomic.Pointer[time.Time]{}
	clock.Store(&start)

	h, store := newTestHandlerWithClock(t, upstream.URL, clock)

	doRequest(t, h, "i=tt0137523&apikey=client-key")
	exhausted.Store(false)
	probeTime := start.Add(probeInterval)
	clock.Store(&probeTime)
	doRequest(t, h, "i=tt9999999&apikey=client-key")

	quota, err := store.Quota(t.Context(), *clock.Load())
	if err != nil {
		t.Fatalf("store.Quota: %v", err)
	}
	if quota.ExhaustedAt != nil {
		t.Errorf("ExhaustedAt = %v, want nil: a served miss proves the key works", quota.ExhaustedAt)
	}
	if quota.Used != 1 {
		t.Errorf("Used = %d, want 1 (fresh budget, minus the probe)", quota.Used)
	}
}

// TestIndexRendersUpstreamRefusal exercises the dashboard's other
// branch. A mistyped field there is invisible until the page is
// rendered in exactly this state, which is the state an operator is
// most likely to be looking at it in.
func TestIndexRendersUpstreamRefusal(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, quotaJSON)
	}))
	defer upstream.Close()

	h, _ := newTestHandler(t, upstream.URL)
	doRequest(t, h, "i=tt0137523&apikey=client-key")

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("index status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "OMDb is refusing this key") {
		t.Errorf("index does not report the refusal:\n%s", body)
	}
	if !strings.Contains(body, "2026-08-30 12:00 UTC") {
		t.Errorf("index does not show when upstream refused us:\n%s", body)
	}
	if !strings.Contains(body, "will try upstream again") {
		t.Errorf("index does not say the next miss retries anyway:\n%s", body)
	}
}
