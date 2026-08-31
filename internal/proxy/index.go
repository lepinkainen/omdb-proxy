package proxy

import (
	"bytes"
	"embed"
	"fmt"
	"html/template"
	"net/http"
	"time"

	"github.com/lepinkainen/omdb-proxy/internal/cache"
)

//go:embed index.html
var indexHTMLFS embed.FS

// indexTemplate is parsed once at package init from the embedded
// index.html, rather than per-request, so a broken template fails at
// startup (well, at first use in tests/build) instead of on every page
// load. No external assets: everything the page needs is inlined in the
// template itself, because it has to render on a bare VPS with no
// internet access.
var indexTemplate = template.Must(template.New("index.html").Funcs(template.FuncMap{
	"utcTime":    formatUTC,
	"utcTimePtr": formatUTCPtr,
	"expiry":     formatExpiry,
}).ParseFS(indexHTMLFS, "index.html"))

// formatUTC renders t as an absolute UTC timestamp for the index page.
func formatUTC(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04 UTC")
}

// formatUTCPtr is formatUTC for the nullable Oldest/NewestFetch fields;
// the template decides what to show instead when the cache is empty.
func formatUTCPtr(t *time.Time) string {
	if t == nil {
		return ""
	}
	return formatUTC(*t)
}

// formatExpiry renders a cache.Summary's ExpiresAt for the recent-entries
// table: "never" for a permanent entry, "expired" once now has passed
// it, otherwise the absolute UTC timestamp.
func formatExpiry(exp *time.Time, now time.Time) string {
	if exp == nil {
		return "never"
	}
	if !now.Before(*exp) {
		return "expired"
	}
	return formatUTC(*exp)
}

// formatDuration renders a countdown as "12h04m". The dashboard's whole
// job here is to answer "when does this clear?" without the reader
// doing timezone arithmetic against their own clock, which is exactly
// the mistake a bare "resets at midnight UTC" invites.
func formatDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	d = d.Round(time.Minute)
	return fmt.Sprintf("%dh%02dm", int(d.Hours()), int(d.Minutes())%60)
}

// indexPageData is everything index.html needs to render. It is built
// fresh on every request from live store reads plus the in-memory
// counters — the index page is a dashboard, not something worth caching
// itself.
type indexPageData struct {
	Now time.Time

	QuotaUsedToday int
	QuotaBudget    int
	QuotaRemaining int
	QuotaExhausted bool

	// QuotaBreakerArmed separates "OMDb refused us" from "we spent our
	// own budget". Both show zero remaining and want different
	// reactions from whoever is reading the page, and conflating them
	// is what made a forfeited day look like a busy one.
	QuotaBreakerArmed bool
	QuotaExhaustedAt  *time.Time
	QuotaNextProbeAt  *time.Time
	QuotaResetsAt     time.Time
	QuotaResetsIn     string

	Stats  cache.Stats
	Recent []cache.Summary

	Hits           int64
	Misses         int64
	Stales         int64
	TotalServed    int64
	HitRatePercent float64
}

// serveIndex renders the human-readable dashboard at bare GET /. See the
// routing guard in ServeHTTP for why this path is reachable at all
// without shadowing real proxy traffic, and why it is deliberately
// ungated by PROXY_TOKEN.
//
// It still produces exactly one request log line, like every other path
// through ServeHTTP: cache="INDEX" and an empty canonical query, since
// there is no client query to report.
func (h *Handler) serveIndex(w http.ResponseWriter, r *http.Request, start time.Time) {
	ctx := r.Context()
	now := h.now()

	// The quota read mirrors StatsHandler's: if we can't even tell the
	// operator how much budget is left, the page isn't worth serving
	// half-broken, so this one failure is fatal like it is in /stats.
	quota, err := h.quotaView(ctx, now)
	if err != nil {
		h.logger.Error("index: read quota", "error", err.Error())
		h.logRequest("", "INDEX", http.StatusInternalServerError, start)
		http.Error(w, "omdb-proxy: failed to read quota", http.StatusInternalServerError)
		return
	}

	// Everything below is best-effort: a failure to read cache-wide
	// stats or the recent-entries list still leaves a useful page (quota
	// is the operator's most urgent question), so we log and carry on
	// with zero-value/empty data rather than failing the request.
	cacheStats, err := h.store.Stats(ctx, now)
	if err != nil {
		h.logger.Error("index: read cache stats", "error", err.Error())
	}

	recent, err := h.store.Recent(ctx, 25)
	if err != nil {
		h.logger.Error("index: read recent entries", "error", err.Error())
	}

	memStats := h.Stats()
	total := memStats.Hits + memStats.Misses + memStats.Stales
	var hitRate float64
	if total > 0 {
		hitRate = float64(memStats.Hits) / float64(total) * 100
	}

	data := indexPageData{
		Now: now,

		QuotaUsedToday: quota.Used,
		QuotaBudget:    quota.Budget,
		QuotaRemaining: quota.Remaining,
		QuotaExhausted: quota.Remaining == 0,

		QuotaBreakerArmed: quota.BreakerArmed,
		QuotaExhaustedAt:  quota.ExhaustedAt,
		QuotaNextProbeAt:  quota.NextProbeAt,
		QuotaResetsAt:     quota.ResetsAt,
		QuotaResetsIn:     formatDuration(quota.ResetsAt.Sub(now)),

		Stats:  cacheStats,
		Recent: recent,

		Hits:           memStats.Hits,
		Misses:         memStats.Misses,
		Stales:         memStats.Stales,
		TotalServed:    total,
		HitRatePercent: hitRate,
	}

	// Render into a buffer first: a template execution error partway
	// through would otherwise leave a half-written 200 response with no
	// way to turn it into a 500.
	//
	// Every value the template interpolates is either our own counters
	// or Query, which canonicalQuery has already stripped apikey from
	// before it ever reached the cache — this page must never display a
	// key. html/template's automatic HTML escaping is what keeps a
	// hostile query string (any other field OMDb echoes back) from
	// injecting markup into the page.
	var buf bytes.Buffer
	if err := indexTemplate.Execute(&buf, data); err != nil {
		h.logger.Error("index: render template", "error", err.Error())
		h.logRequest("", "INDEX", http.StatusInternalServerError, start)
		http.Error(w, "omdb-proxy: failed to render index", http.StatusInternalServerError)
		return
	}

	h.logRequest("", "INDEX", http.StatusOK, start)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())
}
