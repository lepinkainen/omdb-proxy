package proxy

import (
	"encoding/json"
	"regexp"
	"strings"
	"time"
)

// Expiry policy constants. Movie metadata for anything more than a year
// old essentially never changes, so the policy is deliberately skewed
// towards "cache forever" — the whole point of this proxy is to stop
// re-spending quota on the same old films.
const (
	// expiryRecentMovie is used when the release year looks like "this
	// year or later that isn't the future" — i.e. a title released
	// within roughly the last twelve months, which occasionally still
	// picks up rating or plot corrections.
	expiryRecentMovie = 7 * 24 * time.Hour

	// expiryUnknownYear covers responses whose year we could not parse
	// (missing, "N/A", or a genuinely future year, which usually means
	// an unreleased title whose metadata is still in flux). Kept short
	// so those entries self-correct quickly rather than going stale.
	expiryUnknownYear = 24 * time.Hour

	// expirySearch applies to s= search queries regardless of content:
	// the result set for a search term can grow as new movies are
	// added to OMDb's catalogue, so it is never cached permanently.
	expirySearch = 30 * 24 * time.Hour

	// DefaultNotFoundTTL is the fallback expiry for a Response:"False"
	// miss when NOTFOUND_TTL isn't set.
	//
	// A miss has two indistinguishable causes: the title genuinely has
	// no OMDb entry and never will, or it simply hasn't been added yet
	// and will appear in a few weeks. Caching permanently is correct
	// for the first case and permanently wrong for the second, and the
	// response gives no way to tell them apart. A 7-day TTL retries
	// the "not yet added" case cheaply while still collapsing the
	// "never will exist" case from "every project, every run" down to
	// one request a week for the whole fleet.
	//
	// This deliberately diverges from the per-project caches this
	// proxy replaces, which cached misses permanently. That was the
	// right call when each project paid its own 1000/day out of its
	// own pocket — retrying a genuine miss was pure waste. With one
	// shared key and one shared cache, a weekly retry costs a single
	// request across all consumers combined, so the trade flips: the
	// insurance against a late catalogue addition is now nearly free.
	DefaultNotFoundTTL = 7 * 24 * time.Hour
)

// omdbEnvelope captures just enough of OMDb's JSON shape to drive
// caching decisions. Consumers of this proxy decode the full response
// themselves for whatever fields they need — this proxy only ever needs
// to know whether the lookup succeeded, why it might have failed, and
// (best-effort) which year the title was released.
type omdbEnvelope struct {
	Response string `json:"Response"`
	Error    string `json:"Error"`
	Year     string `json:"Year"`
}

var yearPattern = regexp.MustCompile(`\d{4}`)

// parseEnvelope extracts the fields we care about from a raw OMDb
// response body. It is best-effort by design: OMDb's r=xml format is a
// niche path, and if the body doesn't parse as anything recognisable we
// fall back to safe defaults (not found, not a quota error, unknown
// year) rather than erroring the whole request. Callers are expected to
// have already read the actual body verbatim for the client response;
// this is a side read purely to inform cache policy.
func parseEnvelope(body []byte, contentType string) (found bool, quotaError bool, year int, yearOK bool) {
	if strings.Contains(strings.ToLower(contentType), "xml") {
		return parseXMLEnvelope(body)
	}

	var env omdbEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		// Content-Type lied, or the body is something unexpected
		// (HTML error page from a misbehaving upstream, etc). Fall
		// back to a raw substring scan so a quota error still gets
		// detected even when we can't parse structure out of it.
		return false, isQuotaError(string(body)), 0, false
	}

	found = strings.EqualFold(env.Response, "true")
	quotaError = isQuotaError(env.Error)
	year, yearOK = parseYear(env.Year)
	return found, quotaError, year, yearOK
}

// parseXMLEnvelope handles OMDb's r=xml format, e.g.
// `<root response="False"><error>Movie not found!</error></root>` for a
// miss, or `<root response="True"><movie ... year="1999" .../></root>`
// for a hit. We deliberately don't pull in encoding/xml for two
// attributes and a text node — a couple of targeted regexps are less
// code and tolerate the format drifting slightly.
func parseXMLEnvelope(body []byte) (found bool, quotaError bool, year int, yearOK bool) {
	text := string(body)

	if m := xmlResponseAttr.FindStringSubmatch(text); m != nil {
		found = strings.EqualFold(m[1], "true")
	}

	if m := xmlErrorElem.FindStringSubmatch(text); m != nil {
		quotaError = isQuotaError(m[1])
	} else {
		quotaError = isQuotaError(text)
	}

	if m := xmlYearAttr.FindStringSubmatch(text); m != nil {
		year, yearOK = parseYear(m[1])
	}

	return found, quotaError, year, yearOK
}

var (
	xmlResponseAttr = regexp.MustCompile(`response="([^"]*)"`)
	xmlErrorElem    = regexp.MustCompile(`<error>([^<]*)</error>`)
	xmlYearAttr     = regexp.MustCompile(`year="([^"]*)"`)
)

// isQuotaError matches OMDb's "Request limit reached!" message
// case-insensitively and on a substring, not an exact string. OMDb owns
// the exact wording and is free to change punctuation or capitalisation
// without notice; pinning the literal string would silently stop
// detecting quota exhaustion and start caching it as a real miss.
func isQuotaError(errMsg string) bool {
	return strings.Contains(strings.ToLower(errMsg), "request limit")
}

// parseYear pulls the first four-digit run out of an OMDb Year field,
// which can be a bare year ("1999"), a series range ("2010–2015"), or
// "N/A". Returns ok=false when nothing four-digit-shaped is present.
func parseYear(raw string) (year int, ok bool) {
	m := yearPattern.FindString(raw)
	if m == "" {
		return 0, false
	}
	var y int
	for _, c := range m {
		y = y*10 + int(c-'0')
	}
	return y, true
}

// expiryFor decides when a cache entry should stop being served as
// fresh. now is injected rather than read directly so tests can pin a
// release year relative to a fixed clock instead of racing the
// calendar.
//
// The year-based split intentionally works at calendar-year
// granularity rather than trying to reconstruct an exact release date
// from a four-digit field: a title whose Year equals the current
// calendar year is treated as "within the last year" (7-day expiry),
// and anything from an earlier calendar year is treated as "more than a
// year ago" (cached permanently). This slightly under-caches titles
// released very late last year and slightly over-caches ones released
// very early this year, but OMDb only ever gives us a year, not a full
// date, so this is the best split available — and it is deliberately
// biased towards the cheap side (permanent) once a title is unambiguously
// old.
//
// notFoundTTL is the expiry applied to Response:"False" misses; see
// DefaultNotFoundTTL for why that case is deliberately finite rather
// than permanent.
func expiryFor(now time.Time, isSearch bool, found bool, year int, yearOK bool, notFoundTTL time.Duration) *time.Time {
	if isSearch {
		t := now.Add(expirySearch)
		return &t
	}

	if !found {
		t := now.Add(notFoundTTL)
		return &t
	}

	if !yearOK || year > now.Year() {
		t := now.Add(expiryUnknownYear)
		return &t
	}

	if year < now.Year() {
		return nil // permanent
	}

	t := now.Add(expiryRecentMovie)
	return &t
}
