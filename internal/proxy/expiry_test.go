package proxy

import (
	"testing"
	"time"
)

func TestParseEnvelopeJSONFound(t *testing.T) {
	body := []byte(`{"Title":"The Matrix","Year":"1999","Response":"True"}`)
	found, quota, year, ok := parseEnvelope(body, "application/json")
	if !found {
		t.Error("found = false, want true")
	}
	if quota {
		t.Error("quota = true, want false")
	}
	if !ok || year != 1999 {
		t.Errorf("year = %d, ok = %v, want 1999, true", year, ok)
	}
}

func TestParseEnvelopeNotFound(t *testing.T) {
	body := []byte(`{"Response":"False","Error":"Movie not found!"}`)
	found, quota, _, _ := parseEnvelope(body, "application/json")
	if found {
		t.Error("found = true, want false")
	}
	if quota {
		t.Error("quota = true, want false — this is an ordinary miss")
	}
}

func TestParseEnvelopeQuotaError(t *testing.T) {
	body := []byte(`{"Response":"False","Error":"Request limit reached!"}`)
	found, quota, _, _ := parseEnvelope(body, "application/json")
	if found {
		t.Error("found = true, want false")
	}
	if !quota {
		t.Error("quota = false, want true")
	}
}

func TestParseEnvelopeQuotaErrorIsCaseInsensitiveAndNotPinnedToExactWording(t *testing.T) {
	body := []byte(`{"Response":"False","Error":"REQUEST LIMIT reached for today, sorry!"}`)
	_, quota, _, _ := parseEnvelope(body, "application/json")
	if !quota {
		t.Error("quota = false, want true — detection must match on substring, case-insensitively")
	}
}

func TestParseEnvelopeSeriesYearRange(t *testing.T) {
	body := []byte(`{"Response":"True","Year":"2010–2015"}`)
	_, _, year, ok := parseEnvelope(body, "application/json")
	if !ok || year != 2010 {
		t.Errorf("year = %d, ok = %v, want 2010, true", year, ok)
	}
}

func TestParseEnvelopeUnparseableYear(t *testing.T) {
	body := []byte(`{"Response":"True","Year":"N/A"}`)
	_, _, _, ok := parseEnvelope(body, "application/json")
	if ok {
		t.Error("ok = true, want false for an unparseable year")
	}
}

func TestExpiryForOldMovieIsPermanent(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	got := expiryFor(now, false, true, 1999, true, DefaultNotFoundTTL)
	if got != nil {
		t.Errorf("expiryFor = %v, want nil (permanent)", got)
	}
}

func TestExpiryForCurrentYearMovieIsFinite(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	got := expiryFor(now, false, true, 2026, true, DefaultNotFoundTTL)
	if got == nil {
		t.Fatal("expiryFor = nil, want a finite expiry for a current-year release")
	}
	if !got.After(now) {
		t.Errorf("expiry %v is not after now %v", got, now)
	}
	wantAround := now.Add(expiryRecentMovie)
	if got.Sub(wantAround).Abs() > time.Second {
		t.Errorf("expiry = %v, want approximately %v", got, wantAround)
	}
}

func TestExpiryForUnknownYearIsShort(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	got := expiryFor(now, false, true, 0, false, DefaultNotFoundTTL)
	if got == nil {
		t.Fatal("expiryFor = nil, want a finite expiry for an unknown year")
	}
	if got.Sub(now) != expiryUnknownYear {
		t.Errorf("expiry offset = %v, want %v", got.Sub(now), expiryUnknownYear)
	}
}

func TestExpiryForFutureYearIsShort(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	got := expiryFor(now, false, true, 2030, true, DefaultNotFoundTTL)
	if got == nil {
		t.Fatal("expiryFor = nil, want a finite expiry for a future year")
	}
	if got.Sub(now) != expiryUnknownYear {
		t.Errorf("expiry offset = %v, want %v", got.Sub(now), expiryUnknownYear)
	}
}

func TestExpiryForNotFoundIsFiniteNotPermanent(t *testing.T) {
	// Spec correction: not-found misses used to be cached permanently.
	// They now get a finite TTL because the proxy can't tell "will
	// never exist" apart from "not added to OMDb yet" — see
	// DefaultNotFoundTTL's doc comment.
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	got := expiryFor(now, false, false, 0, false, DefaultNotFoundTTL)
	if got == nil {
		t.Fatal("expiryFor = nil, want a finite expiry for a not-found miss")
	}
	if got.Sub(now) != DefaultNotFoundTTL {
		t.Errorf("expiry offset = %v, want %v", got.Sub(now), DefaultNotFoundTTL)
	}
}

func TestExpiryForNotFoundRespectsCustomTTL(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	custom := 3 * time.Hour
	got := expiryFor(now, false, false, 0, false, custom)
	if got == nil || got.Sub(now) != custom {
		t.Errorf("expiryFor = %v, want now+%v", got, custom)
	}
}

func TestExpiryForSearchIsAlways30DaysRegardlessOfContent(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)

	// Search with results.
	got := expiryFor(now, true, true, 1999, true, DefaultNotFoundTTL)
	if got == nil || got.Sub(now) != expirySearch {
		t.Errorf("search-with-results expiry = %v, want now+%v", got, expirySearch)
	}

	// Search with no results still gets the search TTL, not the
	// not-found TTL.
	got = expiryFor(now, true, false, 0, false, DefaultNotFoundTTL)
	if got == nil || got.Sub(now) != expirySearch {
		t.Errorf("search-with-no-results expiry = %v, want now+%v", got, expirySearch)
	}
}
