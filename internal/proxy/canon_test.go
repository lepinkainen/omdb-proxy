package proxy

import (
	"net/url"
	"testing"
)

func TestCanonicalQueryDropsAPIKey(t *testing.T) {
	raw := url.Values{"i": {"tt0137523"}, "apikey": {"whatever"}}
	got := canonicalQuery(raw)
	if got != "i=tt0137523" {
		t.Errorf("canonicalQuery = %q, want %q", got, "i=tt0137523")
	}
}

func TestCanonicalQueryIgnoresKeyOrderAndCase(t *testing.T) {
	a := canonicalQuery(url.Values{"i": {"TT0137523"}, "apikey": {"a"}})
	b := canonicalQuery(url.Values{"apikey": {"b"}, "I": {"tt0137523"}})
	if a != b {
		t.Errorf("canonical queries differ: %q vs %q", a, b)
	}
	if cacheKeyFor(a) != cacheKeyFor(b) {
		t.Errorf("cache keys differ for equivalent queries")
	}
}

func TestCanonicalQueryLowercasesTitleAndCollapsesWhitespace(t *testing.T) {
	a := canonicalQuery(url.Values{"t": {"The   Matrix"}})
	b := canonicalQuery(url.Values{"t": {"the matrix"}})
	if a != b {
		t.Errorf("title canonicalisation differs: %q vs %q", a, b)
	}
}

func TestCanonicalQueryTrimsWhitespace(t *testing.T) {
	a := canonicalQuery(url.Values{"i": {"  tt0137523  "}})
	b := canonicalQuery(url.Values{"i": {"tt0137523"}})
	if a != b {
		t.Errorf("whitespace trimming differs: %q vs %q", a, b)
	}
}

func TestCacheKeyForDeterministic(t *testing.T) {
	q := canonicalQuery(url.Values{"i": {"tt0137523"}})
	k1 := cacheKeyFor(q)
	k2 := cacheKeyFor(q)
	if k1 != k2 {
		t.Errorf("cacheKeyFor is not deterministic: %q vs %q", k1, k2)
	}
	if len(k1) != 64 {
		t.Errorf("cacheKeyFor length = %d, want 64 (hex sha256)", len(k1))
	}
}
