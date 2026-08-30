package proxy

import (
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"strings"
)

// canonicalQuery produces a stable, cache-key-worthy representation of an
// incoming OMDb-style query.
//
// The client's apikey is dropped entirely: two projects hitting this
// proxy with different upstream keys (or none at all, once PROXY_TOKEN
// is in play) must still land on the same cache row for the same movie.
// "i" and "type" are lowercased because IMDb ids and the type filter are
// case-insensitive upstream, and "t" (title) is lowercased with
// whitespace collapsed because OMDb title matching is also
// case-insensitive and tolerant of stray spacing. Every value is
// trimmed, and the result is sorted by key so that parameter order in
// the original request never affects the key.
func canonicalQuery(raw url.Values) string {
	out := url.Values{}
	for key, values := range raw {
		lowerKey := strings.ToLower(strings.TrimSpace(key))
		if lowerKey == "apikey" {
			continue
		}
		if len(values) == 0 {
			continue
		}
		value := strings.TrimSpace(values[0])
		if value == "" {
			continue
		}

		switch lowerKey {
		case "i", "type":
			value = strings.ToLower(value)
		case "t":
			value = strings.ToLower(collapseWhitespace(value))
		}

		out.Set(lowerKey, value)
	}
	// url.Values.Encode sorts by key, giving a deterministic string.
	return out.Encode()
}

// cacheKeyFor hashes a canonical query string down to the primary key
// stored in the responses table.
func cacheKeyFor(canonical string) string {
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])
}

// collapseWhitespace turns runs of whitespace into a single space, so
// "The   Matrix" and "The Matrix" canonicalise identically.
func collapseWhitespace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
