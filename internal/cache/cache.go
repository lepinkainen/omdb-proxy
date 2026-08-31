// Package cache implements the SQLite-backed response cache and daily
// quota accounting for the OMDb proxy.
//
// The store deliberately opens SQLite with a single connection
// (db.SetMaxOpenConns(1)). This proxy serves a handful of personal
// projects on a LAN, so the throughput cost is irrelevant, and it buys
// two things for free: PRAGMA settings (WAL, busy_timeout) only need to
// be applied once, and a BEGIN/…/COMMIT sequence is automatically
// atomic with respect to other goroutines, because database/sql will
// block any concurrent caller until the single connection is returned
// to the pool. That is what makes TryReserveQuota race-free without an
// additional application-level lock.
package cache

import (
	"context"
	"database/sql"
	"time"

	"github.com/cockroachdb/errors"
	_ "modernc.org/sqlite" // pure-Go SQLite driver, registers "sqlite"
)

// Entry is one cached OMDb response, stored and returned verbatim.
type Entry struct {
	CacheKey    string
	Query       string // canonical query string, kept for debugging
	Body        []byte // raw upstream bytes, byte-for-byte
	ContentType string
	Status      int  // upstream HTTP status code
	Found       bool // true when the upstream body says Response:"True"
	FetchedAt   time.Time
	ExpiresAt   *time.Time // nil means the entry never expires
}

// Expired reports whether e should be treated as stale as of now.
// A nil ExpiresAt means the entry is permanent and never expires.
func (e *Entry) Expired(now time.Time) bool {
	return e.ExpiresAt != nil && !now.Before(*e.ExpiresAt)
}

// Stats summarises the cache contents for the /stats admin endpoint and
// the index page.
type Stats struct {
	TotalRows     int
	PermanentRows int
	ExpiringRows  int
	FoundRows     int        // body said Response:"True"
	NotFoundRows  int        // body said Response:"False"
	ExpiredRows   int        // has an expires_at that is already in the past, as of the Stats call's now
	OldestFetch   *time.Time // nil when the cache is empty
	NewestFetch   *time.Time
}

// Summary is one row of the index page's recent-entries table. It
// deliberately omits Body: the page never renders response bodies, and
// selecting the blobs would make the query cost scale with cache size.
type Summary struct {
	Query     string
	Status    int
	Found     bool
	FetchedAt time.Time
	ExpiresAt *time.Time
}

// Store is the SQLite-backed cache and quota ledger.
type Store struct {
	db *sql.DB
}

const schema = `
CREATE TABLE IF NOT EXISTS responses (
    cache_key   TEXT PRIMARY KEY,
    query       TEXT NOT NULL,
    body        BLOB NOT NULL,
    content_type TEXT NOT NULL,
    status      INTEGER NOT NULL,
    found       INTEGER NOT NULL,
    fetched_at  TEXT NOT NULL,
    expires_at  TEXT
);

CREATE TABLE IF NOT EXISTS quota_state (
    id             INTEGER PRIMARY KEY CHECK (id = 1),
    used           INTEGER NOT NULL,
    counting_since TEXT NOT NULL,
    exhausted_at   TEXT
);
`

// Open creates or opens the SQLite database at path, applies the schema,
// and configures WAL mode with a busy timeout so that a slow writer
// makes readers wait instead of failing with SQLITE_BUSY.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, errors.Wrap(err, "open sqlite database")
	}

	// See the package doc comment for why a single connection is
	// deliberate rather than an oversight.
	db.SetMaxOpenConns(1)

	if _, err := db.Exec(`PRAGMA journal_mode = WAL;`); err != nil {
		db.Close()
		return nil, errors.Wrap(err, "enable WAL mode")
	}
	if _, err := db.Exec(`PRAGMA busy_timeout = 5000;`); err != nil {
		db.Close()
		return nil, errors.Wrap(err, "set busy timeout")
	}
	if err := dropLegacyQuotaTable(db); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, errors.Wrap(err, "create schema")
	}

	return &Store{db: db}, nil
}

// dropLegacyQuotaTable removes the UTC-day-keyed quota table that
// preceded the current ledger. It runs before the schema is applied, so
// a database still carrying it is never read on the old shape.
//
// Its rows cannot be carried over: each counted requests spent within
// one UTC calendar day, which is not a quantity this proxy tracks any
// more, and rows written by the pre-breaker code hold a forged used
// value (the old exhaustion marker pushed it up to the budget to stop
// itself). Starting the counter fresh costs nothing — it is a reporting
// number now, not a limit.
func dropLegacyQuotaTable(db *sql.DB) error {
	if _, err := db.Exec(`DROP TABLE IF EXISTS quota`); err != nil {
		return errors.Wrap(err, "drop legacy quota table")
	}
	return nil
}

// Close releases the underlying database handle.
func (s *Store) Close() error {
	return s.db.Close()
}

// Get looks up a cached entry by its cache key. It returns (nil, nil)
// when there is no such entry — a miss is not an error.
func (s *Store) Get(ctx context.Context, cacheKey string) (*Entry, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT cache_key, query, body, content_type, status, found, fetched_at, expires_at
		FROM responses WHERE cache_key = ?`, cacheKey)

	var e Entry
	var fetchedAt string
	var expiresAt sql.NullString
	var found int
	if err := row.Scan(&e.CacheKey, &e.Query, &e.Body, &e.ContentType, &e.Status, &found, &fetchedAt, &expiresAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "query cache entry")
	}

	e.Found = found != 0
	t, err := time.Parse(time.RFC3339, fetchedAt)
	if err != nil {
		return nil, errors.Wrap(err, "parse fetched_at")
	}
	e.FetchedAt = t

	if expiresAt.Valid {
		et, err := time.Parse(time.RFC3339, expiresAt.String)
		if err != nil {
			return nil, errors.Wrap(err, "parse expires_at")
		}
		e.ExpiresAt = &et
	}

	return &e, nil
}

// Put inserts or replaces the cached entry for e.CacheKey.
func (s *Store) Put(ctx context.Context, e Entry) error {
	var expiresAt sql.NullString
	if e.ExpiresAt != nil {
		expiresAt = sql.NullString{String: e.ExpiresAt.UTC().Format(time.RFC3339), Valid: true}
	}

	found := 0
	if e.Found {
		found = 1
	}

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO responses (cache_key, query, body, content_type, status, found, fetched_at, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(cache_key) DO UPDATE SET
			query = excluded.query,
			body = excluded.body,
			content_type = excluded.content_type,
			status = excluded.status,
			found = excluded.found,
			fetched_at = excluded.fetched_at,
			expires_at = excluded.expires_at
	`, e.CacheKey, e.Query, e.Body, e.ContentType, e.Status, found, e.FetchedAt.UTC().Format(time.RFC3339), expiresAt)
	if err != nil {
		return errors.Wrap(err, "store cache entry")
	}
	return nil
}

// QuotaState is what the proxy remembers about its upstream quota.
//
// It is deliberately thin. The proxy has no local budget and no timer: a
// cache miss simply tries upstream, and OMDb's own refusal is the only
// limit. So the only fact worth storing is whether upstream is refusing
// us right now — everything else here exists so an operator can see what
// has been happening.
type QuotaState struct {
	// Used counts upstream requests actually served since
	// CountingSince. Nothing reads it to make a decision; it is there
	// so /stats and the dashboard can show the spend.
	Used int
	// CountingSince is when Used started from zero: the last observed
	// upstream rollover, or first use.
	CountingSince time.Time
	// ExhaustedAt is when upstream last told us the key was spent, or
	// nil when it is serving normally. This is the whole control state.
	ExhaustedAt *time.Time
}

// rowQuerier is the shared surface of *sql.DB and *sql.Tx that readQuota
// needs.
type rowQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// readQuota returns the ledger as of now. An empty ledger is not an
// error: it is a proxy that has not spent anything yet.
func readQuota(ctx context.Context, q rowQuerier, now time.Time) (QuotaState, error) {
	var (
		used        int
		startedAt   string
		exhaustedAt sql.NullString
	)
	row := q.QueryRowContext(ctx, `SELECT used, counting_since, exhausted_at FROM quota_state WHERE id = 1`)
	if err := row.Scan(&used, &startedAt, &exhaustedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return QuotaState{CountingSince: now}, nil
		}
		return QuotaState{}, errors.Wrap(err, "read quota")
	}

	started, err := time.Parse(time.RFC3339, startedAt)
	if err != nil {
		return QuotaState{}, errors.Wrap(err, "parse counting_since")
	}

	state := QuotaState{Used: used, CountingSince: started}
	if exhaustedAt.Valid {
		t, err := time.Parse(time.RFC3339, exhaustedAt.String)
		if err != nil {
			return QuotaState{}, errors.Wrap(err, "parse exhausted_at")
		}
		state.ExhaustedAt = &t
	}
	return state, nil
}

// MarkExhausted records that upstream reported the key spent as of now.
//
// It changes nothing about what the proxy will do next — the next cache
// miss still tries upstream, because trying is the only way to learn
// that OMDb's day has rolled over. What it buys is the memory that makes
// the *next* success meaningful: see RecordServed.
//
// It also deliberately leaves used alone. An earlier design tripped a
// breaker by forcing used up to the budget, which destroyed the
// evidence: a period the proxy never got to use looked byte-identical to
// one it spent, so neither /stats nor the dashboard could say which had
// happened.
//
// The timestamp only ever moves forward (MAX), so a slightly stale
// caller cannot wind it back. RFC3339 in UTC is fixed-width, so lexical
// MAX is chronological MAX.
func (s *Store) MarkExhausted(ctx context.Context, now time.Time) error {
	stamp := now.UTC().Format(time.RFC3339)
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO quota_state (id, used, counting_since, exhausted_at) VALUES (1, 0, ?, ?)
		ON CONFLICT(id) DO UPDATE SET exhausted_at = MAX(COALESCE(exhausted_at, ''), excluded.exhausted_at)
	`, stamp, stamp)
	if err != nil {
		return errors.Wrap(err, "mark quota exhausted")
	}
	return nil
}

// RecordServed records one upstream request that OMDb actually answered.
//
// It carries the entire recovery rule, which is why it is one statement
// rather than a read and a branch: if the key was previously refused,
// this answer proves OMDb has rolled into a new quota day, so the
// counter restarts from this request and the refusal is forgotten.
// Otherwise it is an ordinary increment.
//
// OMDb publishes no endpoint for remaining quota and no documented reset
// time, so a refused key answering again is the only rollover it ever
// makes visible. This assumes the proxy is the sole spender of its key;
// if something else shares it, the restart is optimistic and the proxy
// simply rediscovers exhaustion on a later miss.
//
// The caller must only invoke this for a response that is recognisably
// OMDb answering — see recognisedEnvelope in the proxy package. A 502
// HTML page from a CDN and an "Invalid API key!" both lack a quota error
// while saying nothing about the quota day.
func (s *Store) RecordServed(ctx context.Context, now time.Time) error {
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO quota_state (id, used, counting_since, exhausted_at) VALUES (1, 1, ?, NULL)
		ON CONFLICT(id) DO UPDATE SET
			used = CASE WHEN exhausted_at IS NULL THEN used + 1 ELSE 1 END,
			counting_since = CASE WHEN exhausted_at IS NULL
				THEN counting_since ELSE excluded.counting_since END,
			exhausted_at = NULL
	`, now.UTC().Format(time.RFC3339)); err != nil {
		return errors.Wrap(err, "record served request")
	}
	return nil
}

// Quota returns the ledger for reporting via /stats and the index page.
func (s *Store) Quota(ctx context.Context, now time.Time) (QuotaState, error) {
	return readQuota(ctx, s.db, now)
}

// Stats reports cache-wide counts for the /stats admin endpoint and the
// index page. now is needed only to classify ExpiredRows; every other
// field is derived from the stored rows alone.
func (s *Store) Stats(ctx context.Context, now time.Time) (Stats, error) {
	// expires_at is compared lexicographically against now formatted the
	// same way Put writes it (now.UTC().Format(time.RFC3339)). That
	// format is fixed-width and always Z-suffixed (UTC), so string order
	// equals time order — this silently breaks if anything ever writes a
	// non-UTC or offset timestamp into expires_at. The comparison is <=,
	// not <, to match Entry.Expired's !now.Before(expires_at): an entry
	// expiring exactly now is already stale, and the two must not
	// disagree about it.
	nowStr := now.UTC().Format(time.RFC3339)

	row := s.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*),
			SUM(CASE WHEN expires_at IS NULL THEN 1 ELSE 0 END),
			SUM(CASE WHEN found != 0 THEN 1 ELSE 0 END),
			SUM(CASE WHEN found = 0 THEN 1 ELSE 0 END),
			SUM(CASE WHEN expires_at IS NOT NULL AND expires_at <= ? THEN 1 ELSE 0 END),
			MIN(fetched_at),
			MAX(fetched_at)
		FROM responses
	`, nowStr)

	var stats Stats
	var permanent, found, notFound, expired sql.NullInt64
	var oldest, newest sql.NullString
	if err := row.Scan(&stats.TotalRows, &permanent, &found, &notFound, &expired, &oldest, &newest); err != nil {
		return Stats{}, errors.Wrap(err, "read cache stats")
	}
	stats.PermanentRows = int(permanent.Int64)
	stats.ExpiringRows = stats.TotalRows - stats.PermanentRows
	stats.FoundRows = int(found.Int64)
	stats.NotFoundRows = int(notFound.Int64)
	stats.ExpiredRows = int(expired.Int64)

	if oldest.Valid {
		t, err := time.Parse(time.RFC3339, oldest.String)
		if err != nil {
			return Stats{}, errors.Wrap(err, "parse oldest fetched_at")
		}
		stats.OldestFetch = &t
	}
	if newest.Valid {
		t, err := time.Parse(time.RFC3339, newest.String)
		if err != nil {
			return Stats{}, errors.Wrap(err, "parse newest fetched_at")
		}
		stats.NewestFetch = &t
	}

	return stats, nil
}

// Recent lists the most recently fetched entries, newest first, for the
// index page's recent-entries table.
func (s *Store) Recent(ctx context.Context, limit int) ([]Summary, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT query, status, found, fetched_at, expires_at
		FROM responses
		ORDER BY fetched_at DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, errors.Wrap(err, "query recent entries")
	}
	defer rows.Close()

	var out []Summary
	for rows.Next() {
		var sum Summary
		var fetchedAt string
		var expiresAt sql.NullString
		var found int
		if err := rows.Scan(&sum.Query, &sum.Status, &found, &fetchedAt, &expiresAt); err != nil {
			return nil, errors.Wrap(err, "scan recent entry")
		}
		sum.Found = found != 0

		t, err := time.Parse(time.RFC3339, fetchedAt)
		if err != nil {
			return nil, errors.Wrap(err, "parse fetched_at")
		}
		sum.FetchedAt = t

		if expiresAt.Valid {
			et, err := time.Parse(time.RFC3339, expiresAt.String)
			if err != nil {
				return nil, errors.Wrap(err, "parse expires_at")
			}
			sum.ExpiresAt = &et
		}

		out = append(out, sum)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "iterate recent entries")
	}

	return out, nil
}
