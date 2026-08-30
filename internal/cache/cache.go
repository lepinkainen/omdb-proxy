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

CREATE TABLE IF NOT EXISTS quota (
    day  TEXT PRIMARY KEY,
    used INTEGER NOT NULL
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
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, errors.Wrap(err, "create schema")
	}

	return &Store{db: db}, nil
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

// TryReserveQuota attempts to spend one unit of the given day's budget.
// It reports whether the reservation succeeded (used < budget before the
// call) and the number of requests used for that day afterwards.
//
// The check-then-increment happens inside a single transaction. Combined
// with the store's single-connection pool, this makes the whole
// operation atomic across concurrent callers: only one goroutine can
// hold the connection at a time, so no two callers can both observe
// used < budget and both increment past it.
func (s *Store) TryReserveQuota(ctx context.Context, day string, budget int) (reserved bool, used int, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, 0, errors.Wrap(err, "begin quota transaction")
	}
	defer tx.Rollback() //nolint:errcheck // no-op once committed

	row := tx.QueryRowContext(ctx, `SELECT used FROM quota WHERE day = ?`, day)
	if err := row.Scan(&used); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return false, 0, errors.Wrap(err, "read quota")
		}
		used = 0
	}

	if used >= budget {
		return false, used, nil
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO quota (day, used) VALUES (?, 1)
		ON CONFLICT(day) DO UPDATE SET used = used + 1
	`, day); err != nil {
		return false, 0, errors.Wrap(err, "increment quota")
	}

	if err := tx.Commit(); err != nil {
		return false, 0, errors.Wrap(err, "commit quota transaction")
	}

	return true, used + 1, nil
}

// MarkExhausted trips the local circuit breaker for the rest of day: it
// forces the day's used counter up to at least budget, so every later
// TryReserveQuota call for that day is refused before it ever reaches
// upstream.
//
// The local counter is only ever a *prediction* of OMDb's own quota —
// it can disagree with reality if the cache DB was recreated, the
// budget was raised, or something else spent requests against the
// same upstream key. An upstream response that decodes as a quota
// error is ground truth that the prediction was wrong, and the caller
// is expected to invoke this the moment it sees one, so the rest of
// the day's cache misses stop paying for upstream calls that are
// already known to fail. This matters most in combination with stale
// serving: when a stale cache entry exists, the caller hands the
// consumer a perfectly good STALE response and the consumer never
// sees the quota error itself, so nothing else in the system would
// otherwise learn that the key is exhausted for the day.
//
// The upsert only ever raises the counter (MAX), never lowers it, so
// a concurrent caller that has already pushed used past budget is not
// clobbered back down by a slightly stale MarkExhausted call.
func (s *Store) MarkExhausted(ctx context.Context, day string, budget int) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO quota (day, used) VALUES (?, ?)
		ON CONFLICT(day) DO UPDATE SET used = MAX(used, excluded.used)
	`, day, budget)
	if err != nil {
		return errors.Wrap(err, "mark quota exhausted")
	}
	return nil
}

// QuotaUsed returns the number of upstream requests already spent on the
// given day, for reporting via /stats.
func (s *Store) QuotaUsed(ctx context.Context, day string) (int, error) {
	row := s.db.QueryRowContext(ctx, `SELECT used FROM quota WHERE day = ?`, day)
	var used int
	if err := row.Scan(&used); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, errors.Wrap(err, "read quota")
	}
	return used, nil
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
