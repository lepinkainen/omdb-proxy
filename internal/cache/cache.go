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
    day          TEXT PRIMARY KEY,
    used         INTEGER NOT NULL,
    exhausted_at TEXT
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
	if err := migrateQuotaExhaustedAt(db); err != nil {
		db.Close()
		return nil, err
	}

	return &Store{db: db}, nil
}

// migrateQuotaExhaustedAt adds the quota.exhausted_at column to a
// database created before the circuit breaker was separated from the
// used counter. CREATE TABLE IF NOT EXISTS leaves a pre-existing table
// alone, so a deployment upgrading in place would otherwise keep the
// two-column table and fail every quota read. SQLite has no
// ADD COLUMN IF NOT EXISTS, hence the pragma check.
func migrateQuotaExhaustedAt(db *sql.DB) error {
	rows, err := db.Query(`SELECT name FROM pragma_table_info('quota')`)
	if err != nil {
		return errors.Wrap(err, "inspect quota columns")
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return errors.Wrap(err, "scan quota column name")
		}
		if name == "exhausted_at" {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return errors.Wrap(err, "iterate quota columns")
	}

	if _, err := db.Exec(`ALTER TABLE quota ADD COLUMN exhausted_at TEXT`); err != nil {
		return errors.Wrap(err, "add quota.exhausted_at column")
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

// Reservation is the outcome of a TryReserveQuota call.
type Reservation struct {
	// Granted reports whether the caller may spend an upstream request.
	Granted bool
	// Used is the day's counter after the call.
	Used int
	// Probe is true when this reservation was granted as the single
	// half-open probe past an armed breaker. The caller must report
	// what upstream said: MarkRecovered on a normal response,
	// MarkExhausted on another quota error. Nothing else re-closes the
	// breaker, so a dropped probe result leaves the proxy refusing
	// until the next interval lapses.
	Probe bool
}

// QuotaDay is the ledger state for one accounting day.
type QuotaDay struct {
	// Used counts upstream requests actually spent. Unlike the
	// pre-breaker design, nothing ever forces this to the budget: it is
	// a real measurement, which is what makes "did we spend it or did
	// upstream cut us off?" answerable after the fact.
	Used int
	// ExhaustedAt is when the breaker was last armed or probed, or nil
	// when it is closed.
	ExhaustedAt *time.Time
}

// TryReserveQuota attempts to spend one unit of the given day's budget,
// subject to both the local budget and the upstream circuit breaker.
//
// It refuses when the day's counter has reached budget, or when the
// breaker was armed (or last probed) less than probeInterval ago. Once
// that interval lapses, exactly one caller is granted a probe: the
// grant pushes exhausted_at forward to now, so every other caller
// arriving in the same interval is still refused. That is what keeps a
// burst of cache misses from turning into a burst of doomed upstream
// calls the moment the breaker becomes probeable.
//
// A probe deliberately ignores the budget check. The breaker is only
// ever armed by upstream telling us the key is spent, and a probe that
// succeeds resets the counter to zero anyway (see MarkRecovered), so
// the worst case is one request past the local cap — and the local cap
// exists to stay under OMDb's limit, which upstream has already told us
// we are at.
//
// The check-then-increment happens inside a single transaction.
// Combined with the store's single-connection pool, this makes the
// whole operation atomic across concurrent callers: only one goroutine
// can hold the connection at a time, so no two callers can both observe
// used < budget and both increment past it, and no two can both take
// the same probe.
func (s *Store) TryReserveQuota(ctx context.Context, day string, budget int, now time.Time, probeInterval time.Duration) (Reservation, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Reservation{}, errors.Wrap(err, "begin quota transaction")
	}
	defer tx.Rollback() //nolint:errcheck // no-op once committed

	var (
		used        int
		exhaustedAt sql.NullString
	)
	row := tx.QueryRowContext(ctx, `SELECT used, exhausted_at FROM quota WHERE day = ?`, day)
	if err := row.Scan(&used, &exhaustedAt); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return Reservation{}, errors.Wrap(err, "read quota")
		}
		used = 0
	}

	probe := false
	if exhaustedAt.Valid {
		armedAt, err := time.Parse(time.RFC3339, exhaustedAt.String)
		if err != nil {
			return Reservation{}, errors.Wrap(err, "parse exhausted_at")
		}
		if now.Sub(armedAt) < probeInterval {
			return Reservation{Used: used}, nil
		}
		probe = true
	}

	if !probe && used >= budget {
		return Reservation{Used: used}, nil
	}

	if probe {
		// Claim the probe by moving the breaker's timer to now, in the
		// same transaction that spends the request.
		if _, err := tx.ExecContext(ctx, `
			UPDATE quota SET used = used + 1, exhausted_at = ? WHERE day = ?
		`, now.UTC().Format(time.RFC3339), day); err != nil {
			return Reservation{}, errors.Wrap(err, "claim quota probe")
		}
	} else if _, err := tx.ExecContext(ctx, `
		INSERT INTO quota (day, used) VALUES (?, 1)
		ON CONFLICT(day) DO UPDATE SET used = used + 1
	`, day); err != nil {
		return Reservation{}, errors.Wrap(err, "increment quota")
	}

	if err := tx.Commit(); err != nil {
		return Reservation{}, errors.Wrap(err, "commit quota transaction")
	}

	return Reservation{Granted: true, Used: used + 1, Probe: probe}, nil
}

// MarkExhausted arms the upstream circuit breaker for day: it records
// that upstream reported the key spent as of now, which makes
// TryReserveQuota refuse until a probe interval has passed.
//
// The local counter is only ever a *prediction* of OMDb's own quota —
// it can disagree with reality if the cache DB was recreated, the
// budget was raised, or something else spent requests against the same
// upstream key. An upstream response that decodes as a quota error is
// ground truth that the prediction was wrong, and the caller is
// expected to invoke this the moment it sees one, so the rest of the
// day's cache misses stop paying for upstream calls that are already
// known to fail. This matters most in combination with stale serving:
// when a stale cache entry exists, the caller hands the consumer a
// perfectly good STALE response and the consumer never sees the quota
// error itself, so nothing else in the system would otherwise learn
// that the key is exhausted.
//
// It deliberately leaves used alone. An earlier design tripped the
// breaker by forcing used up to the budget, which worked but destroyed
// the evidence: a forfeited day and a genuinely spent one produced
// byte-identical rows, so neither /stats nor the dashboard could say
// which had happened.
//
// The timestamp only ever moves forward (MAX), so a slightly stale
// caller cannot wind the breaker back and hand out an early probe.
// RFC3339 in UTC is fixed-width, so lexical MAX is chronological MAX.
func (s *Store) MarkExhausted(ctx context.Context, day string, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO quota (day, used, exhausted_at) VALUES (?, 0, ?)
		ON CONFLICT(day) DO UPDATE SET exhausted_at = MAX(COALESCE(exhausted_at, ''), excluded.exhausted_at)
	`, day, now.UTC().Format(time.RFC3339))
	if err != nil {
		return errors.Wrap(err, "mark quota exhausted")
	}
	return nil
}

// MarkRecovered closes the breaker for day and resets the day's counter
// to zero. The caller invokes it when a probe comes back as an ordinary
// response rather than a quota error.
//
// Resetting used is the point, not a side effect. OMDb publishes no
// endpoint for remaining quota and no documented reset time, so the
// only observable boundary of an upstream quota day is the moment a
// previously refused key starts answering again. Treating that as the
// start of a fresh budget is what stops the proxy's UTC-midnight
// accounting from drifting against OMDb's own, which is precisely the
// failure this replaced: a quota error just after UTC midnight used to
// forfeit the entire following day.
//
// This assumes the proxy is the only spender of its key. If something
// else shares it, the reset is optimistic and the proxy will simply
// rediscover exhaustion on a later probe.
//
// The counter restarts at 1, not 0: the probe that discovered the
// recovery was itself served out of the new upstream day, so OMDb has
// already counted it. Erring towards over-counting is the safe
// direction — the budget exists to stay under a limit we cannot read.
func (s *Store) MarkRecovered(ctx context.Context, day string) error {
	if _, err := s.db.ExecContext(ctx, `
		UPDATE quota SET used = 1, exhausted_at = NULL WHERE day = ?
	`, day); err != nil {
		return errors.Wrap(err, "mark quota recovered")
	}
	return nil
}

// Quota returns the ledger state for the given day, for reporting via
// /stats and the index page. A day with no row is not an error: it is
// simply a day on which nothing has been spent yet.
func (s *Store) Quota(ctx context.Context, day string) (QuotaDay, error) {
	row := s.db.QueryRowContext(ctx, `SELECT used, exhausted_at FROM quota WHERE day = ?`, day)

	var (
		q           QuotaDay
		exhaustedAt sql.NullString
	)
	if err := row.Scan(&q.Used, &exhaustedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return QuotaDay{}, nil
		}
		return QuotaDay{}, errors.Wrap(err, "read quota")
	}
	if exhaustedAt.Valid {
		t, err := time.Parse(time.RFC3339, exhaustedAt.String)
		if err != nil {
			return QuotaDay{}, errors.Wrap(err, "parse exhausted_at")
		}
		q.ExhaustedAt = &t
	}
	return q, nil
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
