package cache

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/cockroachdb/errors"
	"path/filepath"
	"testing"
	"time"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "cache.db")
	store, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestGetMissing(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	entry, err := store.Get(ctx, "does-not-exist")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if entry != nil {
		t.Fatalf("expected nil entry for a miss, got %+v", entry)
	}
}

func TestPutAndGetRoundTrip(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	expires := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	want := Entry{
		CacheKey:    "abc123",
		Query:       "i=tt0137523",
		Body:        []byte(`{"Title":"The Matrix"}`),
		ContentType: "application/json",
		Status:      200,
		Found:       true,
		FetchedAt:   time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		ExpiresAt:   &expires,
	}

	if err := store.Put(ctx, want); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := store.Get(ctx, want.CacheKey)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil {
		t.Fatal("expected an entry, got nil")
	}
	if string(got.Body) != string(want.Body) {
		t.Errorf("Body = %q, want %q", got.Body, want.Body)
	}
	if got.ContentType != want.ContentType {
		t.Errorf("ContentType = %q, want %q", got.ContentType, want.ContentType)
	}
	if got.Status != want.Status {
		t.Errorf("Status = %d, want %d", got.Status, want.Status)
	}
	if got.Found != want.Found {
		t.Errorf("Found = %v, want %v", got.Found, want.Found)
	}
	if got.ExpiresAt == nil || !got.ExpiresAt.Equal(expires) {
		t.Errorf("ExpiresAt = %v, want %v", got.ExpiresAt, expires)
	}
}

func TestPutOverwritesExistingEntry(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	base := Entry{
		CacheKey:    "k",
		Query:       "i=tt1",
		Body:        []byte("old"),
		ContentType: "text/plain",
		Status:      200,
		Found:       true,
		FetchedAt:   time.Now().UTC(),
	}
	if err := store.Put(ctx, base); err != nil {
		t.Fatalf("first Put: %v", err)
	}

	base.Body = []byte("new")
	if err := store.Put(ctx, base); err != nil {
		t.Fatalf("second Put: %v", err)
	}

	got, err := store.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got.Body) != "new" {
		t.Errorf("Body = %q, want %q (overwrite should win)", got.Body, "new")
	}
}

func TestPermanentEntryHasNilExpiry(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	if err := store.Put(ctx, Entry{
		CacheKey:    "permanent",
		Query:       "i=tt1",
		Body:        []byte("{}"),
		ContentType: "application/json",
		Status:      200,
		Found:       false,
		FetchedAt:   time.Now().UTC(),
		ExpiresAt:   nil,
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := store.Get(ctx, "permanent")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ExpiresAt != nil {
		t.Errorf("ExpiresAt = %v, want nil (permanent)", got.ExpiresAt)
	}
	if got.Expired(time.Now().Add(100 * 365 * 24 * time.Hour)) {
		t.Error("a permanent entry must never report as expired")
	}
}

// quotaTestClock is an arbitrary fixed instant; quota tests move
// relative to it rather than to the wall clock.
var quotaTestClock = time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)

func TestRecordServedCountsUpstreamRequests(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if err := store.RecordServed(ctx, quotaTestClock); err != nil {
			t.Fatalf("RecordServed: %v", err)
		}
	}

	got, err := store.Quota(ctx, quotaTestClock)
	if err != nil {
		t.Fatalf("Quota: %v", err)
	}
	if got.Used != 3 {
		t.Errorf("Used = %d, want 3", got.Used)
	}
	if got.ExhaustedAt != nil {
		t.Errorf("ExhaustedAt = %v, want nil", got.ExhaustedAt)
	}
	if !got.CountingSince.Equal(quotaTestClock) {
		t.Errorf("CountingSince = %v, want %v (the first request starts the count)", got.CountingSince, quotaTestClock)
	}
}

// TestRecordServedAfterExhaustionRestartsTheCount is the whole recovery
// rule. OMDb exposes no way to read remaining quota and documents no
// reset time, so a request it actually answers, after refusing us, is
// the only evidence its day has rolled over.
func TestRecordServedAfterExhaustionRestartsTheCount(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if err := store.RecordServed(ctx, quotaTestClock); err != nil {
			t.Fatalf("RecordServed: %v", err)
		}
	}
	if err := store.MarkExhausted(ctx, quotaTestClock.Add(time.Hour)); err != nil {
		t.Fatalf("MarkExhausted: %v", err)
	}

	// Still five: being refused is not spending.
	got, err := store.Quota(ctx, quotaTestClock)
	if err != nil {
		t.Fatalf("Quota: %v", err)
	}
	if got.Used != 5 {
		t.Errorf("Used = %d, want 5: MarkExhausted must record what upstream said, not rewrite what we spent", got.Used)
	}
	if got.ExhaustedAt == nil {
		t.Fatal("ExhaustedAt = nil, want the refusal recorded")
	}

	// Hours later OMDb answers again: a new quota day.
	rollover := quotaTestClock.Add(9 * time.Hour)
	if err := store.RecordServed(ctx, rollover); err != nil {
		t.Fatalf("RecordServed: %v", err)
	}

	got, err = store.Quota(ctx, rollover)
	if err != nil {
		t.Fatalf("Quota: %v", err)
	}
	if got.Used != 1 {
		t.Errorf("Used = %d, want 1: the answering request is the first of the new day", got.Used)
	}
	if got.ExhaustedAt != nil {
		t.Errorf("ExhaustedAt = %v, want nil once upstream answers again", got.ExhaustedAt)
	}
	if !got.CountingSince.Equal(rollover) {
		t.Errorf("CountingSince = %v, want %v (the observed rollover)", got.CountingSince, rollover)
	}

	// And the next one increments from there rather than restarting.
	if err := store.RecordServed(ctx, rollover.Add(time.Minute)); err != nil {
		t.Fatalf("RecordServed: %v", err)
	}
	got, err = store.Quota(ctx, rollover)
	if err != nil {
		t.Fatalf("Quota: %v", err)
	}
	if got.Used != 2 || !got.CountingSince.Equal(rollover) {
		t.Errorf("Quota = %+v, want used=2 counting from %v", got, rollover)
	}
}

// TestLateSuccessFromBeforeARefusalIsNotRecovery pins the causality
// rule. Requests for different cache keys run concurrently, so a
// response OMDb accepted just before it started refusing can be written
// after the refusal has landed. Treating that as proof of a rollover
// would clear a refusal that still stands and collapse the day's
// measured spend to 1 — which is precisely the "did we spend it or did
// OMDb cut us off?" blindness this ledger exists to prevent.
func TestLateSuccessFromBeforeARefusalIsNotRecovery(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	issuedEarly := quotaTestClock
	for i := 0; i < 4; i++ {
		if err := store.RecordServed(ctx, issuedEarly); err != nil {
			t.Fatalf("RecordServed: %v", err)
		}
	}

	refusedAt := quotaTestClock.Add(time.Minute)
	if err := store.MarkExhausted(ctx, refusedAt); err != nil {
		t.Fatalf("MarkExhausted: %v", err)
	}

	// A request issued before the refusal, landing after it.
	if err := store.RecordServed(ctx, refusedAt.Add(-30*time.Second)); err != nil {
		t.Fatalf("RecordServed (late arrival): %v", err)
	}

	got, err := store.Quota(ctx, refusedAt)
	if err != nil {
		t.Fatalf("Quota: %v", err)
	}
	if got.ExhaustedAt == nil {
		t.Fatal("the refusal was cleared by a request issued before it, want it kept")
	}
	if !got.ExhaustedAt.Equal(refusedAt) {
		t.Errorf("ExhaustedAt = %v, want %v", got.ExhaustedAt, refusedAt)
	}
	if got.Used != 5 {
		t.Errorf("Used = %d, want 5: the late arrival is still a served request, just not a rollover", got.Used)
	}
	if !got.CountingSince.Equal(quotaTestClock) {
		t.Errorf("CountingSince = %v, want %v (unmoved)", got.CountingSince, quotaTestClock)
	}

	// A request issued after the refusal is the real thing.
	rollover := refusedAt.Add(time.Hour)
	if err := store.RecordServed(ctx, rollover); err != nil {
		t.Fatalf("RecordServed (after the refusal): %v", err)
	}
	got, err = store.Quota(ctx, rollover)
	if err != nil {
		t.Fatalf("Quota: %v", err)
	}
	if got.ExhaustedAt != nil {
		t.Errorf("ExhaustedAt = %v, want nil", got.ExhaustedAt)
	}
	if got.Used != 1 || !got.CountingSince.Equal(rollover) {
		t.Errorf("Quota = %+v, want used=1 counting from %v", got, rollover)
	}
}

// TestMarkExhaustedNeverWindsTheTimestampBack guards the MAX in the
// upsert, so a slightly stale caller cannot rewrite when upstream
// refused us.
func TestMarkExhaustedNeverWindsTheTimestampBack(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	if err := store.MarkExhausted(ctx, quotaTestClock); err != nil {
		t.Fatalf("MarkExhausted: %v", err)
	}
	if err := store.MarkExhausted(ctx, quotaTestClock.Add(-10*time.Minute)); err != nil {
		t.Fatalf("MarkExhausted (stale): %v", err)
	}

	got, err := store.Quota(ctx, quotaTestClock)
	if err != nil {
		t.Fatalf("Quota: %v", err)
	}
	if got.ExhaustedAt == nil || !got.ExhaustedAt.Equal(quotaTestClock) {
		t.Errorf("ExhaustedAt = %v, want %v", got.ExhaustedAt, quotaTestClock)
	}
}

func TestQuotaOnAnEmptyLedgerIsZero(t *testing.T) {
	store := openTestStore(t)
	got, err := store.Quota(context.Background(), quotaTestClock)
	if err != nil {
		t.Fatalf("Quota: %v", err)
	}
	if got.Used != 0 || got.ExhaustedAt != nil {
		t.Errorf("Quota = %+v, want an unspent ledger", got)
	}
	if !got.CountingSince.Equal(quotaTestClock) {
		t.Errorf("CountingSince = %v, want now (%v)", got.CountingSince, quotaTestClock)
	}
}

// TestOpenDropsTheLegacyDayKeyedQuotaTable covers upgrading a deployed
// database. The old rows counted spend within a UTC calendar day, which
// this proxy no longer tracks, and rows written by the pre-breaker code
// hold a forged used value. The drop has to happen before the schema is
// applied, or a legacy database would be read on the old shape.
func TestOpenDropsTheLegacyDayKeyedQuotaTable(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "legacy.db")

	legacy, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	if _, err := legacy.Exec(`CREATE TABLE quota (day TEXT PRIMARY KEY, used INTEGER NOT NULL);
		INSERT INTO quota (day, used) VALUES ('2026-08-31', 900);`); err != nil {
		t.Fatalf("create legacy schema: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy handle: %v", err)
	}

	store, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open on a pre-simplification database: %v", err)
	}
	defer store.Close()

	var name string
	err = store.db.QueryRow(`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'quota'`).Scan(&name)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("legacy quota table still present (err=%v), want it dropped", err)
	}

	got, err := store.Quota(context.Background(), quotaTestClock)
	if err != nil {
		t.Fatalf("Quota after migration: %v", err)
	}
	if got.Used != 0 {
		t.Errorf("Used = %d, want 0: a forged legacy counter must not carry over", got.Used)
	}
}

func TestStatsCountsPermanentAndExpiringRows(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	future := time.Now().Add(24 * time.Hour)
	rows := []Entry{
		{CacheKey: "permanent-1", Query: "q1", Body: []byte("{}"), ContentType: "application/json", Status: 200, FetchedAt: time.Now(), ExpiresAt: nil},
		{CacheKey: "permanent-2", Query: "q2", Body: []byte("{}"), ContentType: "application/json", Status: 200, FetchedAt: time.Now(), ExpiresAt: nil},
		{CacheKey: "expiring-1", Query: "q3", Body: []byte("{}"), ContentType: "application/json", Status: 200, FetchedAt: time.Now(), ExpiresAt: &future},
	}
	for _, r := range rows {
		if err := store.Put(ctx, r); err != nil {
			t.Fatalf("Put(%s): %v", r.CacheKey, err)
		}
	}

	stats, err := store.Stats(ctx, time.Now())
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.TotalRows != 3 {
		t.Errorf("TotalRows = %d, want 3", stats.TotalRows)
	}
	if stats.PermanentRows != 2 {
		t.Errorf("PermanentRows = %d, want 2", stats.PermanentRows)
	}
	if stats.ExpiringRows != 1 {
		t.Errorf("ExpiringRows = %d, want 1", stats.ExpiringRows)
	}
}

func TestStatsEmptyCacheHasNilOldestFetch(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	stats, err := store.Stats(ctx, time.Now())
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.TotalRows != 0 {
		t.Errorf("TotalRows = %d, want 0", stats.TotalRows)
	}
	if stats.OldestFetch != nil {
		t.Errorf("OldestFetch = %v, want nil on an empty cache", stats.OldestFetch)
	}
}

func TestStatsCountsFoundNotFoundAndExpiredRows(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	past := now.Add(-24 * time.Hour)  // already expired as of now
	future := now.Add(24 * time.Hour) // not yet expired as of now

	rows := []Entry{
		{CacheKey: "found-permanent", Query: "q1", Body: []byte("{}"), ContentType: "application/json", Status: 200, Found: true, FetchedAt: now.Add(-2 * time.Hour), ExpiresAt: nil},
		{CacheKey: "found-expired", Query: "q2", Body: []byte("{}"), ContentType: "application/json", Status: 200, Found: true, FetchedAt: now.Add(-1 * time.Hour), ExpiresAt: &past},
		{CacheKey: "notfound-live", Query: "q3", Body: []byte("{}"), ContentType: "application/json", Status: 200, Found: false, FetchedAt: now, ExpiresAt: &future},
	}
	for _, r := range rows {
		if err := store.Put(ctx, r); err != nil {
			t.Fatalf("Put(%s): %v", r.CacheKey, err)
		}
	}

	stats, err := store.Stats(ctx, now)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.FoundRows != 2 {
		t.Errorf("FoundRows = %d, want 2", stats.FoundRows)
	}
	if stats.NotFoundRows != 1 {
		t.Errorf("NotFoundRows = %d, want 1", stats.NotFoundRows)
	}
	if stats.ExpiredRows != 1 {
		t.Errorf("ExpiredRows = %d, want 1 (only the past-expiry row)", stats.ExpiredRows)
	}
	if stats.OldestFetch == nil || !stats.OldestFetch.Equal(now.Add(-2*time.Hour)) {
		t.Errorf("OldestFetch = %v, want %v", stats.OldestFetch, now.Add(-2*time.Hour))
	}
}

func TestRecentOrdersNewestFirstAndRespectsLimit(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	base := time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		e := Entry{
			CacheKey:    fmt.Sprintf("key-%d", i),
			Query:       fmt.Sprintf("i=tt%d", i),
			Body:        []byte("{}"),
			ContentType: "application/json",
			Status:      200,
			Found:       true,
			FetchedAt:   base.Add(time.Duration(i) * time.Hour),
		}
		if err := store.Put(ctx, e); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}

	recent, err := store.Recent(ctx, 3)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	if len(recent) != 3 {
		t.Fatalf("len(Recent) = %d, want 3 (limit respected)", len(recent))
	}

	wantOrder := []string{"i=tt4", "i=tt3", "i=tt2"}
	for i, want := range wantOrder {
		if recent[i].Query != want {
			t.Errorf("Recent[%d].Query = %q, want %q (newest first)", i, recent[i].Query, want)
		}
	}
}
