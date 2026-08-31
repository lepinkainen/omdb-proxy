package cache

import (
	"context"
	"database/sql"
	"fmt"
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
// relative to it rather than to the wall clock so a probe interval can
// be crossed without sleeping.
var quotaTestClock = time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)

const testProbeInterval = 15 * time.Minute

func TestTryReserveQuotaEnforcesBudget(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	const day = "2026-08-30"

	res, err := store.TryReserveQuota(ctx, day, 2, quotaTestClock, testProbeInterval)
	if err != nil || !res.Granted || res.Used != 1 {
		t.Fatalf("1st reservation: %+v err=%v, want granted with used=1", res, err)
	}

	res, err = store.TryReserveQuota(ctx, day, 2, quotaTestClock, testProbeInterval)
	if err != nil || !res.Granted || res.Used != 2 {
		t.Fatalf("2nd reservation: %+v err=%v, want granted with used=2", res, err)
	}

	res, err = store.TryReserveQuota(ctx, day, 2, quotaTestClock, testProbeInterval)
	if err != nil || res.Granted {
		t.Fatalf("3rd reservation: %+v err=%v, want refused (budget exhausted)", res, err)
	}
	if res.Probe {
		t.Error("a budget refusal must never be reported as a probe: nothing upstream has refused us")
	}

	got, err := store.Quota(ctx, day)
	if err != nil {
		t.Fatalf("Quota: %v", err)
	}
	if got.Used != 2 {
		t.Errorf("Used = %d, want 2", got.Used)
	}
	if got.ExhaustedAt != nil {
		t.Errorf("ExhaustedAt = %v, want nil: spending the local budget is not an upstream refusal", got.ExhaustedAt)
	}
}

// TestMarkExhaustedArmsBreakerWithoutTouchingUsed pins the split that
// makes a forfeited day diagnosable. The old design tripped the breaker
// by forcing used up to the budget, which left a day the proxy never
// got to use looking byte-identical to one it spent legitimately.
func TestMarkExhaustedArmsBreakerWithoutTouchingUsed(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	const day = "2026-08-30"

	for i := 0; i < 3; i++ {
		if _, err := store.TryReserveQuota(ctx, day, 900, quotaTestClock, testProbeInterval); err != nil {
			t.Fatalf("TryReserveQuota: %v", err)
		}
	}

	if err := store.MarkExhausted(ctx, day, quotaTestClock); err != nil {
		t.Fatalf("MarkExhausted: %v", err)
	}

	got, err := store.Quota(ctx, day)
	if err != nil {
		t.Fatalf("Quota: %v", err)
	}
	if got.Used != 3 {
		t.Errorf("Used = %d, want 3: MarkExhausted must record what upstream said, not rewrite what we spent", got.Used)
	}
	if got.ExhaustedAt == nil || !got.ExhaustedAt.Equal(quotaTestClock) {
		t.Fatalf("ExhaustedAt = %v, want %v", got.ExhaustedAt, quotaTestClock)
	}

	// Refused immediately afterwards despite 897 of the budget being
	// nominally free: upstream's word beats the local prediction.
	res, err := store.TryReserveQuota(ctx, day, 900, quotaTestClock.Add(time.Minute), testProbeInterval)
	if err != nil {
		t.Fatalf("TryReserveQuota: %v", err)
	}
	if res.Granted {
		t.Error("granted = true, want false while the breaker is freshly armed")
	}

	// A different day must be entirely unaffected.
	other, err := store.Quota(ctx, "2026-08-31")
	if err != nil {
		t.Fatalf("Quota(other day): %v", err)
	}
	if other.Used != 0 || other.ExhaustedAt != nil {
		t.Errorf("other day = %+v, want zero value (the breaker must not leak across days)", other)
	}
}

// TestBreakerGrantsOneProbePerInterval is the core of the recovery
// path: once the interval lapses exactly one caller gets through, and
// the grant re-arms the timer so a burst of misses cannot become a
// burst of doomed upstream calls.
func TestBreakerGrantsOneProbePerInterval(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	const day = "2026-08-30"

	if err := store.MarkExhausted(ctx, day, quotaTestClock); err != nil {
		t.Fatalf("MarkExhausted: %v", err)
	}

	justBefore := quotaTestClock.Add(testProbeInterval - time.Second)
	res, err := store.TryReserveQuota(ctx, day, 900, justBefore, testProbeInterval)
	if err != nil {
		t.Fatalf("TryReserveQuota: %v", err)
	}
	if res.Granted {
		t.Fatal("granted = true one second before the interval lapsed, want false")
	}

	lapsed := quotaTestClock.Add(testProbeInterval)
	res, err = store.TryReserveQuota(ctx, day, 900, lapsed, testProbeInterval)
	if err != nil {
		t.Fatalf("TryReserveQuota: %v", err)
	}
	if !res.Granted || !res.Probe {
		t.Fatalf("reservation = %+v, want a granted probe once the interval lapsed", res)
	}

	// The second caller in the same instant must be refused: the probe
	// is a single request, not an open door.
	res, err = store.TryReserveQuota(ctx, day, 900, lapsed, testProbeInterval)
	if err != nil {
		t.Fatalf("TryReserveQuota: %v", err)
	}
	if res.Granted {
		t.Error("granted = true for a second caller in the same interval, want false (only one probe at a time)")
	}

	// ...and the next probe is due an interval after the probe, not
	// after the original arming.
	res, err = store.TryReserveQuota(ctx, day, 900, lapsed.Add(testProbeInterval), testProbeInterval)
	if err != nil {
		t.Fatalf("TryReserveQuota: %v", err)
	}
	if !res.Granted || !res.Probe {
		t.Errorf("reservation = %+v, want a second probe one interval after the first", res)
	}
}

// TestProbeIgnoresSpentBudget covers the deliberate one-request
// overshoot: a probe is about detecting OMDb's day boundary, and
// refusing it because the local counter is full would leave the proxy
// unable to ever discover that the boundary had passed.
func TestProbeIgnoresSpentBudget(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	const day = "2026-08-30"

	if _, err := store.TryReserveQuota(ctx, day, 1, quotaTestClock, testProbeInterval); err != nil {
		t.Fatalf("TryReserveQuota: %v", err)
	}
	if err := store.MarkExhausted(ctx, day, quotaTestClock); err != nil {
		t.Fatalf("MarkExhausted: %v", err)
	}

	res, err := store.TryReserveQuota(ctx, day, 1, quotaTestClock.Add(testProbeInterval), testProbeInterval)
	if err != nil {
		t.Fatalf("TryReserveQuota: %v", err)
	}
	if !res.Granted || !res.Probe {
		t.Fatalf("reservation = %+v, want a granted probe even with the budget spent", res)
	}
}

// TestMarkRecoveredClosesBreakerAndResetsCounter pins the behaviour
// that fixes the forfeited-day bug: a probe that comes back normal
// means OMDb has rolled into a new quota day, so the proxy's day rolls
// with it instead of waiting for UTC midnight.
func TestMarkRecoveredClosesBreakerAndResetsCounter(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	const day = "2026-08-30"

	for i := 0; i < 4; i++ {
		if _, err := store.TryReserveQuota(ctx, day, 900, quotaTestClock, testProbeInterval); err != nil {
			t.Fatalf("TryReserveQuota: %v", err)
		}
	}
	if err := store.MarkExhausted(ctx, day, quotaTestClock); err != nil {
		t.Fatalf("MarkExhausted: %v", err)
	}
	if err := store.MarkRecovered(ctx, day); err != nil {
		t.Fatalf("MarkRecovered: %v", err)
	}

	got, err := store.Quota(ctx, day)
	if err != nil {
		t.Fatalf("Quota: %v", err)
	}
	if got.Used != 1 {
		t.Errorf("Used = %d, want 1: a recovered probe starts a fresh upstream day, having spent one request itself", got.Used)
	}
	if got.ExhaustedAt != nil {
		t.Errorf("ExhaustedAt = %v, want nil after recovery", got.ExhaustedAt)
	}

	res, err := store.TryReserveQuota(ctx, day, 900, quotaTestClock.Add(time.Minute), testProbeInterval)
	if err != nil {
		t.Fatalf("TryReserveQuota: %v", err)
	}
	if !res.Granted || res.Probe {
		t.Errorf("reservation = %+v, want an ordinary grant once the breaker is closed", res)
	}
}

// TestMarkExhaustedNeverWindsTheBreakerBack guards the MAX in the
// upsert: a slightly stale caller must not shorten the wait and hand
// out an early probe.
func TestMarkExhaustedNeverWindsTheBreakerBack(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	const day = "2026-08-30"

	if err := store.MarkExhausted(ctx, day, quotaTestClock); err != nil {
		t.Fatalf("MarkExhausted: %v", err)
	}
	if err := store.MarkExhausted(ctx, day, quotaTestClock.Add(-10*time.Minute)); err != nil {
		t.Fatalf("MarkExhausted (stale): %v", err)
	}

	got, err := store.Quota(ctx, day)
	if err != nil {
		t.Fatalf("Quota: %v", err)
	}
	if got.ExhaustedAt == nil || !got.ExhaustedAt.Equal(quotaTestClock) {
		t.Errorf("ExhaustedAt = %v, want %v (an older mark must not wind the breaker back)", got.ExhaustedAt, quotaTestClock)
	}

	// Idempotent: repeating the same mark changes nothing.
	if err := store.MarkExhausted(ctx, day, quotaTestClock); err != nil {
		t.Fatalf("MarkExhausted (repeat): %v", err)
	}
	got, err = store.Quota(ctx, day)
	if err != nil {
		t.Fatalf("Quota: %v", err)
	}
	if got.ExhaustedAt == nil || !got.ExhaustedAt.Equal(quotaTestClock) {
		t.Errorf("ExhaustedAt = %v, want %v", got.ExhaustedAt, quotaTestClock)
	}
}

func TestQuotaForUnknownDayIsZero(t *testing.T) {
	store := openTestStore(t)
	got, err := store.Quota(context.Background(), "1999-01-01")
	if err != nil {
		t.Fatalf("Quota: %v", err)
	}
	if got.Used != 0 || got.ExhaustedAt != nil {
		t.Errorf("Quota = %+v, want zero value", got)
	}
}

// TestOpenMigratesPreBreakerQuotaTable covers upgrading a deployed
// database in place: CREATE TABLE IF NOT EXISTS leaves the old
// two-column quota table alone, so without the ALTER every quota read
// would fail on a VPS that has been running since before the breaker
// column existed.
func TestOpenMigratesPreBreakerQuotaTable(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "legacy.db")

	legacy, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	if _, err := legacy.Exec(`CREATE TABLE quota (day TEXT PRIMARY KEY, used INTEGER NOT NULL);
		INSERT INTO quota (day, used) VALUES ('2026-08-30', 42);`); err != nil {
		t.Fatalf("create legacy schema: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy handle: %v", err)
	}

	store, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open on a pre-breaker database: %v", err)
	}
	defer store.Close()

	got, err := store.Quota(context.Background(), "2026-08-30")
	if err != nil {
		t.Fatalf("Quota after migration: %v", err)
	}
	if got.Used != 42 {
		t.Errorf("Used = %d, want 42 (the existing ledger must survive the migration)", got.Used)
	}
	if got.ExhaustedAt != nil {
		t.Errorf("ExhaustedAt = %v, want nil on a freshly migrated row", got.ExhaustedAt)
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

func TestStatsEmptyCacheHasNilFetchTimes(t *testing.T) {
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
	if stats.NewestFetch != nil {
		t.Errorf("NewestFetch = %v, want nil on an empty cache", stats.NewestFetch)
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
	if stats.NewestFetch == nil || !stats.NewestFetch.Equal(now) {
		t.Errorf("NewestFetch = %v, want %v", stats.NewestFetch, now)
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
