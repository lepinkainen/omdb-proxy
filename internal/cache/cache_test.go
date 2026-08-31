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
// relative to it rather than to the wall clock so an interval or an
// epoch can be crossed without sleeping.
var quotaTestClock = time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)

const testProbeInterval = 15 * time.Minute

func TestTryReserveQuotaEnforcesBudget(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	res, err := store.TryReserveQuota(ctx, 2, quotaTestClock, testProbeInterval)
	if err != nil || !res.Granted || res.Used != 1 {
		t.Fatalf("1st reservation: %+v err=%v, want granted with used=1", res, err)
	}

	res, err = store.TryReserveQuota(ctx, 2, quotaTestClock, testProbeInterval)
	if err != nil || !res.Granted || res.Used != 2 {
		t.Fatalf("2nd reservation: %+v err=%v, want granted with used=2", res, err)
	}

	res, err = store.TryReserveQuota(ctx, 2, quotaTestClock, testProbeInterval)
	if err != nil || res.Granted {
		t.Fatalf("3rd reservation: %+v err=%v, want refused (budget exhausted)", res, err)
	}
	if res.Probe {
		t.Error("a budget refusal must never be reported as a probe: nothing upstream has refused us")
	}

	got, err := store.Quota(ctx, quotaTestClock)
	if err != nil {
		t.Fatalf("Quota: %v", err)
	}
	if got.Used != 2 {
		t.Errorf("Used = %d, want 2", got.Used)
	}
	if got.ExhaustedAt != nil {
		t.Errorf("ExhaustedAt = %v, want nil: spending the local budget is not an upstream refusal", got.ExhaustedAt)
	}
	if !got.EpochStartedAt.Equal(quotaTestClock) {
		t.Errorf("EpochStartedAt = %v, want %v (the first spend starts the epoch)", got.EpochStartedAt, quotaTestClock)
	}
}

// TestUTCMidnightDoesNotResetTheCounter is the regression test for the
// second half of the quota-accounting bug. The counter used to be keyed
// by UTC date, which silently assumed OMDb's day starts at UTC midnight.
// It does not: with a rollover observed at 06:00, crossing our midnight
// would present an empty row and hand out a second full budget inside a
// single upstream quota day, walking straight into a real refusal.
func TestUTCMidnightDoesNotResetTheCounter(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	// An upstream rollover observed at 06:00 anchors the epoch there.
	rollover := time.Date(2026, 8, 30, 6, 0, 0, 0, time.UTC)
	if err := store.MarkRecovered(ctx, rollover); err != nil {
		t.Fatalf("MarkRecovered: %v", err)
	}

	// Spend the rest of the budget during the day.
	for i := 0; i < 3; i++ {
		if _, err := store.TryReserveQuota(ctx, 4, rollover.Add(time.Hour), testProbeInterval); err != nil {
			t.Fatalf("TryReserveQuota: %v", err)
		}
	}

	// Cross UTC midnight. OMDb's day has not rolled — the next one is
	// due at 06:00 — so the counter must not reset.
	pastMidnight := time.Date(2026, 8, 31, 0, 30, 0, 0, time.UTC)
	got, err := store.Quota(ctx, pastMidnight)
	if err != nil {
		t.Fatalf("Quota: %v", err)
	}
	if got.Used != 4 {
		t.Errorf("Used = %d after UTC midnight, want 4: the calendar is not the accounting boundary", got.Used)
	}
	if !got.EpochStartedAt.Equal(rollover) {
		t.Errorf("EpochStartedAt = %v, want %v (unchanged across UTC midnight)", got.EpochStartedAt, rollover)
	}

	res, err := store.TryReserveQuota(ctx, 4, pastMidnight, testProbeInterval)
	if err != nil {
		t.Fatalf("TryReserveQuota: %v", err)
	}
	if res.Granted {
		t.Error("granted = true after UTC midnight, want false: that would be a second full budget inside one upstream day")
	}
}

// TestEpochRollsForwardPreservingItsHour covers the fallback that keeps
// a proxy which never exhausts its budget from stopping forever, and
// the phase it keeps: an epoch anchored to OMDb's observed 06:00
// rollover must roll again at 06:00, not at whenever traffic resumed.
func TestEpochRollsForwardPreservingItsHour(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	rollover := time.Date(2026, 8, 30, 6, 0, 0, 0, time.UTC)
	if err := store.MarkRecovered(ctx, rollover); err != nil {
		t.Fatalf("MarkRecovered: %v", err)
	}

	justBefore := rollover.Add(24*time.Hour - time.Minute)
	got, err := store.Quota(ctx, justBefore)
	if err != nil {
		t.Fatalf("Quota: %v", err)
	}
	if got.Used != 1 || !got.EpochStartedAt.Equal(rollover) {
		t.Fatalf("Quota just before the epoch ends = %+v, want used=1 and the original epoch", got)
	}

	// A full epoch on, the counter starts over at the same hour.
	next := rollover.Add(24 * time.Hour)
	got, err = store.Quota(ctx, next.Add(time.Minute))
	if err != nil {
		t.Fatalf("Quota: %v", err)
	}
	if got.Used != 0 {
		t.Errorf("Used = %d, want 0 once a full epoch has elapsed", got.Used)
	}
	if !got.EpochStartedAt.Equal(next) {
		t.Errorf("EpochStartedAt = %v, want %v", got.EpochStartedAt, next)
	}

	// Idle for days: the epoch advances in whole steps, so it still
	// lands on 06:00 rather than re-anchoring to now.
	muchLater := rollover.Add(72*time.Hour + 3*time.Hour)
	got, err = store.Quota(ctx, muchLater)
	if err != nil {
		t.Fatalf("Quota: %v", err)
	}
	want := rollover.Add(72 * time.Hour)
	if !got.EpochStartedAt.Equal(want) {
		t.Errorf("EpochStartedAt = %v, want %v (whole-epoch steps preserve the observed hour)", got.EpochStartedAt, want)
	}
}

// TestEpochRolloverKeepsTheBreakerArmed: the rollover is our own
// estimate of when the next upstream day starts, while the breaker
// records something upstream actually said. Clearing it on the estimate
// would resume full-rate traffic on a guess.
func TestEpochRolloverKeepsTheBreakerArmed(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	if _, err := store.TryReserveQuota(ctx, 900, quotaTestClock, testProbeInterval); err != nil {
		t.Fatalf("TryReserveQuota: %v", err)
	}
	if err := store.MarkExhausted(ctx, quotaTestClock); err != nil {
		t.Fatalf("MarkExhausted: %v", err)
	}

	afterEpoch := quotaTestClock.Add(25 * time.Hour)
	got, err := store.Quota(ctx, afterEpoch)
	if err != nil {
		t.Fatalf("Quota: %v", err)
	}
	if got.Used != 0 {
		t.Errorf("Used = %d, want 0 after the epoch rolled", got.Used)
	}
	if got.ExhaustedAt == nil {
		t.Error("breaker cleared by the epoch rollover, want it left armed for a probe to settle")
	}
}

// TestMarkExhaustedArmsBreakerWithoutTouchingUsed pins the split that
// makes a forfeited period diagnosable. The old design tripped the
// breaker by forcing used up to the budget, which left a period the
// proxy never got to use looking byte-identical to one it spent.
func TestMarkExhaustedArmsBreakerWithoutTouchingUsed(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if _, err := store.TryReserveQuota(ctx, 900, quotaTestClock, testProbeInterval); err != nil {
			t.Fatalf("TryReserveQuota: %v", err)
		}
	}

	if err := store.MarkExhausted(ctx, quotaTestClock); err != nil {
		t.Fatalf("MarkExhausted: %v", err)
	}

	got, err := store.Quota(ctx, quotaTestClock)
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
	res, err := store.TryReserveQuota(ctx, 900, quotaTestClock.Add(time.Minute), testProbeInterval)
	if err != nil {
		t.Fatalf("TryReserveQuota: %v", err)
	}
	if res.Granted {
		t.Error("granted = true, want false while the breaker is freshly armed")
	}
}

// TestBreakerGrantsOneProbePerInterval is the core of the recovery
// path: once the interval lapses exactly one caller gets through, and
// the grant re-arms the timer so a burst of misses cannot become a
// burst of doomed upstream calls.
func TestBreakerGrantsOneProbePerInterval(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	if err := store.MarkExhausted(ctx, quotaTestClock); err != nil {
		t.Fatalf("MarkExhausted: %v", err)
	}

	justBefore := quotaTestClock.Add(testProbeInterval - time.Second)
	res, err := store.TryReserveQuota(ctx, 900, justBefore, testProbeInterval)
	if err != nil {
		t.Fatalf("TryReserveQuota: %v", err)
	}
	if res.Granted {
		t.Fatal("granted = true one second before the interval lapsed, want false")
	}

	lapsed := quotaTestClock.Add(testProbeInterval)
	res, err = store.TryReserveQuota(ctx, 900, lapsed, testProbeInterval)
	if err != nil {
		t.Fatalf("TryReserveQuota: %v", err)
	}
	if !res.Granted || !res.Probe {
		t.Fatalf("reservation = %+v, want a granted probe once the interval lapsed", res)
	}

	// The second caller in the same instant must be refused: the probe
	// is a single request, not an open door.
	res, err = store.TryReserveQuota(ctx, 900, lapsed, testProbeInterval)
	if err != nil {
		t.Fatalf("TryReserveQuota: %v", err)
	}
	if res.Granted {
		t.Error("granted = true for a second caller in the same interval, want false (only one probe at a time)")
	}

	// ...and the next probe is due an interval after the probe, not
	// after the original arming.
	res, err = store.TryReserveQuota(ctx, 900, lapsed.Add(testProbeInterval), testProbeInterval)
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

	if _, err := store.TryReserveQuota(ctx, 1, quotaTestClock, testProbeInterval); err != nil {
		t.Fatalf("TryReserveQuota: %v", err)
	}
	if err := store.MarkExhausted(ctx, quotaTestClock); err != nil {
		t.Fatalf("MarkExhausted: %v", err)
	}

	res, err := store.TryReserveQuota(ctx, 1, quotaTestClock.Add(testProbeInterval), testProbeInterval)
	if err != nil {
		t.Fatalf("TryReserveQuota: %v", err)
	}
	if !res.Granted || !res.Probe {
		t.Fatalf("reservation = %+v, want a granted probe even with the budget spent", res)
	}
}

// TestMarkRecoveredStartsANewEpoch pins the behaviour that keeps the
// proxy's accounting aligned to OMDb's day: a probe that comes back
// normal is the only observable start of an upstream quota day, so it
// becomes the epoch.
func TestMarkRecoveredStartsANewEpoch(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		if _, err := store.TryReserveQuota(ctx, 900, quotaTestClock, testProbeInterval); err != nil {
			t.Fatalf("TryReserveQuota: %v", err)
		}
	}
	if err := store.MarkExhausted(ctx, quotaTestClock); err != nil {
		t.Fatalf("MarkExhausted: %v", err)
	}

	recoveredAt := quotaTestClock.Add(90 * time.Minute)
	if err := store.MarkRecovered(ctx, recoveredAt); err != nil {
		t.Fatalf("MarkRecovered: %v", err)
	}

	got, err := store.Quota(ctx, recoveredAt)
	if err != nil {
		t.Fatalf("Quota: %v", err)
	}
	if got.Used != 1 {
		t.Errorf("Used = %d, want 1: a recovered probe starts a fresh epoch, having spent one request itself", got.Used)
	}
	if got.ExhaustedAt != nil {
		t.Errorf("ExhaustedAt = %v, want nil after recovery", got.ExhaustedAt)
	}
	if !got.EpochStartedAt.Equal(recoveredAt) {
		t.Errorf("EpochStartedAt = %v, want %v (the recovery is the observed rollover)", got.EpochStartedAt, recoveredAt)
	}
	if !got.ResetsAt().Equal(recoveredAt.Add(24 * time.Hour)) {
		t.Errorf("ResetsAt = %v, want one epoch after the recovery", got.ResetsAt())
	}

	res, err := store.TryReserveQuota(ctx, 900, recoveredAt.Add(time.Minute), testProbeInterval)
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
		t.Errorf("ExhaustedAt = %v, want %v (an older mark must not wind the breaker back)", got.ExhaustedAt, quotaTestClock)
	}

	// Idempotent: repeating the same mark changes nothing.
	if err := store.MarkExhausted(ctx, quotaTestClock); err != nil {
		t.Fatalf("MarkExhausted (repeat): %v", err)
	}
	got, err = store.Quota(ctx, quotaTestClock)
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
	if !got.EpochStartedAt.Equal(quotaTestClock) {
		t.Errorf("EpochStartedAt = %v, want now (%v): the epoch starts with the first request", got.EpochStartedAt, quotaTestClock)
	}
}

// TestOpenDropsTheLegacyDayKeyedQuotaTable covers upgrading a deployed
// database. The old rows counted spend within a UTC calendar day, which
// is not what the epoch ledger measures, and rows written before the
// breaker was separated from the counter hold a forged used value —
// carrying either forward would start the proxy on a number it should
// not trust.
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
		t.Fatalf("Open on a pre-epoch database: %v", err)
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
