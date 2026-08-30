package cache

import (
	"context"
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

func TestTryReserveQuotaEnforcesBudget(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	const day = "2026-08-30"

	ok, used, err := store.TryReserveQuota(ctx, day, 2)
	if err != nil || !ok || used != 1 {
		t.Fatalf("1st reservation: ok=%v used=%d err=%v, want ok=true used=1", ok, used, err)
	}

	ok, used, err = store.TryReserveQuota(ctx, day, 2)
	if err != nil || !ok || used != 2 {
		t.Fatalf("2nd reservation: ok=%v used=%d err=%v, want ok=true used=2", ok, used, err)
	}

	ok, used, err = store.TryReserveQuota(ctx, day, 2)
	if err != nil || ok {
		t.Fatalf("3rd reservation: ok=%v used=%d err=%v, want ok=false (budget exhausted)", ok, used, err)
	}

	got, err := store.QuotaUsed(ctx, day)
	if err != nil {
		t.Fatalf("QuotaUsed: %v", err)
	}
	if got != 2 {
		t.Errorf("QuotaUsed = %d, want 2", got)
	}
}

func TestMarkExhaustedForcesUsedToAtLeastBudget(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	const day = "2026-08-30"

	if err := store.MarkExhausted(ctx, day, 900); err != nil {
		t.Fatalf("MarkExhausted: %v", err)
	}

	got, err := store.QuotaUsed(ctx, day)
	if err != nil {
		t.Fatalf("QuotaUsed: %v", err)
	}
	if got != 900 {
		t.Errorf("QuotaUsed = %d, want 900", got)
	}

	// TryReserveQuota must now refuse for the rest of the day.
	reserved, used, err := store.TryReserveQuota(ctx, day, 900)
	if err != nil {
		t.Fatalf("TryReserveQuota: %v", err)
	}
	if reserved {
		t.Error("reserved = true, want false (budget marked exhausted)")
	}
	if used != 900 {
		t.Errorf("used = %d, want 900", used)
	}

	// A different day must be entirely unaffected — this is the whole
	// reset story: the marker is keyed by UTC day like the rest of the
	// quota accounting, so it clears itself at UTC midnight along with
	// everything else, rather than needing any explicit cleanup.
	otherDayUsed, err := store.QuotaUsed(ctx, "2026-08-31")
	if err != nil {
		t.Fatalf("QuotaUsed(other day): %v", err)
	}
	if otherDayUsed != 0 {
		t.Errorf("QuotaUsed(other day) = %d, want 0 (marker must not leak across days)", otherDayUsed)
	}
}

func TestMarkExhaustedIsIdempotentAndNeverLowersCounter(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	const day = "2026-08-30"

	// Push the counter above a hypothetical "budget" first, as
	// concurrent traffic reserving quota might.
	for i := 0; i < 5; i++ {
		if _, _, err := store.TryReserveQuota(ctx, day, 1000); err != nil {
			t.Fatalf("TryReserveQuota: %v", err)
		}
	}
	got, err := store.QuotaUsed(ctx, day)
	if err != nil {
		t.Fatalf("QuotaUsed: %v", err)
	}
	if got != 5 {
		t.Fatalf("QuotaUsed = %d, want 5 before MarkExhausted", got)
	}

	// A MarkExhausted call with a budget lower than the current counter
	// must not claw the counter back down — the upsert only ever
	// raises it.
	if err := store.MarkExhausted(ctx, day, 3); err != nil {
		t.Fatalf("MarkExhausted: %v", err)
	}
	got, err = store.QuotaUsed(ctx, day)
	if err != nil {
		t.Fatalf("QuotaUsed: %v", err)
	}
	if got != 5 {
		t.Errorf("QuotaUsed = %d, want 5 (MarkExhausted must never lower an already-higher counter)", got)
	}

	// Calling it repeatedly with the same budget is idempotent.
	if err := store.MarkExhausted(ctx, day, 900); err != nil {
		t.Fatalf("MarkExhausted: %v", err)
	}
	if err := store.MarkExhausted(ctx, day, 900); err != nil {
		t.Fatalf("MarkExhausted (again): %v", err)
	}
	got, err = store.QuotaUsed(ctx, day)
	if err != nil {
		t.Fatalf("QuotaUsed: %v", err)
	}
	if got != 900 {
		t.Errorf("QuotaUsed = %d, want 900", got)
	}
}

func TestQuotaUsedForUnknownDayIsZero(t *testing.T) {
	store := openTestStore(t)
	used, err := store.QuotaUsed(context.Background(), "1999-01-01")
	if err != nil {
		t.Fatalf("QuotaUsed: %v", err)
	}
	if used != 0 {
		t.Errorf("QuotaUsed = %d, want 0", used)
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

	stats, err := store.Stats(ctx)
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
