package memory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gocache "github.com/goliatone/go-cache"
	"github.com/goliatone/go-cache/internal/conformancetest"
)

var _ gocache.SetIfPresentCache[string, string] = (*Store[string, string])(nil)

func TestMemoryStoreStringConformance(t *testing.T) {
	factory := conformancetest.Factory[string, string]{
		Name: "memory",
		New: func(t *testing.T, options conformancetest.Options[string, string]) gocache.Cache[string, string] {
			t.Helper()
			opts := make([]Option[string, string], 0, 3)
			if options.Codec != nil {
				opts = append(opts, WithCodec[string, string](options.Codec))
			}
			if options.KeyEncoder != nil {
				opts = append(opts, WithKeyEncoder[string, string](options.KeyEncoder))
			}
			if options.Observer != nil {
				opts = append(opts, WithObserver[string, string](options.Observer))
			}
			store, err := NewStore[string, string](opts...)
			if err != nil {
				t.Fatalf("new store failed: %v", err)
			}
			return store
		},
	}

	conformancetest.RunStringCacheContractTests(t, factory)
	conformancetest.RunStringLogicalCapabilityTests(t, factory)
	conformancetest.RunSetIfPresentEncodingFailureTests(t, factory)
}

func TestMemoryStoreIntKeyEncodingConformance(t *testing.T) {
	factory := conformancetest.Factory[int, string]{
		Name: "memory",
		New: func(t *testing.T, options conformancetest.Options[int, string]) gocache.Cache[int, string] {
			t.Helper()
			opts := make([]Option[int, string], 0, 3)
			if options.Codec != nil {
				opts = append(opts, WithCodec[int, string](options.Codec))
			}
			if options.KeyEncoder != nil {
				opts = append(opts, WithKeyEncoder[int, string](options.KeyEncoder))
			}
			if options.Observer != nil {
				opts = append(opts, WithObserver[int, string](options.Observer))
			}
			store, err := NewStore[int, string](opts...)
			if err != nil {
				t.Fatalf("new store failed: %v", err)
			}
			return store
		},
	}

	conformancetest.RunIntKeyEncodingContractTests(t, factory)
}

func TestMemoryStoreObserverEvents(t *testing.T) {
	events := make([]gocache.Observation, 0, 4)
	observer := gocache.ObserverFunc(func(_ context.Context, event gocache.Observation) {
		events = append(events, event)
	})

	store, err := NewStore[string, string](WithObserver[string, string](observer))
	if err != nil {
		t.Fatalf("new store failed: %v", err)
	}

	if err := store.Set(context.Background(), "k1", "v1", time.Second); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	if _, _, err := store.Get(context.Background(), "k1"); err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if err := store.Delete(context.Background(), "k1"); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if len(events) < 3 {
		t.Fatalf("expected at least 3 events, got %d", len(events))
	}
	if events[0].Backend != "memory" {
		t.Fatalf("expected memory backend, got %q", events[0].Backend)
	}
}

func TestMemoryStoreTagsWithEncodedNonStringKeys(t *testing.T) {
	encoder := gocache.KeyEncoderFunc[int](func(key int) (string, error) {
		return "id:" + string(rune('a'+key)), nil
	})
	store, err := NewStore[int, string](WithKeyEncoder[int, string](encoder))
	if err != nil {
		t.Fatalf("new store failed: %v", err)
	}
	if setErr := store.Set(context.Background(), 1, "v", time.Minute); setErr != nil {
		t.Fatalf("set failed: %v", setErr)
	}
	if addTagsErr := store.AddTagsForKey(context.Background(), 1, []string{"group"}); addTagsErr != nil {
		t.Fatalf("add tags failed: %v", addTagsErr)
	}
	if invalidateErr := store.InvalidateTags(context.Background(), []string{"group"}); invalidateErr != nil {
		t.Fatalf("invalidate tags failed: %v", invalidateErr)
	}
	value, hit, err := store.Get(context.Background(), 1)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if hit || value != "" {
		t.Fatalf("expected miss after tag invalidation, hit=%v value=%q", hit, value)
	}
}

func TestMemoryStoreRequiresEncoderForNonStringKeys(t *testing.T) {
	_, err := NewStore[int, string]()
	if !errors.Is(err, gocache.ErrKeyEncoderRequired) {
		t.Fatalf("expected ErrKeyEncoderRequired, got %v", err)
	}
}

func TestMemoryStoreMaxEntriesRequiresPositiveLimit(t *testing.T) {
	for _, limit := range []int{0, -1} {
		_, err := NewStore[string, string](WithMaxEntries[string, string](limit))
		if !errors.Is(err, errInvalidMaxEntries) {
			t.Fatalf("limit %d: expected errInvalidMaxEntries, got %v", limit, err)
		}
	}
}

func TestMemoryStoreMaxEntriesPurgesExpiredBeforeEviction(t *testing.T) {
	now := time.Date(2026, time.August, 11, 12, 0, 0, 0, time.UTC)
	events := make([]gocache.Observation, 0, 8)
	store, err := NewStore[string, string](
		WithMaxEntries[string, string](3),
		WithClock[string, string](func() time.Time { return now }),
		WithObserver[string, string](gocache.ObserverFunc(func(_ context.Context, event gocache.Observation) {
			events = append(events, event)
		})),
	)
	if err != nil {
		t.Fatalf("new store failed: %v", err)
	}

	if err := store.Set(context.Background(), "expired", "old", time.Second); err != nil {
		t.Fatalf("set expired candidate failed: %v", err)
	}
	if err := store.Set(context.Background(), "live", "keep", time.Minute); err != nil {
		t.Fatalf("set live failed: %v", err)
	}
	now = now.Add(2 * time.Second)
	if err := store.Set(context.Background(), "new", "value", time.Minute); err != nil {
		t.Fatalf("set new failed: %v", err)
	}
	store.mu.RLock()
	_, expiredStillStored := store.entries["expired"]
	occupancy := len(store.entries)
	store.mu.RUnlock()
	if expiredStillStored || occupancy != 2 {
		t.Fatalf("admission did not purge expired entries: stored=%v occupancy=%d", expiredStillStored, occupancy)
	}

	for _, key := range []string{"live", "new"} {
		if _, hit, getErr := store.Get(context.Background(), key); getErr != nil || !hit {
			t.Fatalf("expected %q hit, hit=%v err=%v", key, hit, getErr)
		}
	}
	if _, hit, getErr := store.Get(context.Background(), "expired"); getErr != nil || hit {
		t.Fatalf("expected expired miss, hit=%v err=%v", hit, getErr)
	}
	for _, event := range events {
		if event.Operation == gocache.OperationEvict {
			t.Fatalf("expired-first admission must not evict a live entry: %+v", event)
		}
	}
}

func TestMemoryStoreMaxEntriesEvictsLeastRecentlyUsed(t *testing.T) {
	events := make([]gocache.Observation, 0, 8)
	store, err := NewStore[string, string](
		WithMaxEntries[string, string](2),
		WithObserver[string, string](gocache.ObserverFunc(func(_ context.Context, event gocache.Observation) {
			events = append(events, event)
		})),
	)
	if err != nil {
		t.Fatalf("new store failed: %v", err)
	}
	for _, key := range []string{"a", "b"} {
		if err := store.Set(context.Background(), key, key, time.Minute); err != nil {
			t.Fatalf("set %q failed: %v", key, err)
		}
	}
	if _, hit, getErr := store.Get(context.Background(), "a"); getErr != nil || !hit {
		t.Fatalf("refresh a failed: hit=%v err=%v", hit, getErr)
	}
	if err := store.Set(context.Background(), "c", "c", time.Minute); err != nil {
		t.Fatalf("set c failed: %v", err)
	}

	if _, hit, getErr := store.Get(context.Background(), "b"); getErr != nil || hit {
		t.Fatalf("expected least-recently-used b miss, hit=%v err=%v", hit, getErr)
	}
	for _, key := range []string{"a", "c"} {
		if _, hit, getErr := store.Get(context.Background(), key); getErr != nil || !hit {
			t.Fatalf("expected %q hit, hit=%v err=%v", key, hit, getErr)
		}
	}

	var eviction *gocache.Observation
	for i := range events {
		if events[i].Operation == gocache.OperationEvict {
			eviction = &events[i]
		}
	}
	if eviction == nil {
		t.Fatal("expected eviction observation")
	}
	if eviction.Key != "" || eviction.Count != 1 || eviction.Occupancy != 2 || eviction.Capacity != 2 {
		t.Fatalf("unsafe or incomplete eviction observation: %+v", *eviction)
	}
}

func TestMemoryStoreMaxEntriesConcurrentChurnNeverExceedsLimit(t *testing.T) {
	const limit = 17
	var maxObserved atomic.Int64
	var unsafeEviction atomic.Bool
	observer := gocache.ObserverFunc(func(_ context.Context, event gocache.Observation) {
		for current := maxObserved.Load(); int64(event.Occupancy) > current; current = maxObserved.Load() {
			if maxObserved.CompareAndSwap(current, int64(event.Occupancy)) {
				break
			}
		}
		if event.Operation == gocache.OperationEvict && event.Key != "" {
			unsafeEviction.Store(true)
		}
	})
	store, err := NewStore[string, string](
		WithMaxEntries[string, string](limit),
		WithObserver[string, string](observer),
	)
	if err != nil {
		t.Fatalf("new store failed: %v", err)
	}

	var workers sync.WaitGroup
	for worker := 0; worker < 24; worker++ {
		worker := worker
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := 0; i < 100; i++ {
				key := fmt.Sprintf("worker-%d-key-%d", worker, i)
				if setErr := store.Set(context.Background(), key, key, time.Minute); setErr != nil {
					t.Errorf("set %q failed: %v", key, setErr)
					return
				}
				if _, _, getErr := store.Get(context.Background(), key); getErr != nil {
					t.Errorf("get %q failed: %v", key, getErr)
					return
				}
			}
		}()
	}
	workers.Wait()

	store.mu.RLock()
	occupancy := len(store.entries)
	recencyEntries := len(store.recencyByStorage)
	store.mu.RUnlock()
	if occupancy > limit || recencyEntries > limit {
		t.Fatalf("hard limit exceeded: entries=%d recency=%d limit=%d", occupancy, recencyEntries, limit)
	}
	if observed := maxObserved.Load(); observed > limit {
		t.Fatalf("observed occupancy exceeded limit: %d > %d", observed, limit)
	}
	if unsafeEviction.Load() {
		t.Fatal("eviction observation exposed a key")
	}
}
