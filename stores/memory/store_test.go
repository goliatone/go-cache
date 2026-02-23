package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	gocache "github.com/goliatone/go-cache"
	"github.com/goliatone/go-cache/internal/conformancetest"
)

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
	if err := store.Set(context.Background(), 1, "v", time.Minute); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	if err := store.AddTagsForKey(context.Background(), 1, []string{"group"}); err != nil {
		t.Fatalf("add tags failed: %v", err)
	}
	if err := store.InvalidateTags(context.Background(), []string{"group"}); err != nil {
		t.Fatalf("invalidate tags failed: %v", err)
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
