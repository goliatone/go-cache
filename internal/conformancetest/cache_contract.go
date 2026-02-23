package conformancetest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	gocache "github.com/goliatone/go-cache"
	"github.com/goliatone/go-cache/codec"
)

// Options are shared backend-constructor options for conformance tests.
type Options[K comparable, V any] struct {
	Codec      codec.Codec[V]
	KeyEncoder gocache.KeyEncoder[K]
	Observer   gocache.Observer
}

// Factory creates cache instances for conformance tests.
type Factory[K comparable, V any] struct {
	Name string
	New  func(t *testing.T, options Options[K, V]) gocache.Cache[K, V]
}

// RunStringCacheContractTests runs reusable contract checks for string-key backends.
func RunStringCacheContractTests(t *testing.T, factory Factory[string, string]) {
	t.Helper()

	t.Run(factory.Name+"/miss-semantics", func(t *testing.T) {
		cache := factory.New(t, Options[string, string]{})
		value, ok, err := cache.Get(context.Background(), "missing")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ok {
			t.Fatal("expected cache miss")
		}
		if value != "" {
			t.Fatalf("expected zero value, got %q", value)
		}
	})

	t.Run(factory.Name+"/set-get-delete", func(t *testing.T) {
		cache := factory.New(t, Options[string, string]{})
		if err := cache.Set(context.Background(), "k1", "v1", time.Minute); err != nil {
			t.Fatalf("set failed: %v", err)
		}
		value, ok, err := cache.Get(context.Background(), "k1")
		if err != nil {
			t.Fatalf("get failed: %v", err)
		}
		if !ok || value != "v1" {
			t.Fatalf("unexpected value: ok=%v value=%q", ok, value)
		}
		if err := cache.Delete(context.Background(), "k1"); err != nil {
			t.Fatalf("delete failed: %v", err)
		}
		value, ok, err = cache.Get(context.Background(), "k1")
		if err != nil {
			t.Fatalf("get after delete failed: %v", err)
		}
		if ok || value != "" {
			t.Fatalf("expected miss after delete: ok=%v value=%q", ok, value)
		}
	})

	t.Run(factory.Name+"/ttl-expiry-and-non-expiring", func(t *testing.T) {
		cache := factory.New(t, Options[string, string]{})
		if err := cache.Set(context.Background(), "exp", "v", 30*time.Millisecond); err != nil {
			t.Fatalf("set expiring failed: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
		value, ok, err := cache.Get(context.Background(), "exp")
		if err != nil {
			t.Fatalf("get expiring failed: %v", err)
		}
		if ok || value != "" {
			t.Fatalf("expected expired miss: ok=%v value=%q", ok, value)
		}

		if err := cache.Set(context.Background(), "persist0", "v0", 0); err != nil {
			t.Fatalf("set ttl=0 failed: %v", err)
		}
		if err := cache.Set(context.Background(), "persistNeg", "v1", -time.Second); err != nil {
			t.Fatalf("set ttl<0 failed: %v", err)
		}
		time.Sleep(30 * time.Millisecond)
		value, ok, err = cache.Get(context.Background(), "persist0")
		if err != nil || !ok || value != "v0" {
			t.Fatalf("expected ttl=0 hit, got ok=%v value=%q err=%v", ok, value, err)
		}
		value, ok, err = cache.Get(context.Background(), "persistNeg")
		if err != nil || !ok || value != "v1" {
			t.Fatalf("expected ttl<0 hit, got ok=%v value=%q err=%v", ok, value, err)
		}
	})

	t.Run(factory.Name+"/concurrent-set-get", func(t *testing.T) {
		cache := factory.New(t, Options[string, string]{})
		var wg sync.WaitGroup
		for i := 0; i < 64; i++ {
			i := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				key := fmt.Sprintf("k%d", i%8)
				value := fmt.Sprintf("v%d", i)
				if err := cache.Set(context.Background(), key, value, time.Second); err != nil {
					t.Errorf("set failed: %v", err)
					return
				}
				_, _, err := cache.Get(context.Background(), key)
				if err != nil {
					t.Errorf("get failed: %v", err)
				}
			}()
		}
		wg.Wait()
	})

	t.Run(factory.Name+"/decode-failure", func(t *testing.T) {
		cache := factory.New(t, Options[string, string]{
			Codec: decodeFailingCodec[string]{base: codec.NewJSONCodec[string]()},
		})
		if err := cache.Set(context.Background(), "broken", "value", time.Second); err != nil {
			t.Fatalf("set failed: %v", err)
		}
		value, ok, err := cache.Get(context.Background(), "broken")
		if err == nil {
			t.Fatal("expected decode error")
		}
		if ok {
			t.Fatal("expected miss on decode error")
		}
		if value != "" {
			t.Fatalf("expected zero value on decode error, got %q", value)
		}
	})

	t.Run(factory.Name+"/context-cancellation", func(t *testing.T) {
		cache := factory.New(t, Options[string, string]{})
		canceledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		if err := cache.Set(canceledCtx, "k", "v", time.Second); !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled from Set, got %v", err)
		}
		value, ok, err := cache.Get(canceledCtx, "k")
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled from Get, got %v", err)
		}
		if ok || value != "" {
			t.Fatalf("expected zero miss on canceled get, got ok=%v value=%q", ok, value)
		}
		if err := cache.Delete(canceledCtx, "k"); !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled from Delete, got %v", err)
		}
	})

	t.Run(factory.Name+"/optional-capabilities", func(t *testing.T) {
		cache := factory.New(t, Options[string, string]{})
		ctx := context.Background()

		if clearable, ok := cache.(gocache.Clearable); ok {
			if err := cache.Set(ctx, "a", "1", time.Minute); err != nil {
				t.Fatalf("set failed: %v", err)
			}
			if err := clearable.Clear(ctx); err != nil {
				t.Fatalf("clear failed: %v", err)
			}
			_, hit, err := cache.Get(ctx, "a")
			if err != nil || hit {
				t.Fatalf("expected clear miss, hit=%v err=%v", hit, err)
			}
		}

		if purgeable, ok := cache.(gocache.Purgeable); ok {
			if err := cache.Set(ctx, "purge", "1", 20*time.Millisecond); err != nil {
				t.Fatalf("set failed: %v", err)
			}
			time.Sleep(35 * time.Millisecond)
			removed, err := purgeable.PurgeExpired(ctx)
			if err != nil {
				t.Fatalf("purge failed: %v", err)
			}
			if removed < 1 {
				t.Fatalf("expected at least one removed entry, got %d", removed)
			}
		}

		if setIfAbsent, ok := cache.(gocache.SetIfAbsentCache[string, string]); ok {
			set, err := setIfAbsent.SetIfAbsent(ctx, "absent", "a", time.Minute)
			if err != nil {
				t.Fatalf("set-if-absent first call failed: %v", err)
			}
			if !set {
				t.Fatal("expected first set-if-absent to store value")
			}
			set, err = setIfAbsent.SetIfAbsent(ctx, "absent", "b", time.Minute)
			if err != nil {
				t.Fatalf("set-if-absent second call failed: %v", err)
			}
			if set {
				t.Fatal("expected second set-if-absent to skip value")
			}
		}

		if batchInvalidator, ok := cache.(gocache.BatchInvalidator[string]); ok {
			if err := cache.Set(ctx, "b1", "x", time.Minute); err != nil {
				t.Fatalf("set b1 failed: %v", err)
			}
			if err := cache.Set(ctx, "b2", "y", time.Minute); err != nil {
				t.Fatalf("set b2 failed: %v", err)
			}
			if err := batchInvalidator.InvalidateKeys(ctx, []string{"b1", "b2"}); err != nil {
				t.Fatalf("invalidate keys failed: %v", err)
			}
			_, hit, err := cache.Get(ctx, "b1")
			if err != nil || hit {
				t.Fatalf("expected b1 miss, hit=%v err=%v", hit, err)
			}
			_, hit, err = cache.Get(ctx, "b2")
			if err != nil || hit {
				t.Fatalf("expected b2 miss, hit=%v err=%v", hit, err)
			}
		}

		if prefixInvalidator, ok := cache.(gocache.TypedPrefixInvalidator[string]); ok {
			if err := cache.Set(ctx, "pre:1", "x", time.Minute); err != nil {
				t.Fatalf("set pre:1 failed: %v", err)
			}
			if err := cache.Set(ctx, "pre:2", "y", time.Minute); err != nil {
				t.Fatalf("set pre:2 failed: %v", err)
			}
			if err := cache.Set(ctx, "other:1", "z", time.Minute); err != nil {
				t.Fatalf("set other:1 failed: %v", err)
			}
			if err := prefixInvalidator.DeleteByKeyPrefix(ctx, "pre:"); err != nil {
				t.Fatalf("delete by prefix failed: %v", err)
			}
			_, hit, err := cache.Get(ctx, "pre:1")
			if err != nil || hit {
				t.Fatalf("expected pre:1 miss, hit=%v err=%v", hit, err)
			}
			value, hit, err := cache.Get(ctx, "other:1")
			if err != nil || !hit || value != "z" {
				t.Fatalf("expected other:1 hit, hit=%v value=%q err=%v", hit, value, err)
			}
		} else if prefixInvalidator, ok := cache.(gocache.PrefixInvalidator); ok {
			if err := cache.Set(ctx, "pre:1", "x", time.Minute); err != nil {
				t.Fatalf("set pre:1 failed: %v", err)
			}
			if err := cache.Set(ctx, "pre:2", "y", time.Minute); err != nil {
				t.Fatalf("set pre:2 failed: %v", err)
			}
			if err := cache.Set(ctx, "other:1", "z", time.Minute); err != nil {
				t.Fatalf("set other:1 failed: %v", err)
			}
			if err := prefixInvalidator.DeleteByPrefix(ctx, "pre:"); err != nil {
				t.Fatalf("delete by prefix failed: %v", err)
			}
			_, hit, err := cache.Get(ctx, "pre:1")
			if err != nil || hit {
				t.Fatalf("expected pre:1 miss, hit=%v err=%v", hit, err)
			}
			value, hit, err := cache.Get(ctx, "other:1")
			if err != nil || !hit || value != "z" {
				t.Fatalf("expected other:1 hit, hit=%v value=%q err=%v", hit, value, err)
			}
		}

		if tagRegistry, ok := cache.(gocache.TypedTagRegistry[string]); ok {
			if err := cache.Set(ctx, "t1", "x", time.Minute); err != nil {
				t.Fatalf("set t1 failed: %v", err)
			}
			if err := cache.Set(ctx, "t2", "y", time.Minute); err != nil {
				t.Fatalf("set t2 failed: %v", err)
			}
			if err := cache.Set(ctx, "t3", "z", time.Minute); err != nil {
				t.Fatalf("set t3 failed: %v", err)
			}
			if err := tagRegistry.AddTagsForKey(ctx, "t1", []string{"grpA"}); err != nil {
				t.Fatalf("add tags t1 failed: %v", err)
			}
			if err := tagRegistry.AddTagsForKey(ctx, "t2", []string{"grpA", "grpB"}); err != nil {
				t.Fatalf("add tags t2 failed: %v", err)
			}
			if err := tagRegistry.InvalidateTags(ctx, []string{"grpA"}); err != nil {
				t.Fatalf("invalidate tags failed: %v", err)
			}
			_, hit, err := cache.Get(ctx, "t1")
			if err != nil || hit {
				t.Fatalf("expected t1 miss, hit=%v err=%v", hit, err)
			}
			_, hit, err = cache.Get(ctx, "t2")
			if err != nil || hit {
				t.Fatalf("expected t2 miss, hit=%v err=%v", hit, err)
			}
			value, hit, err := cache.Get(ctx, "t3")
			if err != nil || !hit || value != "z" {
				t.Fatalf("expected t3 hit, hit=%v value=%q err=%v", hit, value, err)
			}
		} else if tagRegistry, ok := cache.(gocache.TagRegistry); ok {
			if err := cache.Set(ctx, "t1", "x", time.Minute); err != nil {
				t.Fatalf("set t1 failed: %v", err)
			}
			if err := cache.Set(ctx, "t2", "y", time.Minute); err != nil {
				t.Fatalf("set t2 failed: %v", err)
			}
			if err := cache.Set(ctx, "t3", "z", time.Minute); err != nil {
				t.Fatalf("set t3 failed: %v", err)
			}
			if err := tagRegistry.AddTags(ctx, "t1", []string{"grpA"}); err != nil {
				t.Fatalf("add tags t1 failed: %v", err)
			}
			if err := tagRegistry.AddTags(ctx, "t2", []string{"grpA", "grpB"}); err != nil {
				t.Fatalf("add tags t2 failed: %v", err)
			}
			if err := tagRegistry.InvalidateTags(ctx, []string{"grpA"}); err != nil {
				t.Fatalf("invalidate tags failed: %v", err)
			}
			_, hit, err := cache.Get(ctx, "t1")
			if err != nil || hit {
				t.Fatalf("expected t1 miss, hit=%v err=%v", hit, err)
			}
			_, hit, err = cache.Get(ctx, "t2")
			if err != nil || hit {
				t.Fatalf("expected t2 miss, hit=%v err=%v", hit, err)
			}
			value, hit, err := cache.Get(ctx, "t3")
			if err != nil || !hit || value != "z" {
				t.Fatalf("expected t3 hit, hit=%v value=%q err=%v", hit, value, err)
			}
		}
	})
}

// RunIntKeyEncodingContractTests validates that non-string keys work through deterministic encoders.
func RunIntKeyEncodingContractTests(t *testing.T, factory Factory[int, string]) {
	t.Helper()

	t.Run(factory.Name+"/int-key-encoding-fixtures", func(t *testing.T) {
		encoder := gocache.KeyEncoderFunc[int](func(k int) (string, error) {
			if k < 0 {
				return "", errors.New("negative key")
			}
			return fmt.Sprintf("fixture/%03d", k), nil
		})

		cache := factory.New(t, Options[int, string]{
			KeyEncoder: encoder,
		})
		if keyEncoder, ok := cache.(interface {
			EncodeKey(int) (string, error)
		}); ok {
			got, err := keyEncoder.EncodeKey(41)
			if err != nil {
				t.Fatalf("encode key 41 failed: %v", err)
			}
			if got != "fixture/041" {
				t.Fatalf("unexpected encoded key for 41: %q", got)
			}
			got, err = keyEncoder.EncodeKey(42)
			if err != nil {
				t.Fatalf("encode key 42 failed: %v", err)
			}
			if got != "fixture/042" {
				t.Fatalf("unexpected encoded key for 42: %q", got)
			}
		}

		if err := cache.Set(context.Background(), 41, "a", time.Minute); err != nil {
			t.Fatalf("set key 41 failed: %v", err)
		}
		if err := cache.Set(context.Background(), 42, "b", time.Minute); err != nil {
			t.Fatalf("set key 42 failed: %v", err)
		}
		value, hit, err := cache.Get(context.Background(), 41)
		if err != nil || !hit || value != "a" {
			t.Fatalf("expected key 41 hit, hit=%v value=%q err=%v", hit, value, err)
		}
		value, hit, err = cache.Get(context.Background(), 42)
		if err != nil || !hit || value != "b" {
			t.Fatalf("expected key 42 hit, hit=%v value=%q err=%v", hit, value, err)
		}

		if err := cache.Set(context.Background(), -1, "bad", time.Minute); err == nil {
			t.Fatal("expected key encoder error for negative key")
		}

		if prefixInvalidator, ok := cache.(gocache.TypedPrefixInvalidator[int]); ok {
			if err := prefixInvalidator.DeleteByKeyPrefix(context.Background(), 41); !errors.Is(err, gocache.ErrLogicalPrefixUnsupported) {
				t.Fatalf("expected ErrLogicalPrefixUnsupported for int prefix invalidation, got %v", err)
			}
		}

		if tagRegistry, ok := cache.(gocache.TypedTagRegistry[int]); ok {
			if err := tagRegistry.AddTagsForKey(context.Background(), 41, []string{"grpA"}); err != nil {
				t.Fatalf("add tags by int key failed: %v", err)
			}
			if err := tagRegistry.InvalidateTags(context.Background(), []string{"grpA"}); err != nil {
				t.Fatalf("invalidate tags failed: %v", err)
			}
			_, hit, err := cache.Get(context.Background(), 41)
			if err != nil || hit {
				t.Fatalf("expected key 41 miss after tag invalidation, hit=%v err=%v", hit, err)
			}
			value, hit, err := cache.Get(context.Background(), 42)
			if err != nil || !hit || value != "b" {
				t.Fatalf("expected key 42 hit after unrelated tag invalidation, hit=%v value=%q err=%v", hit, value, err)
			}
		}
	})
}

// RunStringLogicalCapabilityTests validates logical-key behavior when storage encoding is non-identity.
func RunStringLogicalCapabilityTests(t *testing.T, factory Factory[string, string]) {
	t.Helper()

	t.Run(factory.Name+"/logical-capabilities-custom-string-encoder", func(t *testing.T) {
		encoder := gocache.KeyEncoderFunc[string](func(key string) (string, error) {
			return "enc<" + key + ">", nil
		})
		cache := factory.New(t, Options[string, string]{KeyEncoder: encoder})
		ctx := context.Background()

		if err := cache.Set(ctx, "pre:1", "x", time.Minute); err != nil {
			t.Fatalf("set pre:1 failed: %v", err)
		}
		if err := cache.Set(ctx, "pre:2", "y", time.Minute); err != nil {
			t.Fatalf("set pre:2 failed: %v", err)
		}
		if err := cache.Set(ctx, "other:1", "z", time.Minute); err != nil {
			t.Fatalf("set other:1 failed: %v", err)
		}

		if typedPrefix, ok := cache.(gocache.TypedPrefixInvalidator[string]); ok {
			if err := typedPrefix.DeleteByKeyPrefix(ctx, "pre:"); err != nil {
				t.Fatalf("typed delete by key prefix failed: %v", err)
			}
			_, hit, err := cache.Get(ctx, "pre:1")
			if err != nil || hit {
				t.Fatalf("expected pre:1 miss, hit=%v err=%v", hit, err)
			}
			value, hit, err := cache.Get(ctx, "other:1")
			if err != nil || !hit || value != "z" {
				t.Fatalf("expected other:1 hit, hit=%v value=%q err=%v", hit, value, err)
			}
		}

		if typedTags, ok := cache.(gocache.TypedTagRegistry[string]); ok {
			if err := typedTags.AddTagsForKey(ctx, "other:1", []string{"grpX"}); err != nil {
				t.Fatalf("typed add tags failed: %v", err)
			}
			if err := typedTags.InvalidateTags(ctx, []string{"grpX"}); err != nil {
				t.Fatalf("typed invalidate tags failed: %v", err)
			}
			_, hit, err := cache.Get(ctx, "other:1")
			if err != nil || hit {
				t.Fatalf("expected other:1 miss after tag invalidation, hit=%v err=%v", hit, err)
			}
		}
	})
}

type decodeFailingCodec[V any] struct {
	base codec.Codec[V]
}

func (c decodeFailingCodec[V]) Marshal(value V) ([]byte, error) {
	return c.base.Marshal(value)
}

func (decodeFailingCodec[V]) Unmarshal([]byte) (V, error) {
	var zero V
	return zero, errors.New("decode failed")
}
