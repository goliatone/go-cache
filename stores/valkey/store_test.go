package valkey

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	gocache "github.com/goliatone/go-cache"
	"github.com/goliatone/go-cache/internal/conformancetest"
	vk "github.com/valkey-io/valkey-go"
)

var _ gocache.SetIfPresentCache[string, string] = (*Store[string, string])(nil)

func TestValkeyStoreStringConformance(t *testing.T) {
	mr := mustRunMiniRedis(t)
	defer mr.Close()

	factory := conformancetest.Factory[string, string]{
		Name: "valkey",
		New: func(t *testing.T, options conformancetest.Options[string, string]) gocache.Cache[string, string] {
			t.Helper()
			store := newStoreForTest[string, string](t, mr.Addr(), options)
			t.Cleanup(func() {
				_ = store.Close()
			})
			return store
		},
	}
	conformancetest.RunStringCacheContractTests(t, factory)
	conformancetest.RunStringLogicalCapabilityTests(t, factory)
	conformancetest.RunSetIfPresentEncodingFailureTests(t, factory)
}

func TestValkeyStoreIntKeyEncodingConformance(t *testing.T) {
	mr := mustRunMiniRedis(t)
	defer mr.Close()

	factory := conformancetest.Factory[int, string]{
		Name: "valkey",
		New: func(t *testing.T, options conformancetest.Options[int, string]) gocache.Cache[int, string] {
			t.Helper()
			store := newStoreForTest[int, string](t, mr.Addr(), options)
			t.Cleanup(func() {
				_ = store.Close()
			})
			return store
		},
	}
	conformancetest.RunIntKeyEncodingContractTests(t, factory)
}

func TestValkeyStoreTransientNetworkErrorMapping(t *testing.T) {
	mr := mustRunMiniRedis(t)
	store, err := NewStore[string, string](
		WithAddress[string, string](mr.Addr()),
		WithNamespace[string, string]("test"),
		WithClientOption[string, string](vk.ClientOption{
			ForceSingleClient: true,
			AlwaysRESP2:       true,
			DisableCache:      true,
		}),
	)
	if err != nil {
		t.Fatalf("new store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Set(context.Background(), "k", "v", time.Minute); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	mr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	_, _, err = store.Get(ctx, "k")
	if err == nil {
		t.Fatal("expected network error")
	}
	if !errors.Is(err, ErrTransientNetwork) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected transient network-style error, got %v", err)
	}
}

func TestValkeyStoreRequiresEncoderForNonStringKeys(t *testing.T) {
	mr := mustRunMiniRedis(t)
	defer mr.Close()

	_, err := NewStore[int, string](
		WithAddress[int, string](mr.Addr()),
		WithClientOption[int, string](vk.ClientOption{
			ForceSingleClient: true,
			AlwaysRESP2:       true,
			DisableCache:      true,
		}),
	)
	if !errors.Is(err, gocache.ErrKeyEncoderRequired) {
		t.Fatalf("expected ErrKeyEncoderRequired, got %v", err)
	}
}

func TestValkeyStoreDeleteByPrefixEscapesGlobPattern(t *testing.T) {
	mr := mustRunMiniRedis(t)
	defer mr.Close()

	store := newStoreForTest[string, string](t, mr.Addr(), conformancetest.Options[string, string]{})
	defer func() { _ = store.Close() }()

	if err := store.Set(context.Background(), "pre*1", "a", time.Minute); err != nil {
		t.Fatalf("set pre*1 failed: %v", err)
	}
	if err := store.Set(context.Background(), "pre:2", "b", time.Minute); err != nil {
		t.Fatalf("set pre:2 failed: %v", err)
	}

	if err := store.DeleteByPrefix(context.Background(), "pre*"); err != nil {
		t.Fatalf("delete by prefix failed: %v", err)
	}

	_, hit, err := store.Get(context.Background(), "pre*1")
	if err != nil {
		t.Fatalf("get pre*1 failed: %v", err)
	}
	if hit {
		t.Fatal("expected pre*1 to be deleted")
	}
	value, hit, err := store.Get(context.Background(), "pre:2")
	if err != nil {
		t.Fatalf("get pre:2 failed: %v", err)
	}
	if !hit || value != "b" {
		t.Fatalf("expected pre:2 to remain, hit=%v value=%q", hit, value)
	}
}

func TestValkeyStoreSetRollsBackOnIndexFailure(t *testing.T) {
	mr := mustRunMiniRedis(t)
	defer mr.Close()

	store := newStoreForTest[string, string](t, mr.Addr(), conformancetest.Options[string, string]{})
	defer func() { _ = store.Close() }()

	store.client = saddFailClient{Client: store.client}
	err := store.Set(context.Background(), "k", "v", time.Minute)
	if err == nil {
		t.Fatal("expected set error from injected SADD failure")
	}

	_, hit, getErr := store.Get(context.Background(), "k")
	if getErr != nil {
		t.Fatalf("get failed: %v", getErr)
	}
	if hit {
		t.Fatal("expected key rollback after index failure")
	}
}

func TestValkeyStoreSetIfAbsentRollsBackOnIndexFailure(t *testing.T) {
	mr := mustRunMiniRedis(t)
	defer mr.Close()

	store := newStoreForTest[string, string](t, mr.Addr(), conformancetest.Options[string, string]{})
	defer func() { _ = store.Close() }()

	store.client = saddFailClient{Client: store.client}
	set, err := store.SetIfAbsent(context.Background(), "k", "v", time.Minute)
	if err == nil {
		t.Fatal("expected set-if-absent error from injected SADD failure")
	}
	if set {
		t.Fatal("expected set-if-absent result false on error")
	}

	_, hit, getErr := store.Get(context.Background(), "k")
	if getErr != nil {
		t.Fatalf("get failed: %v", getErr)
	}
	if hit {
		t.Fatal("expected key rollback after index failure")
	}
}

func TestValkeyStoreSetIfPresentExpiredDoesNotRecreate(t *testing.T) {
	mr := mustRunMiniRedis(t)
	defer mr.Close()

	store := newStoreForTest[string, string](t, mr.Addr(), conformancetest.Options[string, string]{})
	defer func() { _ = store.Close() }()
	if err := store.Set(context.Background(), "k", "old", time.Second); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	mr.FastForward(2 * time.Second)
	updated, err := store.SetIfPresent(context.Background(), "k", "new", time.Minute)
	if err != nil {
		t.Fatalf("set-if-present failed: %v", err)
	}
	if updated {
		t.Fatal("expected expired key not to be recreated")
	}
	_, hit, err := store.Get(context.Background(), "k")
	if err != nil || hit {
		t.Fatalf("expected expired key to remain absent, hit=%v err=%v", hit, err)
	}
}

func newStoreForTest[K comparable, V any](t *testing.T, address string, options conformancetest.Options[K, V]) *Store[K, V] {
	t.Helper()
	opts := []Option[K, V]{
		WithAddress[K, V](address),
		WithNamespace[K, V]("test"),
		WithClientOption[K, V](vk.ClientOption{
			ForceSingleClient: true,
			AlwaysRESP2:       true,
			DisableCache:      true,
		}),
	}
	if options.Codec != nil {
		opts = append(opts, WithCodec[K, V](options.Codec))
	}
	if options.KeyEncoder != nil {
		opts = append(opts, WithKeyEncoder[K, V](options.KeyEncoder))
	}
	if options.Observer != nil {
		opts = append(opts, WithObserver[K, V](options.Observer))
	}
	store, err := NewStore[K, V](opts...)
	if err != nil {
		t.Fatalf("new store failed: %v", err)
	}
	return store
}

func mustRunMiniRedis(t *testing.T) *miniredis.Miniredis {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	return mr
}

type saddFailClient struct {
	vk.Client
}

func (c saddFailClient) Do(ctx context.Context, cmd vk.Completed) vk.ValkeyResult {
	commands := cmd.Commands()
	if len(commands) > 0 && strings.EqualFold(commands[0], "SADD") {
		return c.Client.Do(ctx, c.Client.B().Arbitrary("NOT_A_COMMAND").Build())
	}
	return c.Client.Do(ctx, cmd)
}
