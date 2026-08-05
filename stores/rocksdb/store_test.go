//go:build rocksdb

package rocksdb

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	gocache "github.com/goliatone/go-cache"
	"github.com/goliatone/go-cache/internal/conformancetest"
	"github.com/linxGnu/grocksdb"
)

var _ gocache.SetIfPresentCache[string, string] = (*Store[string, string])(nil)

func TestRocksDBStoreStringConformance(t *testing.T) {
	factory := conformancetest.Factory[string, string]{
		Name: "rocksdb",
		New: func(t *testing.T, options conformancetest.Options[string, string]) gocache.Cache[string, string] {
			t.Helper()
			dir := t.TempDir()
			opts := []Option[string, string]{
				WithPath[string, string](filepath.Join(dir, "cache.db")),
				WithNamespace[string, string]("test"),
			}
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

func TestRocksDBStoreIntKeyEncodingConformance(t *testing.T) {
	factory := conformancetest.Factory[int, string]{
		Name: "rocksdb",
		New: func(t *testing.T, options conformancetest.Options[int, string]) gocache.Cache[int, string] {
			t.Helper()
			dir := t.TempDir()
			opts := []Option[int, string]{
				WithPath[int, string](filepath.Join(dir, "cache.db")),
				WithNamespace[int, string]("test"),
			}
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
			t.Cleanup(func() {
				_ = store.Close()
			})
			return store
		},
	}
	conformancetest.RunIntKeyEncodingContractTests(t, factory)
}

func TestRocksDBStoreRequiresEncoderForNonStringKeys(t *testing.T) {
	dir := t.TempDir()
	_, err := NewStore[int, string](WithPath[int, string](filepath.Join(dir, "cache.db")))
	if !errors.Is(err, gocache.ErrKeyEncoderRequired) {
		t.Fatalf("expected ErrKeyEncoderRequired, got %v", err)
	}
}

func TestRocksDBStoreSetIfAbsentExpiredReplacement(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	store, err := NewStore[string, string](
		WithPath[string, string](filepath.Join(dir, "cache.db")),
		WithNamespace[string, string]("test"),
		WithClock[string, string](func() time.Time { return now }),
	)
	if err != nil {
		t.Fatalf("new store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Set(nil, "k", "v1", 50*time.Millisecond); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	now = now.Add(80 * time.Millisecond)
	ok, err := store.SetIfAbsent(nil, "k", "v2", time.Minute)
	if err != nil {
		t.Fatalf("set if absent failed: %v", err)
	}
	if !ok {
		t.Fatal("expected expired key to be replaced")
	}
	value, hit, err := store.Get(nil, "k")
	if err != nil || !hit || value != "v2" {
		t.Fatalf("expected replaced value, hit=%v value=%q err=%v", hit, value, err)
	}
}

func TestRocksDBStoreSharedDBInvalidationSerializesWithSetIfPresent(t *testing.T) {
	options := grocksdb.NewDefaultOptions()
	options.SetCreateIfMissing(true)
	defer options.Destroy()
	db, err := grocksdb.OpenDb(options, filepath.Join(t.TempDir(), "shared.db"))
	if err != nil {
		t.Fatalf("open shared db: %v", err)
	}
	defer db.Close()

	entered := make(chan struct{})
	resume := make(chan struct{})
	var clockCalls atomic.Int32
	updater, err := NewStore[string, string](
		WithDB[string, string](db),
		WithNamespace[string, string]("shared"),
		WithClock[string, string](func() time.Time {
			if clockCalls.Add(1) == 2 {
				close(entered)
				<-resume
			}
			return time.Now()
		}),
	)
	if err != nil {
		t.Fatalf("new updater store: %v", err)
	}
	defer func() { _ = updater.Close() }()
	invalidator, err := NewStore[string, string](
		WithDB[string, string](db),
		WithNamespace[string, string]("shared"),
	)
	if err != nil {
		t.Fatalf("new invalidator store: %v", err)
	}
	defer func() { _ = invalidator.Close() }()
	if err := invalidator.Set(context.Background(), "k", "old", time.Minute); err != nil {
		t.Fatalf("set fixture: %v", err)
	}

	type updateResult struct {
		updated bool
		err     error
	}
	updateDone := make(chan updateResult, 1)
	go func() {
		updated, updateErr := updater.SetIfPresent(context.Background(), "k", "renewed", time.Minute)
		updateDone <- updateResult{updated: updated, err: updateErr}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("conditional update did not reach the post-read clock")
	}
	deleteDone := make(chan error, 1)
	go func() { deleteDone <- invalidator.Delete(context.Background(), "k") }()
	var deleteErr error
	deleteReturnedEarly := false
	select {
	case deleteErr = <-deleteDone:
		deleteReturnedEarly = true
	case <-time.After(50 * time.Millisecond):
	}
	close(resume)
	result := <-updateDone
	if !deleteReturnedEarly {
		deleteErr = <-deleteDone
	}
	if result.err != nil || !result.updated {
		t.Fatalf("conditional update failed: updated=%v err=%v", result.updated, result.err)
	}
	if deleteErr != nil {
		t.Fatalf("delete failed: %v", deleteErr)
	}
	if deleteReturnedEarly {
		t.Fatal("delete bypassed the shared database mutation lock")
	}
	_, hit, err := updater.Get(context.Background(), "k")
	if err != nil || hit {
		t.Fatalf("invalidation was undone by conditional update: hit=%v err=%v", hit, err)
	}
}
