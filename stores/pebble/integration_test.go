package pebble

import (
	"path/filepath"
	"testing"
	"time"
)

func TestPebbleDurabilityAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cache.db")

	store, err := NewStore[string, string](
		WithPath[string, string](path),
		WithNamespace[string, string]("itest"),
	)
	if err != nil {
		t.Fatalf("new store failed: %v", err)
	}
	if err := store.Set(nil, "durable-key", "value", time.Minute); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	store, err = NewStore[string, string](
		WithPath[string, string](path),
		WithNamespace[string, string]("itest"),
	)
	if err != nil {
		t.Fatalf("reopen store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	value, hit, err := store.Get(nil, "durable-key")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if !hit || value != "value" {
		t.Fatalf("expected durable hit, got hit=%v value=%q", hit, value)
	}
}

func TestPebbleExpiredNeverReturnedAfterRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cache.db")

	now := time.Now()
	store, err := NewStore[string, string](
		WithPath[string, string](path),
		WithNamespace[string, string]("itest"),
		WithClock[string, string](func() time.Time { return now }),
	)
	if err != nil {
		t.Fatalf("new store failed: %v", err)
	}
	if err := store.Set(nil, "expiring-key", "value", 100*time.Millisecond); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	now = now.Add(250 * time.Millisecond)
	store, err = NewStore[string, string](
		WithPath[string, string](path),
		WithNamespace[string, string]("itest"),
		WithClock[string, string](func() time.Time { return now }),
	)
	if err != nil {
		t.Fatalf("reopen store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	value, hit, err := store.Get(nil, "expiring-key")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if hit || value != "" {
		t.Fatalf("expected expired miss after restart, got hit=%v value=%q", hit, value)
	}
}
