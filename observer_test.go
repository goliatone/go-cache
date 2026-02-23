package gocache

import (
	"context"
	"testing"
	"time"
)

func TestObserveWithNilObserverIsSafe(t *testing.T) {
	Observe(context.Background(), nil, Observation{
		Backend:   "memory",
		Operation: OperationGetHit,
		Key:       "k",
	})
}

func TestEnsureObserverReturnsNoop(t *testing.T) {
	observer := EnsureObserver(nil)
	if observer == nil {
		t.Fatal("expected non-nil observer")
	}
	observer.Observe(context.Background(), Observation{})
}

func TestObserverFuncReceivesEvent(t *testing.T) {
	calls := 0
	observer := ObserverFunc(func(_ context.Context, event Observation) {
		calls++
		if event.Backend != "memory" {
			t.Fatalf("unexpected backend: %q", event.Backend)
		}
		if event.Operation != OperationSet {
			t.Fatalf("unexpected operation: %q", event.Operation)
		}
		if event.Key != "abc" {
			t.Fatalf("unexpected key: %q", event.Key)
		}
		if event.Latency != 25*time.Millisecond {
			t.Fatalf("unexpected latency: %s", event.Latency)
		}
	})

	Observe(context.Background(), observer, Observation{
		Backend:   "memory",
		Operation: OperationSet,
		Key:       "abc",
		Latency:   25 * time.Millisecond,
	})
	if calls != 1 {
		t.Fatalf("expected one call, got %d", calls)
	}
}
