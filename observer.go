package gocache

import (
	"context"
	"time"
)

// Operation identifies a cache operation for instrumentation events.
type Operation string

const (
	OperationGetHit  Operation = "get_hit"
	OperationGetMiss Operation = "get_miss"
	OperationSet     Operation = "set"
	OperationDelete  Operation = "delete"
	OperationEvict   Operation = "evict"
	OperationError   Operation = "error"
)

// Observation represents a backend instrumentation event.
type Observation struct {
	Backend   string
	Operation Operation
	Key       string
	Err       error
	Latency   time.Duration
	// Count is the number of entries affected by an aggregate operation.
	Count int
	// Occupancy and Capacity report bounded backend usage without exposing keys
	// or values. Unbounded or unsupported backends leave them at zero.
	Occupancy int
	Capacity  int
}

// Observer consumes cache instrumentation events.
type Observer interface {
	Observe(ctx context.Context, event Observation)
}

// ObserverFunc adapts a function to Observer.
type ObserverFunc func(context.Context, Observation)

func (f ObserverFunc) Observe(ctx context.Context, event Observation) {
	if f != nil {
		f(ctx, event)
	}
}

type nopObserver struct{}

func (nopObserver) Observe(context.Context, Observation) {}

// NopObserver returns a no-op observer implementation.
func NopObserver() Observer {
	return nopObserver{}
}

// EnsureObserver returns observer when non-nil, otherwise a no-op observer.
func EnsureObserver(observer Observer) Observer {
	if observer == nil {
		return NopObserver()
	}
	return observer
}

// Observe safely emits one observation if observer is not nil.
func Observe(ctx context.Context, observer Observer, event Observation) {
	if observer == nil {
		return
	}
	observer.Observe(ctx, event)
}
