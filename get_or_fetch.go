package gocache

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	// ErrNilFetch is returned when a nil fetch callback is supplied.
	ErrNilFetch = errors.New("gocache: nil fetch function")
	// ErrInvalidSingleflightValue is returned when a singleflight result cannot be type asserted to V.
	ErrInvalidSingleflightValue = errors.New("gocache: invalid singleflight value type")
)

// ReadErrorMode controls behavior when cache read operations fail.
// TODO: prefer string enums so they are readable and easier to understand in traces/logs
type ReadErrorMode uint8

const (
	// ReadErrorFail returns cache read errors to callers.
	ReadErrorFail ReadErrorMode = iota
	// ReadErrorBypass ignores cache read errors and proceeds with fetch.
	ReadErrorBypass
)

// WriteErrorMode controls behavior when cache write operations fail.
// TODO: prefer string enums so they are readable and easier to understand in traces/logs
type WriteErrorMode uint8

const (
	// WriteErrorFail returns cache write errors to callers.
	WriteErrorFail WriteErrorMode = iota
	// WriteErrorIgnore ignores cache write errors after successful fetch.
	WriteErrorIgnore
)

// SingleflightGroup is compatible with golang.org/x/sync/singleflight.Group.
// The interface keeps go-cache decoupled from that dependency.
type SingleflightGroup interface {
	Do(key string, fn func() (any, error)) (any, error, bool)
}

// GetOrFetchOptions configures read-through behavior.
type GetOrFetchOptions struct {
	TTL            time.Duration
	ReadErrorMode  ReadErrorMode
	WriteErrorMode WriteErrorMode
	Group          SingleflightGroup
	GroupKey       string
}

// GetOrFetch returns a cached value or fetches and stores it with the given TTL.
// TTL <= 0 is treated as non-expiring by compliant cache backends.
func GetOrFetch[K comparable, V any](
	ctx context.Context,
	cache Cache[K, V],
	key K,
	ttl time.Duration,
	fetch func(context.Context) (V, error),
) (V, error) {
	return GetOrFetchWithOptions(ctx, cache, key, GetOrFetchOptions{TTL: ttl}, fetch)
}

// GetOrFetchWithOptions is the configurable read-through helper.
func GetOrFetchWithOptions[K comparable, V any](
	ctx context.Context,
	cache Cache[K, V],
	key K,
	opts GetOrFetchOptions,
	fetch func(context.Context) (V, error),
) (V, error) {
	var zero V

	if fetch == nil {
		return zero, ErrNilFetch
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// No cache implementation supplied: behave as plain fetch-through.
	if cache == nil {
		return fetch(ctx)
	}

	read := func(ctx context.Context) (V, bool, error) {
		value, ok, err := cache.Get(ctx, key)
		if err != nil && opts.ReadErrorMode == ReadErrorBypass {
			return zero, false, nil
		}
		return value, ok, err
	}

	write := func(ctx context.Context, value V) error {
		if err := cache.Set(ctx, key, value, opts.TTL); err != nil {
			if opts.WriteErrorMode == WriteErrorIgnore {
				return nil
			}
			return err
		}
		return nil
	}

	if value, ok, err := read(ctx); err != nil {
		return zero, err
	} else if ok {
		return value, nil
	}

	if opts.Group == nil || opts.GroupKey == "" {
		value, err := fetch(ctx)
		if err != nil {
			return zero, err
		}
		if err := write(ctx, value); err != nil {
			return zero, err
		}
		return value, nil
	}

	out, err, _ := opts.Group.Do(opts.GroupKey, func() (any, error) {
		// Re-check inside group to avoid duplicate upstream calls.
		if value, ok, readErr := read(ctx); readErr != nil {
			return nil, readErr
		} else if ok {
			return value, nil
		}

		value, fetchErr := fetch(ctx)
		if fetchErr != nil {
			return nil, fetchErr
		}
		if writeErr := write(ctx, value); writeErr != nil {
			return nil, writeErr
		}
		return value, nil
	})
	if err != nil {
		return zero, err
	}

	value, ok := out.(V)
	if !ok {
		return zero, fmt.Errorf("%w: got %T", ErrInvalidSingleflightValue, out)
	}
	return value, nil
}
