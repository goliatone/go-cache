package gocache

import (
	"context"
	"errors"
	"testing"
	"time"
)

type cacheStub struct {
	getFn    func(context.Context, string) (string, bool, error)
	setFn    func(context.Context, string, string, time.Duration) error
	getCalls int
	setCalls int
	lastTTL  time.Duration
}

func (s *cacheStub) Get(ctx context.Context, key string) (string, bool, error) {
	s.getCalls++
	if s.getFn == nil {
		return "", false, nil
	}
	return s.getFn(ctx, key)
}

func (s *cacheStub) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	s.setCalls++
	s.lastTTL = ttl
	if s.setFn == nil {
		return nil
	}
	return s.setFn(ctx, key, value, ttl)
}

func (s *cacheStub) Delete(context.Context, string) error {
	return nil
}

type groupStub struct {
	runFn  bool
	result any
	err    error
	calls  int
}

func (g *groupStub) Do(_ string, fn func() (any, error)) (any, error, bool) {
	g.calls++
	if g.runFn {
		v, err := fn()
		return v, err, false
	}
	return g.result, g.err, true
}

func TestGetOrFetchTTLNonPositiveWritesToCache(t *testing.T) {
	cache := &cacheStub{
		setFn: func(_ context.Context, _ string, value string, ttl time.Duration) error {
			if value != "fetched" {
				t.Fatalf("unexpected value written: %q", value)
			}
			if ttl != -1*time.Second {
				t.Fatalf("expected ttl -1s, got %s", ttl)
			}
			return nil
		},
	}

	got, err := GetOrFetch(context.Background(), cache, "k1", -1*time.Second, func(context.Context) (string, error) {
		return "fetched", nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "fetched" {
		t.Fatalf("expected fetched, got %q", got)
	}
	if cache.setCalls != 1 {
		t.Fatalf("expected one write, got %d", cache.setCalls)
	}
}

func TestGetOrFetchReadErrorModes(t *testing.T) {
	readErr := errors.New("read failed")

	t.Run("fail", func(t *testing.T) {
		cache := &cacheStub{
			getFn: func(context.Context, string) (string, bool, error) {
				return "", false, readErr
			},
		}
		fetchCalls := 0
		_, err := GetOrFetchWithOptions(context.Background(), cache, "k", GetOrFetchOptions{
			TTL:           time.Second,
			ReadErrorMode: ReadErrorFail,
		}, func(context.Context) (string, error) {
			fetchCalls++
			return "x", nil
		})
		if !errors.Is(err, readErr) {
			t.Fatalf("expected read error, got %v", err)
		}
		if fetchCalls != 0 {
			t.Fatalf("expected no fetch calls, got %d", fetchCalls)
		}
	})

	t.Run("bypass", func(t *testing.T) {
		cache := &cacheStub{
			getFn: func(context.Context, string) (string, bool, error) {
				return "", false, readErr
			},
		}
		fetchCalls := 0
		got, err := GetOrFetchWithOptions(context.Background(), cache, "k", GetOrFetchOptions{
			TTL:           time.Second,
			ReadErrorMode: ReadErrorBypass,
		}, func(context.Context) (string, error) {
			fetchCalls++
			return "x", nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "x" {
			t.Fatalf("expected x, got %q", got)
		}
		if fetchCalls != 1 {
			t.Fatalf("expected one fetch call, got %d", fetchCalls)
		}
	})
}

func TestGetOrFetchWriteErrorModes(t *testing.T) {
	writeErr := errors.New("write failed")

	t.Run("fail", func(t *testing.T) {
		cache := &cacheStub{
			setFn: func(context.Context, string, string, time.Duration) error {
				return writeErr
			},
		}
		_, err := GetOrFetchWithOptions(context.Background(), cache, "k", GetOrFetchOptions{
			TTL:            time.Second,
			WriteErrorMode: WriteErrorFail,
		}, func(context.Context) (string, error) {
			return "x", nil
		})
		if !errors.Is(err, writeErr) {
			t.Fatalf("expected write error, got %v", err)
		}
	})

	t.Run("ignore", func(t *testing.T) {
		cache := &cacheStub{
			setFn: func(context.Context, string, string, time.Duration) error {
				return writeErr
			},
		}
		got, err := GetOrFetchWithOptions(context.Background(), cache, "k", GetOrFetchOptions{
			TTL:            time.Second,
			WriteErrorMode: WriteErrorIgnore,
		}, func(context.Context) (string, error) {
			return "x", nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "x" {
			t.Fatalf("expected x, got %q", got)
		}
	})
}

func TestGetOrFetchSingleflight(t *testing.T) {
	cache := &cacheStub{}
	group := &groupStub{runFn: true}
	fetchCalls := 0

	got, err := GetOrFetchWithOptions(context.Background(), cache, "k", GetOrFetchOptions{
		TTL:      time.Second,
		Group:    group,
		GroupKey: "k",
	}, func(context.Context) (string, error) {
		fetchCalls++
		return "x", nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "x" {
		t.Fatalf("expected x, got %q", got)
	}
	if group.calls != 1 {
		t.Fatalf("expected one group call, got %d", group.calls)
	}
	if fetchCalls != 1 {
		t.Fatalf("expected one fetch call, got %d", fetchCalls)
	}
}

func TestGetOrFetchSingleflightInvalidType(t *testing.T) {
	group := &groupStub{result: 123}
	cache := &cacheStub{}

	_, err := GetOrFetchWithOptions(context.Background(), cache, "k", GetOrFetchOptions{
		TTL:      time.Second,
		Group:    group,
		GroupKey: "k",
	}, func(context.Context) (string, error) {
		return "x", nil
	})
	if !errors.Is(err, ErrInvalidSingleflightValue) {
		t.Fatalf("expected ErrInvalidSingleflightValue, got %v", err)
	}
}

func TestGetOrFetchNilCacheFetchThrough(t *testing.T) {
	var cache Cache[string, string]
	fetchCalls := 0

	got, err := GetOrFetch(context.Background(), cache, "k", time.Second, func(context.Context) (string, error) {
		fetchCalls++
		return "x", nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "x" {
		t.Fatalf("expected x, got %q", got)
	}
	if fetchCalls != 1 {
		t.Fatalf("expected one fetch call, got %d", fetchCalls)
	}
}

func TestGetOrFetchNilFetch(t *testing.T) {
	cache := &cacheStub{}
	_, err := GetOrFetch(context.Background(), cache, "k", time.Second, nil)
	if !errors.Is(err, ErrNilFetch) {
		t.Fatalf("expected ErrNilFetch, got %v", err)
	}
}
