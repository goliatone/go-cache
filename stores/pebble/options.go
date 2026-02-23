package pebble

import (
	"errors"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	gocache "github.com/goliatone/go-cache"
	"github.com/goliatone/go-cache/codec"
)

// Option configures pebble store construction.
type Option[K comparable, V any] func(*Store[K, V]) error

// WithPath sets the Pebble DB path.
func WithPath[K comparable, V any](path string) Option[K, V] {
	return func(store *Store[K, V]) error {
		path = strings.TrimSpace(path)
		if path == "" {
			return errors.New("pebble: empty path")
		}
		store.path = path
		return nil
	}
}

// WithDB injects an existing Pebble DB instance.
func WithDB[K comparable, V any](db *pebble.DB) Option[K, V] {
	return func(store *Store[K, V]) error {
		if db == nil {
			return errors.New("pebble: nil db")
		}
		store.db = db
		store.ownsDB = false
		return nil
	}
}

// WithNamespace sets a namespace prefix for stored keys.
func WithNamespace[K comparable, V any](namespace string) Option[K, V] {
	return func(store *Store[K, V]) error {
		namespace = strings.TrimSpace(namespace)
		if namespace == "" {
			return errors.New("pebble: empty namespace")
		}
		store.namespace = namespace
		return nil
	}
}

// WithCodec sets value codec.
func WithCodec[K comparable, V any](c codec.Codec[V]) Option[K, V] {
	return func(store *Store[K, V]) error {
		if c == nil {
			return errors.New("pebble: nil codec")
		}
		store.codec = c
		return nil
	}
}

// WithKeyEncoder sets key encoder.
func WithKeyEncoder[K comparable, V any](encoder gocache.KeyEncoder[K]) Option[K, V] {
	return func(store *Store[K, V]) error {
		store.keyEncoder = encoder
		return nil
	}
}

// WithObserver sets observer.
func WithObserver[K comparable, V any](observer gocache.Observer) Option[K, V] {
	return func(store *Store[K, V]) error {
		store.observer = observer
		return nil
	}
}

// WithClock sets clock for TTL checks.
func WithClock[K comparable, V any](clock func() time.Time) Option[K, V] {
	return func(store *Store[K, V]) error {
		if clock == nil {
			return errors.New("pebble: nil clock")
		}
		store.now = clock
		return nil
	}
}
