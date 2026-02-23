//go:build rocksdb

package rocksdb

import (
	"errors"
	"strings"
	"time"

	gocache "github.com/goliatone/go-cache"
	"github.com/goliatone/go-cache/codec"
	"github.com/linxGnu/grocksdb"
)

// Option configures rocksdb store construction.
type Option[K comparable, V any] func(*Store[K, V]) error

// WithPath sets DB path.
func WithPath[K comparable, V any](path string) Option[K, V] {
	return func(store *Store[K, V]) error {
		path = strings.TrimSpace(path)
		if path == "" {
			return errors.New("rocksdb: empty path")
		}
		store.path = path
		return nil
	}
}

// WithDB injects existing DB.
func WithDB[K comparable, V any](db *grocksdb.DB) Option[K, V] {
	return func(store *Store[K, V]) error {
		if db == nil {
			return errors.New("rocksdb: nil db")
		}
		store.db = db
		store.ownsDB = false
		return nil
	}
}

// WithNamespace sets namespace.
func WithNamespace[K comparable, V any](namespace string) Option[K, V] {
	return func(store *Store[K, V]) error {
		namespace = strings.TrimSpace(namespace)
		if namespace == "" {
			return errors.New("rocksdb: empty namespace")
		}
		store.namespace = namespace
		return nil
	}
}

// WithCodec sets codec.
func WithCodec[K comparable, V any](c codec.Codec[V]) Option[K, V] {
	return func(store *Store[K, V]) error {
		if c == nil {
			return errors.New("rocksdb: nil codec")
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

// WithClock sets clock for ttl checks.
func WithClock[K comparable, V any](clock func() time.Time) Option[K, V] {
	return func(store *Store[K, V]) error {
		if clock == nil {
			return errors.New("rocksdb: nil clock")
		}
		store.now = clock
		return nil
	}
}
