package gocache

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrLogicalPrefixUnsupported indicates logical-prefix invalidation was requested on a non-string key type.
	ErrLogicalPrefixUnsupported = errors.New("gocache: logical prefix invalidation unsupported for non-string key type")
)

// Cache is the minimal cross-package key/value cache contract.
// Implementations may be in-memory or distributed.
type Cache[K comparable, V any] interface {
	Get(ctx context.Context, key K) (V, bool, error)
	Set(ctx context.Context, key K, value V, ttl time.Duration) error
	Delete(ctx context.Context, key K) error
}

// Purgeable is an optional capability for stores that can actively evict expired entries.
type Purgeable interface {
	PurgeExpired(ctx context.Context) (int, error)
}

// Clearable is an optional capability for stores that can delete all entries.
type Clearable interface {
	Clear(ctx context.Context) error
}

// SetIfAbsentCache is an optional atomic-set capability.
type SetIfAbsentCache[K comparable, V any] interface {
	SetIfAbsent(ctx context.Context, key K, value V, ttl time.Duration) (bool, error)
}

// SetIfPresentCache is an optional atomic-update capability.
//
// SetIfPresent replaces a live entry and resets its TTL without creating a
// missing or expired entry. It returns true only when the replacement was
// applied. A non-positive TTL makes the replacement non-expiring, matching
// Cache.Set semantics. Implementations must leave existing logical-key and tag
// metadata intact and must honor context cancellation and encoding failures
// without partially replacing the entry.
type SetIfPresentCache[K comparable, V any] interface {
	SetIfPresent(ctx context.Context, key K, value V, ttl time.Duration) (bool, error)
}

// PrefixInvalidator is an optional key-prefix invalidation capability.
//
// Deprecated: prefer TypedPrefixInvalidator[K] so prefix invalidation is keyed by the logical key type.
// This legacy interface historically encouraged encoded-key usage and is retained for compatibility only.
type PrefixInvalidator interface {
	DeleteByPrefix(ctx context.Context, prefix string) error
}

// TypedPrefixInvalidator is an optional logical key-prefix invalidation capability.
// For string-like key types, implementations should treat prefix as logical-key prefix (not storage-key prefix).
type TypedPrefixInvalidator[K comparable] interface {
	DeleteByKeyPrefix(ctx context.Context, prefix K) error
}

// BatchInvalidator is an optional capability for invalidating multiple keys at once.
type BatchInvalidator[K comparable] interface {
	InvalidateKeys(ctx context.Context, keys []K) error
}

// TagRegistry is an optional capability for tag-based invalidation.
//
// Deprecated: prefer TypedTagRegistry[K] so tag registration can be done with typed logical keys.
// This legacy interface uses string keys and is retained for compatibility only.
type TagRegistry interface {
	AddTags(ctx context.Context, key string, tags []string) error
	InvalidateTags(ctx context.Context, tags []string) error
}

// TypedTagRegistry is an optional capability for tag-based invalidation keyed by logical typed keys.
type TypedTagRegistry[K comparable] interface {
	AddTagsForKey(ctx context.Context, key K, tags []string) error
	InvalidateTags(ctx context.Context, tags []string) error
}
