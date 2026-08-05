package pebble

import (
	"context"
	"encoding/base64"
	"errors"
	"reflect"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	gocache "github.com/goliatone/go-cache"
	"github.com/goliatone/go-cache/codec"
	"github.com/goliatone/go-cache/internal/envelope"
	"github.com/goliatone/go-cache/internal/storelock"
)

const (
	defaultNamespace = "gocache"
	backendName      = "pebble"
)

var (
	errNoPathOrDB = errors.New("pebble: either path or db must be provided")
	mutationLocks storelock.Registry[*pebble.DB]
)

// Store implements gocache over Pebble.
type Store[K comparable, V any] struct {
	db     *pebble.DB
	ownsDB bool
	path   string

	namespace  string
	codec      codec.Codec[V]
	keyEncoder gocache.KeyEncoder[K]
	observer   gocache.Observer
	now        func() time.Time

	mutationLock *storelock.Lease[*pebble.DB]
}

// NewStore creates a new Pebble-backed store.
func NewStore[K comparable, V any](opts ...Option[K, V]) (*Store[K, V], error) {
	store := &Store[K, V]{
		namespace: defaultNamespace,
		codec:     codec.NewJSONCodec[V](),
		observer:  gocache.NopObserver(),
		now:       time.Now,
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(store); err != nil {
			return nil, err
		}
	}

	resolvedEncoder, err := gocache.ResolveKeyEncoder(store.keyEncoder)
	if err != nil {
		return nil, err
	}
	store.keyEncoder = resolvedEncoder
	store.observer = gocache.EnsureObserver(store.observer)

	if store.db == nil {
		if strings.TrimSpace(store.path) == "" {
			return nil, errNoPathOrDB
		}
		db, err := pebble.Open(store.path, &pebble.Options{})
		if err != nil {
			return nil, err
		}
		store.db = db
		store.ownsDB = true
	}
	store.mutationLock = mutationLocks.Acquire(store.db)
	return store, nil
}

// Close closes the owned Pebble DB.
func (s *Store[K, V]) Close() error {
	if s.mutationLock != nil {
		defer s.mutationLock.Release()
	}
	if s.ownsDB && s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *Store[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	start := time.Now()
	ctx = normalizeContext(ctx)
	var zero V
	if err := ctx.Err(); err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return zero, false, err
	}
	dataKey, err := s.dataKey(key)
	if err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return zero, false, err
	}

	value, closer, err := s.db.Get([]byte(dataKey))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			s.observe(ctx, gocache.OperationGetMiss, dataKey, start, nil)
			return zero, false, nil
		}
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return zero, false, err
	}
	raw := append([]byte(nil), value...)
	_ = closer.Close()

	record, err := envelope.Unmarshal(raw)
	if err != nil {
		s.mutationLock.Lock()
		_ = s.deleteDataKeyAndMetadataLocked(ctx, dataKey)
		s.mutationLock.Unlock()
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return zero, false, err
	}

	nowUnix := s.now().UnixNano()
	if record.ExpiresAtUnixNano > 0 && nowUnix >= record.ExpiresAtUnixNano {
		s.mutationLock.Lock()
		_ = s.deleteDataKeyAndMetadataLocked(ctx, dataKey)
		s.mutationLock.Unlock()
		s.observe(ctx, gocache.OperationGetMiss, dataKey, start, nil)
		return zero, false, nil
	}

	decoded, err := s.codec.Unmarshal(record.Payload)
	if err != nil {
		s.mutationLock.Lock()
		_ = s.deleteDataKeyAndMetadataLocked(ctx, dataKey)
		s.mutationLock.Unlock()
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return zero, false, err
	}

	s.observe(ctx, gocache.OperationGetHit, dataKey, start, nil)
	return decoded, true, nil
}

func (s *Store[K, V]) Set(ctx context.Context, key K, value V, ttl time.Duration) error {
	start := time.Now()
	ctx = normalizeContext(ctx)
	if err := ctx.Err(); err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return err
	}
	dataKey, err := s.dataKey(key)
	if err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return err
	}
	payload, err := s.codec.Marshal(value)
	if err != nil {
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return err
	}
	var expiresAt int64
	if ttl > 0 {
		expiresAt = s.now().Add(ttl).UnixNano()
	}
	encoded, err := envelope.Marshal(payload, expiresAt)
	if err != nil {
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return err
	}
	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()
	if err := batch.Set([]byte(dataKey), encoded, nil); err != nil {
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return err
	}
	if err := s.setLogicalIndexInBatch(batch, key, dataKey); err != nil {
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return err
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return err
	}
	s.observe(ctx, gocache.OperationSet, dataKey, start, nil)
	return nil
}

// SetIfPresent atomically replaces a live entry while preserving its indices.
func (s *Store[K, V]) SetIfPresent(ctx context.Context, key K, value V, ttl time.Duration) (bool, error) {
	start := time.Now()
	ctx = normalizeContext(ctx)
	if err := ctx.Err(); err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return false, err
	}
	dataKey, err := s.dataKey(key)
	if err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return false, err
	}
	payload, err := s.codec.Marshal(value)
	if err != nil {
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return false, err
	}
	var expiresAt int64
	if ttl > 0 {
		expiresAt = s.now().Add(ttl).UnixNano()
	}
	encoded, err := envelope.Marshal(payload, expiresAt)
	if err != nil {
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return false, err
	}

	s.mutationLock.Lock()
	updated, err := s.setIfPresentLocked(ctx, dataKey, encoded)
	s.mutationLock.Unlock()
	if err != nil {
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return false, err
	}
	if updated {
		s.observe(ctx, gocache.OperationSet, dataKey, start, nil)
	}
	return updated, nil
}

func (s *Store[K, V]) setIfPresentLocked(ctx context.Context, dataKey string, encoded []byte) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	raw, closer, err := s.db.Get([]byte(dataKey))
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	existing := append([]byte(nil), raw...)
	_ = closer.Close()
	record, err := envelope.Unmarshal(existing)
	if err != nil {
		return false, err
	}
	if record.ExpiresAtUnixNano > 0 && s.now().UnixNano() >= record.ExpiresAtUnixNano {
		if err := s.deleteDataKeyAndMetadataLocked(ctx, dataKey); err != nil {
			return false, err
		}
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := s.db.Set([]byte(dataKey), encoded, pebble.Sync); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store[K, V]) Delete(ctx context.Context, key K) error {
	start := time.Now()
	ctx = normalizeContext(ctx)
	if err := ctx.Err(); err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return err
	}
	dataKey, err := s.dataKey(key)
	if err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return err
	}
	s.mutationLock.Lock()
	err = s.deleteDataKeyAndMetadataLocked(ctx, dataKey)
	s.mutationLock.Unlock()
	if err != nil {
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return err
	}
	s.observe(ctx, gocache.OperationDelete, dataKey, start, nil)
	return nil
}

// Clear deletes all records for this namespace.
func (s *Store[K, V]) Clear(ctx context.Context) error {
	start := time.Now()
	ctx = normalizeContext(ctx)
	if err := ctx.Err(); err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return err
	}
	s.mutationLock.Lock()
	defer s.mutationLock.Unlock()

	prefix := s.namespacePrefix()
	keys, err := s.collectKeysByPrefix(ctx, prefix)
	if err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return err
	}
	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()
	for _, key := range keys {
		if err := batch.Delete([]byte(key), nil); err != nil {
			s.observe(ctx, gocache.OperationError, "", start, err)
			return err
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return err
	}
	s.observe(ctx, gocache.OperationDelete, "", start, nil)
	return nil
}

// PurgeExpired deletes expired entries and returns removed count.
func (s *Store[K, V]) PurgeExpired(ctx context.Context) (int, error) {
	start := time.Now()
	ctx = normalizeContext(ctx)
	if err := ctx.Err(); err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return 0, err
	}
	s.mutationLock.Lock()
	defer s.mutationLock.Unlock()

	dataKeys, err := s.collectKeysByPrefix(ctx, s.dataPrefix())
	if err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return 0, err
	}
	nowUnix := s.now().UnixNano()
	removed := 0
	for _, dataKey := range dataKeys {
		value, closer, err := s.db.Get([]byte(dataKey))
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			s.observe(ctx, gocache.OperationError, dataKey, start, err)
			return removed, err
		}
		raw := append([]byte(nil), value...)
		_ = closer.Close()
		record, err := envelope.Unmarshal(raw)
		if err != nil || (record.ExpiresAtUnixNano > 0 && nowUnix >= record.ExpiresAtUnixNano) {
			if err := s.deleteDataKeyAndMetadataLocked(ctx, dataKey); err != nil {
				s.observe(ctx, gocache.OperationError, dataKey, start, err)
				return removed, err
			}
			removed++
		}
	}
	s.observe(ctx, gocache.OperationDelete, "", start, nil)
	return removed, nil
}

// SetIfAbsent stores the value only if there is no live value.
func (s *Store[K, V]) SetIfAbsent(ctx context.Context, key K, value V, ttl time.Duration) (bool, error) {
	start := time.Now()
	ctx = normalizeContext(ctx)
	if err := ctx.Err(); err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return false, err
	}
	dataKey, err := s.dataKey(key)
	if err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return false, err
	}

	s.mutationLock.Lock()
	defer s.mutationLock.Unlock()

	raw, closer, err := s.db.Get([]byte(dataKey))
	if err == nil {
		existing := append([]byte(nil), raw...)
		_ = closer.Close()
		record, unmarshalErr := envelope.Unmarshal(existing)
		if unmarshalErr == nil {
			if record.ExpiresAtUnixNano == 0 || s.now().UnixNano() < record.ExpiresAtUnixNano {
				return false, nil
			}
		}
		if err := s.deleteDataKeyAndMetadataLocked(ctx, dataKey); err != nil {
			s.observe(ctx, gocache.OperationError, dataKey, start, err)
			return false, err
		}
	} else if !errors.Is(err, pebble.ErrNotFound) {
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return false, err
	}

	payload, err := s.codec.Marshal(value)
	if err != nil {
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return false, err
	}
	var expiresAt int64
	if ttl > 0 {
		expiresAt = s.now().Add(ttl).UnixNano()
	}
	encoded, err := envelope.Marshal(payload, expiresAt)
	if err != nil {
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return false, err
	}
	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()
	if err := batch.Set([]byte(dataKey), encoded, nil); err != nil {
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return false, err
	}
	if err := s.setLogicalIndexInBatch(batch, key, dataKey); err != nil {
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return false, err
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return false, err
	}
	s.observe(ctx, gocache.OperationSet, dataKey, start, nil)
	return true, nil
}

// InvalidateKeys deletes a batch of keys.
func (s *Store[K, V]) InvalidateKeys(ctx context.Context, keys []K) error {
	start := time.Now()
	ctx = normalizeContext(ctx)
	if err := ctx.Err(); err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return err
	}
	s.mutationLock.Lock()
	defer s.mutationLock.Unlock()
	for _, key := range keys {
		dataKey, err := s.dataKey(key)
		if err != nil {
			s.observe(ctx, gocache.OperationError, "", start, err)
			return err
		}
		if err := s.deleteDataKeyAndMetadataLocked(ctx, dataKey); err != nil {
			s.observe(ctx, gocache.OperationError, dataKey, start, err)
			return err
		}
	}
	s.observe(ctx, gocache.OperationDelete, "", start, nil)
	return nil
}

// DeleteByKeyPrefix deletes keys matching typed logical-key prefix.
func (s *Store[K, V]) DeleteByKeyPrefix(ctx context.Context, prefix K) error {
	logicalPrefix, ok := logicalStringKey(prefix)
	if !ok {
		return gocache.ErrLogicalPrefixUnsupported
	}
	return s.deleteByLogicalPrefix(ctx, logicalPrefix)
}

// DeleteByPrefix deletes keys matching logical-key prefix.
//
// Deprecated: prefer DeleteByKeyPrefix.
func (s *Store[K, V]) DeleteByPrefix(ctx context.Context, prefix string) error {
	return s.deleteByLogicalPrefix(ctx, prefix)
}

func (s *Store[K, V]) deleteByLogicalPrefix(ctx context.Context, prefix string) error {
	start := time.Now()
	ctx = normalizeContext(ctx)
	if err := ctx.Err(); err != nil {
		s.observe(ctx, gocache.OperationError, prefix, start, err)
		return err
	}
	if !keyTypeIsString[K]() {
		err := gocache.ErrLogicalPrefixUnsupported
		s.observe(ctx, gocache.OperationError, prefix, start, err)
		return err
	}
	s.mutationLock.Lock()
	defer s.mutationLock.Unlock()
	keys, err := s.collectDataKeysByLogicalPrefix(ctx, prefix)
	if err != nil {
		s.observe(ctx, gocache.OperationError, prefix, start, err)
		return err
	}
	for _, key := range keys {
		if err := s.deleteDataKeyAndMetadataLocked(ctx, key); err != nil {
			s.observe(ctx, gocache.OperationError, key, start, err)
			return err
		}
	}
	s.observe(ctx, gocache.OperationDelete, prefix, start, nil)
	return nil
}

// AddTagsForKey associates tags with typed logical key.
func (s *Store[K, V]) AddTagsForKey(ctx context.Context, key K, tags []string) error {
	dataKey, err := s.dataKey(key)
	if err != nil {
		return err
	}
	return s.addTagsByDataKey(ctx, dataKey, tags)
}

// AddTags associates tags with key.
//
// Deprecated: prefer AddTagsForKey.
func (s *Store[K, V]) AddTags(ctx context.Context, key string, tags []string) error {
	return s.addTagsByDataKey(ctx, s.dataKeyFromLogical(key), tags)
}

func (s *Store[K, V]) addTagsByDataKey(ctx context.Context, dataKey string, tags []string) error {
	start := time.Now()
	ctx = normalizeContext(ctx)
	if err := ctx.Err(); err != nil {
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return err
	}
	if dataKey == "" || len(tags) == 0 {
		return nil
	}

	_, closer, err := s.db.Get([]byte(dataKey))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil
		}
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return err
	}
	_ = closer.Close()

	s.mutationLock.Lock()
	defer s.mutationLock.Unlock()

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()
	encodedDataKey := encodeToken(dataKey)
	for _, tag := range tags {
		tag = strings.TrimSpace(tag)
		if tag == "" {
			continue
		}
		tagEncoded := encodeToken(tag)
		if err := batch.Set([]byte(s.keyToTagKeyEncoded(encodedDataKey, tagEncoded)), nil, nil); err != nil {
			s.observe(ctx, gocache.OperationError, dataKey, start, err)
			return err
		}
		if err := batch.Set([]byte(s.tagToKeyKeyEncoded(tagEncoded, encodedDataKey)), nil, nil); err != nil {
			s.observe(ctx, gocache.OperationError, dataKey, start, err)
			return err
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return err
	}
	return nil
}

// InvalidateTags deletes all keys associated with provided tags.
func (s *Store[K, V]) InvalidateTags(ctx context.Context, tags []string) error {
	start := time.Now()
	ctx = normalizeContext(ctx)
	if err := ctx.Err(); err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return err
	}
	if len(tags) == 0 {
		return nil
	}

	s.mutationLock.Lock()
	defer s.mutationLock.Unlock()

	dataKeys := make(map[string]struct{})
	for _, tag := range tags {
		tag = strings.TrimSpace(tag)
		if tag == "" {
			continue
		}
		tagEncoded := encodeToken(tag)
		markers, err := s.collectKeysByPrefix(ctx, s.tagToKeyPrefixEncoded(tagEncoded))
		if err != nil {
			s.observe(ctx, gocache.OperationError, "", start, err)
			return err
		}
		for _, marker := range markers {
			_, encodedData, ok := s.parseTagToKey(marker)
			if !ok {
				continue
			}
			dataKey, decodeErr := decodeToken(encodedData)
			if decodeErr != nil {
				continue
			}
			dataKeys[dataKey] = struct{}{}
		}
	}

	for dataKey := range dataKeys {
		if err := s.deleteDataKeyAndMetadataLocked(ctx, dataKey); err != nil {
			s.observe(ctx, gocache.OperationError, dataKey, start, err)
			return err
		}
	}
	s.observe(ctx, gocache.OperationDelete, "", start, nil)
	return nil
}

func (s *Store[K, V]) dataKey(key K) (string, error) {
	encoded, err := s.keyEncoder.EncodeKey(key)
	if err != nil {
		return "", err
	}
	return s.dataKeyFromLogical(encoded), nil
}

// EncodeKey returns the deterministic encoded logical key used by this backend.
func (s *Store[K, V]) EncodeKey(key K) (string, error) {
	return s.keyEncoder.EncodeKey(key)
}

func (s *Store[K, V]) dataKeyFromLogical(logical string) string {
	return s.dataPrefix() + logical
}

func (s *Store[K, V]) namespacePrefix() string {
	return s.namespace + "|"
}

func (s *Store[K, V]) dataPrefix() string {
	return s.namespace + "|d|"
}

func (s *Store[K, V]) logicalToDataPrefix(logicalPrefix string) string {
	return s.namespace + "|l2d|" + logicalPrefix
}

func (s *Store[K, V]) logicalToDataKey(logical, dataKey string) string {
	return s.logicalToDataPrefix(logical) + "|" + encodeToken(dataKey)
}

func (s *Store[K, V]) dataToLogicalKey(dataKey string) string {
	return s.namespace + "|d2l|" + encodeToken(dataKey)
}

func (s *Store[K, V]) keyToTagPrefixEncoded(encodedDataKey string) string {
	return s.namespace + "|k2t|" + encodedDataKey + "|"
}

func (s *Store[K, V]) keyToTagKeyEncoded(encodedDataKey, encodedTag string) string {
	return s.keyToTagPrefixEncoded(encodedDataKey) + encodedTag
}

func (s *Store[K, V]) tagToKeyPrefixEncoded(encodedTag string) string {
	return s.namespace + "|t2k|" + encodedTag + "|"
}

func (s *Store[K, V]) tagToKeyKeyEncoded(encodedTag, encodedDataKey string) string {
	return s.tagToKeyPrefixEncoded(encodedTag) + encodedDataKey
}

func (s *Store[K, V]) parseKeyToTag(marker string) (encodedData, encodedTag string, ok bool) {
	prefix := s.namespace + "|k2t|"
	rest, ok := strings.CutPrefix(marker, prefix)
	if !ok {
		return "", "", false
	}
	encodedData, encodedTag, ok = strings.Cut(rest, "|")
	return encodedData, encodedTag, ok
}

func (s *Store[K, V]) parseTagToKey(marker string) (encodedTag, encodedData string, ok bool) {
	prefix := s.namespace + "|t2k|"
	rest, ok := strings.CutPrefix(marker, prefix)
	if !ok {
		return "", "", false
	}
	encodedTag, encodedData, ok = strings.Cut(rest, "|")
	return encodedTag, encodedData, ok
}

func (s *Store[K, V]) collectKeysByPrefix(ctx context.Context, prefix string) ([]string, error) {
	lower := []byte(prefix)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: prefixUpperBound(lower),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	keys := make([]string, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		keys = append(keys, string(iter.Key()))
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return keys, nil
}

func (s *Store[K, V]) collectDataKeysByLogicalPrefix(ctx context.Context, prefix string) ([]string, error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(s.logicalToDataPrefix(prefix)),
		UpperBound: prefixUpperBound([]byte(s.logicalToDataPrefix(prefix))),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	keys := make(map[string]struct{})
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		dataKey := string(append([]byte(nil), iter.Value()...))
		if dataKey == "" {
			continue
		}
		keys[dataKey] = struct{}{}
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}

	out := make([]string, 0, len(keys))
	for dataKey := range keys {
		out = append(out, dataKey)
	}
	return out, nil
}

func (s *Store[K, V]) setLogicalIndexInBatch(batch *pebble.Batch, key K, dataKey string) error {
	logical, ok := logicalStringKey(key)
	if !ok {
		return batch.Delete([]byte(s.dataToLogicalKey(dataKey)), nil)
	}
	if err := batch.Set([]byte(s.dataToLogicalKey(dataKey)), []byte(logical), nil); err != nil {
		return err
	}
	return batch.Set([]byte(s.logicalToDataKey(logical, dataKey)), []byte(dataKey), nil)
}

func (s *Store[K, V]) logicalByDataKey(dataKey string) (string, bool, error) {
	raw, closer, err := s.db.Get([]byte(s.dataToLogicalKey(dataKey)))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return "", false, nil
		}
		return "", false, err
	}
	defer func() { _ = closer.Close() }()
	return string(append([]byte(nil), raw...)), true, nil
}

func (s *Store[K, V]) deleteDataKeyAndMetadataLocked(ctx context.Context, dataKey string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()

	if err := batch.Delete([]byte(dataKey), nil); err != nil {
		return err
	}
	if err := batch.Delete([]byte(s.dataToLogicalKey(dataKey)), nil); err != nil {
		return err
	}

	encodedDataKey := encodeToken(dataKey)
	keyTagMarkers, err := s.collectKeysByPrefix(ctx, s.keyToTagPrefixEncoded(encodedDataKey))
	if err != nil {
		return err
	}

	for _, marker := range keyTagMarkers {
		_, encodedTag, ok := s.parseKeyToTag(marker)
		if !ok {
			continue
		}
		if err := batch.Delete([]byte(marker), nil); err != nil {
			return err
		}
		if err := batch.Delete([]byte(s.tagToKeyKeyEncoded(encodedTag, encodedDataKey)), nil); err != nil {
			return err
		}
	}
	logical, ok, err := s.logicalByDataKey(dataKey)
	if err != nil {
		return err
	}
	if ok {
		if err := batch.Delete([]byte(s.logicalToDataKey(logical, dataKey)), nil); err != nil {
			return err
		}
	}

	return batch.Commit(pebble.Sync)
}

func (s *Store[K, V]) observe(ctx context.Context, operation gocache.Operation, key string, start time.Time, err error) {
	gocache.Observe(ctx, s.observer, gocache.Observation{
		Backend:   backendName,
		Operation: operation,
		Key:       key,
		Err:       err,
		Latency:   time.Since(start),
	})
}

func normalizeContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func keyTypeIsString[K comparable]() bool {
	var zero K
	typ := reflect.TypeOf(zero)
	return typ != nil && typ.Kind() == reflect.String
}

func logicalStringKey[K comparable](key K) (string, bool) {
	value := reflect.ValueOf(key)
	if !value.IsValid() || value.Kind() != reflect.String {
		return "", false
	}
	return value.String(), true
}

func encodeToken(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}

func decodeToken(value string) (string, error) {
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func prefixUpperBound(prefix []byte) []byte {
	out := append([]byte(nil), prefix...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] < 0xff {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}
