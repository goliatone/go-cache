package memory

import (
	"container/list"
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"time"

	gocache "github.com/goliatone/go-cache"
	"github.com/goliatone/go-cache/codec"
	"github.com/goliatone/go-cache/internal/envelope"
)

const (
	backendName = "memory"
)

var (
	errNilClock          = errors.New("memory: nil clock func")
	errInvalidMaxEntries = errors.New("memory: max entries must be positive")
)

// Option configures a memory store.
type Option[K comparable, V any] func(*Store[K, V]) error

// Store is an in-memory cache backend with TTL, tagging, and invalidation capabilities.
type Store[K comparable, V any] struct {
	mu               sync.RWMutex
	entries          map[string][]byte
	logicalByStorage map[string]string
	tagsByKey        map[string]map[string]struct{}
	keysByTag        map[string]map[string]struct{}
	recency          *list.List
	recencyByStorage map[string]*list.Element
	maxEntries       int

	codec      codec.Codec[V]
	keyEncoder gocache.KeyEncoder[K]
	observer   gocache.Observer
	now        func() time.Time
}

type recencyEntry struct {
	storageKey string
}

// NewStore creates a new in-memory cache store.
func NewStore[K comparable, V any](opts ...Option[K, V]) (*Store[K, V], error) {
	store := &Store[K, V]{
		entries:          make(map[string][]byte),
		logicalByStorage: make(map[string]string),
		tagsByKey:        make(map[string]map[string]struct{}),
		keysByTag:        make(map[string]map[string]struct{}),
		codec:            codec.NewJSONCodec[V](),
		observer:         gocache.NopObserver(),
		now:              time.Now,
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
	return store, nil
}

// WithCodec sets the value codec used by the store.
func WithCodec[K comparable, V any](c codec.Codec[V]) Option[K, V] {
	return func(store *Store[K, V]) error {
		if c == nil {
			return errors.New("memory: nil codec")
		}
		store.codec = c
		return nil
	}
}

// WithKeyEncoder sets a key encoder used for storage keys.
func WithKeyEncoder[K comparable, V any](encoder gocache.KeyEncoder[K]) Option[K, V] {
	return func(store *Store[K, V]) error {
		store.keyEncoder = encoder
		return nil
	}
}

// WithObserver sets an observer for instrumentation events.
func WithObserver[K comparable, V any](observer gocache.Observer) Option[K, V] {
	return func(store *Store[K, V]) error {
		store.observer = observer
		return nil
	}
}

// WithClock injects a clock function, mainly for tests.
func WithClock[K comparable, V any](clock func() time.Time) Option[K, V] {
	return func(store *Store[K, V]) error {
		if clock == nil {
			return errNilClock
		}
		store.now = clock
		return nil
	}
}

// WithMaxEntries sets a hard positive entry limit. When the store is full,
// expired entries are removed first and then the least recently used live
// entry is evicted before admitting a new key.
func WithMaxEntries[K comparable, V any](maxEntries int) Option[K, V] {
	return func(store *Store[K, V]) error {
		if maxEntries <= 0 {
			return errInvalidMaxEntries
		}
		store.maxEntries = maxEntries
		store.recency = list.New()
		store.recencyByStorage = make(map[string]*list.Element, maxEntries)
		return nil
	}
}

// EncodeKey encodes key K into the store's storage key format.
func (s *Store[K, V]) EncodeKey(key K) (string, error) {
	return s.keyEncoder.EncodeKey(key)
}

func (s *Store[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	var zero V
	start := time.Now()
	ctx = normalizeContext(ctx)
	if err := ctx.Err(); err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return zero, false, err
	}

	storageKey, err := s.EncodeKey(key)
	if err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return zero, false, err
	}

	bounded := s.maxEntries > 0
	var raw []byte
	var ok bool
	if bounded {
		s.mu.Lock()
		raw, ok = s.entries[storageKey]
	} else {
		s.mu.RLock()
		raw, ok = s.entries[storageKey]
		s.mu.RUnlock()
	}
	if !ok {
		if bounded {
			s.mu.Unlock()
		}
		s.observe(ctx, gocache.OperationGetMiss, storageKey, start, nil)
		return zero, false, nil
	}

	record, err := envelope.Unmarshal(raw)
	if err != nil {
		s.deleteAfterRead(storageKey, bounded)
		s.observe(ctx, gocache.OperationError, storageKey, start, err)
		return zero, false, err
	}

	nowUnix := s.now().UnixNano()
	if record.ExpiresAtUnixNano > 0 && nowUnix >= record.ExpiresAtUnixNano {
		s.deleteAfterRead(storageKey, bounded)
		s.observe(ctx, gocache.OperationGetMiss, storageKey, start, nil)
		return zero, false, nil
	}
	if bounded {
		s.touchStorageKeyLocked(storageKey)
		s.mu.Unlock()
	}

	value, err := s.codec.Unmarshal(record.Payload)
	if err != nil {
		s.mu.Lock()
		s.deleteStorageKeyLocked(storageKey)
		s.mu.Unlock()
		s.observe(ctx, gocache.OperationError, storageKey, start, err)
		return zero, false, err
	}

	s.observe(ctx, gocache.OperationGetHit, storageKey, start, nil)
	return value, true, nil
}

func (s *Store[K, V]) Set(ctx context.Context, key K, value V, ttl time.Duration) error {
	start := time.Now()
	ctx = normalizeContext(ctx)
	if err := ctx.Err(); err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return err
	}

	storageKey, err := s.EncodeKey(key)
	if err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return err
	}

	payload, err := s.codec.Marshal(value)
	if err != nil {
		s.observe(ctx, gocache.OperationError, storageKey, start, err)
		return err
	}

	var expiresAt int64
	if ttl > 0 {
		expiresAt = s.now().Add(ttl).UnixNano()
	}
	encoded, err := envelope.Marshal(payload, expiresAt)
	if err != nil {
		s.observe(ctx, gocache.OperationError, storageKey, start, err)
		return err
	}

	s.mu.Lock()
	evicted := 0
	if _, exists := s.entries[storageKey]; !exists {
		evicted = s.prepareAdmissionLocked(s.now().UnixNano())
	}
	s.entries[storageKey] = encoded
	if logical, ok := logicalStringKey(key); ok {
		s.logicalByStorage[storageKey] = logical
	} else {
		delete(s.logicalByStorage, storageKey)
	}
	s.touchStorageKeyLocked(storageKey)
	s.mu.Unlock()

	if evicted > 0 {
		s.observeCount(ctx, gocache.OperationEvict, start, nil, evicted)
	}
	s.observe(ctx, gocache.OperationSet, storageKey, start, nil)
	return nil
}

// SetIfPresent replaces a live entry without creating a missing or expired key.
func (s *Store[K, V]) SetIfPresent(ctx context.Context, key K, value V, ttl time.Duration) (bool, error) {
	start := time.Now()
	ctx = normalizeContext(ctx)
	if err := ctx.Err(); err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return false, err
	}

	storageKey, err := s.EncodeKey(key)
	if err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return false, err
	}
	payload, err := s.codec.Marshal(value)
	if err != nil {
		s.observe(ctx, gocache.OperationError, storageKey, start, err)
		return false, err
	}
	var expiresAt int64
	if ttl > 0 {
		expiresAt = s.now().Add(ttl).UnixNano()
	}
	encoded, err := envelope.Marshal(payload, expiresAt)
	if err != nil {
		s.observe(ctx, gocache.OperationError, storageKey, start, err)
		return false, err
	}

	nowUnix := s.now().UnixNano()
	updated, err := s.replaceLiveEncoded(ctx, storageKey, encoded, nowUnix)
	if err != nil {
		s.observe(ctx, gocache.OperationError, storageKey, start, err)
		return false, err
	}
	if !updated {
		return false, nil
	}

	s.observe(ctx, gocache.OperationSet, storageKey, start, nil)
	return true, nil
}

func (s *Store[K, V]) replaceLiveEncoded(ctx context.Context, storageKey string, encoded []byte, nowUnix int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	raw, ok := s.entries[storageKey]
	if !ok {
		return false, nil
	}
	record, err := envelope.Unmarshal(raw)
	if err != nil {
		s.deleteStorageKeyLocked(storageKey)
		return false, err
	}
	if record.ExpiresAtUnixNano > 0 && nowUnix >= record.ExpiresAtUnixNano {
		s.deleteStorageKeyLocked(storageKey)
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.setEncodedValueLocked(storageKey, encoded)
	return true, nil
}

func (s *Store[K, V]) Delete(ctx context.Context, key K) error {
	start := time.Now()
	ctx = normalizeContext(ctx)
	if err := ctx.Err(); err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return err
	}

	storageKey, err := s.EncodeKey(key)
	if err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return err
	}

	s.mu.Lock()
	s.deleteStorageKeyLocked(storageKey)
	s.mu.Unlock()

	s.observe(ctx, gocache.OperationDelete, storageKey, start, nil)
	return nil
}

// Clear removes all keys and tag indices.
func (s *Store[K, V]) Clear(ctx context.Context) error {
	start := time.Now()
	ctx = normalizeContext(ctx)
	if err := ctx.Err(); err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return err
	}
	s.mu.Lock()
	removed := len(s.entries)
	s.entries = make(map[string][]byte)
	s.logicalByStorage = make(map[string]string)
	s.tagsByKey = make(map[string]map[string]struct{})
	s.keysByTag = make(map[string]map[string]struct{})
	if s.maxEntries > 0 {
		s.recency = list.New()
		s.recencyByStorage = make(map[string]*list.Element, s.maxEntries)
	}
	s.mu.Unlock()
	s.observeCount(ctx, gocache.OperationDelete, start, nil, removed)
	return nil
}

// PurgeExpired removes expired entries and returns removed key count.
func (s *Store[K, V]) PurgeExpired(ctx context.Context) (int, error) {
	start := time.Now()
	ctx = normalizeContext(ctx)
	if err := ctx.Err(); err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return 0, err
	}

	nowUnix := s.now().UnixNano()
	removed := 0

	s.mu.Lock()
	for key, raw := range s.entries {
		record, err := envelope.Unmarshal(raw)
		if err != nil || (record.ExpiresAtUnixNano > 0 && nowUnix >= record.ExpiresAtUnixNano) {
			s.deleteStorageKeyLocked(key)
			removed++
		}
	}
	s.mu.Unlock()
	s.observeCount(ctx, gocache.OperationDelete, start, nil, removed)
	return removed, nil
}

// SetIfAbsent stores a key only when no non-expired value exists.
func (s *Store[K, V]) SetIfAbsent(ctx context.Context, key K, value V, ttl time.Duration) (bool, error) {
	start := time.Now()
	ctx = normalizeContext(ctx)
	if err := ctx.Err(); err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return false, err
	}

	storageKey, err := s.EncodeKey(key)
	if err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return false, err
	}
	payload, err := s.codec.Marshal(value)
	if err != nil {
		s.observe(ctx, gocache.OperationError, storageKey, start, err)
		return false, err
	}
	var expiresAt int64
	if ttl > 0 {
		expiresAt = s.now().Add(ttl).UnixNano()
	}
	encoded, err := envelope.Marshal(payload, expiresAt)
	if err != nil {
		s.observe(ctx, gocache.OperationError, storageKey, start, err)
		return false, err
	}

	nowUnix := s.now().UnixNano()

	s.mu.Lock()
	evicted := 0
	if raw, ok := s.entries[storageKey]; ok {
		record, unmarshalErr := envelope.Unmarshal(raw)
		if unmarshalErr == nil && (record.ExpiresAtUnixNano == 0 || nowUnix < record.ExpiresAtUnixNano) {
			s.mu.Unlock()
			return false, nil
		}
		s.deleteStorageKeyLocked(storageKey)
	}
	if _, exists := s.entries[storageKey]; !exists {
		evicted = s.prepareAdmissionLocked(nowUnix)
	}

	s.entries[storageKey] = encoded
	if logical, ok := logicalStringKey(key); ok {
		s.logicalByStorage[storageKey] = logical
	} else {
		delete(s.logicalByStorage, storageKey)
	}
	s.touchStorageKeyLocked(storageKey)
	s.mu.Unlock()
	if evicted > 0 {
		s.observeCount(ctx, gocache.OperationEvict, start, nil, evicted)
	}
	s.observe(ctx, gocache.OperationSet, storageKey, start, nil)
	return true, nil
}

// InvalidateKeys removes all provided keys.
func (s *Store[K, V]) InvalidateKeys(ctx context.Context, keys []K) error {
	start := time.Now()
	ctx = normalizeContext(ctx)
	if err := ctx.Err(); err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return err
	}
	storageKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		storageKey, err := s.EncodeKey(key)
		if err != nil {
			s.observe(ctx, gocache.OperationError, "", start, err)
			return err
		}
		storageKeys = append(storageKeys, storageKey)
	}

	s.mu.Lock()
	for _, storageKey := range storageKeys {
		s.deleteStorageKeyLocked(storageKey)
	}
	s.mu.Unlock()

	s.observe(ctx, gocache.OperationDelete, "", start, nil)
	return nil
}

// DeleteByKeyPrefix removes all entries where logical key has the provided prefix.
func (s *Store[K, V]) DeleteByKeyPrefix(ctx context.Context, prefix K) error {
	logicalPrefix, ok := logicalStringKey(prefix)
	if !ok {
		return gocache.ErrLogicalPrefixUnsupported
	}
	return s.deleteByLogicalPrefix(ctx, logicalPrefix)
}

// DeleteByPrefix removes all entries where logical key has the provided prefix.
//
// Deprecated: prefer DeleteByKeyPrefix.
func (s *Store[K, V]) DeleteByPrefix(ctx context.Context, prefix string) error {
	return s.deleteByLogicalPrefix(ctx, prefix)
}

func (s *Store[K, V]) deleteByLogicalPrefix(ctx context.Context, prefix string) error {
	start := time.Now()
	ctx = normalizeContext(ctx)
	if err := ctx.Err(); err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return err
	}
	if !keyTypeIsString[K]() {
		err := gocache.ErrLogicalPrefixUnsupported
		s.observe(ctx, gocache.OperationError, prefix, start, err)
		return err
	}

	s.mu.Lock()
	for storageKey, logical := range s.logicalByStorage {
		if strings.HasPrefix(logical, prefix) {
			s.deleteStorageKeyLocked(storageKey)
		}
	}
	s.mu.Unlock()

	s.observe(ctx, gocache.OperationDelete, prefix, start, nil)
	return nil
}

// AddTagsForKey associates tags with a typed logical key.
func (s *Store[K, V]) AddTagsForKey(ctx context.Context, key K, tags []string) error {
	storageKey, err := s.EncodeKey(key)
	if err != nil {
		return err
	}
	return s.addTagsByStorageKey(ctx, storageKey, tags)
}

// AddTags associates tags with a storage key.
//
// Deprecated: prefer AddTagsForKey.
func (s *Store[K, V]) AddTags(ctx context.Context, key string, tags []string) error {
	return s.addTagsByStorageKey(ctx, key, tags)
}

func (s *Store[K, V]) addTagsByStorageKey(ctx context.Context, key string, tags []string) error {
	start := time.Now()
	ctx = normalizeContext(ctx)
	if err := ctx.Err(); err != nil {
		s.observe(ctx, gocache.OperationError, key, start, err)
		return err
	}
	if key == "" || len(tags) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.entries[key]; !exists {
		return nil
	}

	keyTags := s.tagsByKey[key]
	if keyTags == nil {
		keyTags = make(map[string]struct{})
		s.tagsByKey[key] = keyTags
	}

	for _, tag := range tags {
		if tag == "" {
			continue
		}
		keyTags[tag] = struct{}{}
		keys := s.keysByTag[tag]
		if keys == nil {
			keys = make(map[string]struct{})
			s.keysByTag[tag] = keys
		}
		keys[key] = struct{}{}
	}
	return nil
}

// InvalidateTags removes all keys associated with any tag in the list.
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

	keysToDelete := make(map[string]struct{})

	s.mu.Lock()
	for _, tag := range tags {
		keys := s.keysByTag[tag]
		for key := range keys {
			keysToDelete[key] = struct{}{}
		}
	}
	for key := range keysToDelete {
		s.deleteStorageKeyLocked(key)
	}
	for _, tag := range tags {
		delete(s.keysByTag, tag)
	}
	s.mu.Unlock()
	s.observe(ctx, gocache.OperationDelete, "", start, nil)
	return nil
}

func (s *Store[K, V]) observe(ctx context.Context, operation gocache.Operation, key string, start time.Time, err error) {
	s.observeCount(ctx, operation, start, err, 0, key)
}

func (s *Store[K, V]) observeCount(ctx context.Context, operation gocache.Operation, start time.Time, err error, count int, keys ...string) {
	key := ""
	if len(keys) > 0 {
		key = keys[0]
	}
	occupancy := 0
	capacity := s.maxEntries
	if capacity > 0 {
		s.mu.RLock()
		occupancy = len(s.entries)
		s.mu.RUnlock()
	}
	obs := gocache.Observation{
		Backend:   backendName,
		Operation: operation,
		Key:       key,
		Err:       err,
		Latency:   time.Since(start),
		Count:     count,
		Occupancy: occupancy,
		Capacity:  capacity,
	}
	gocache.Observe(ctx, s.observer, obs)
}

func (s *Store[K, V]) deleteStorageKeyLocked(storageKey string) {
	delete(s.entries, storageKey)
	if element := s.recencyByStorage[storageKey]; element != nil {
		s.recency.Remove(element)
		delete(s.recencyByStorage, storageKey)
	}
	delete(s.logicalByStorage, storageKey)
	tags := s.tagsByKey[storageKey]
	for tag := range tags {
		keys := s.keysByTag[tag]
		delete(keys, storageKey)
		if len(keys) == 0 {
			delete(s.keysByTag, tag)
		}
	}
	delete(s.tagsByKey, storageKey)
}

func (s *Store[K, V]) deleteAfterRead(storageKey string, bounded bool) {
	if !bounded {
		s.mu.Lock()
	}
	s.deleteStorageKeyLocked(storageKey)
	s.mu.Unlock()
}

func (s *Store[K, V]) touchStorageKeyLocked(storageKey string) {
	if s.maxEntries == 0 {
		return
	}
	if element := s.recencyByStorage[storageKey]; element != nil {
		s.recency.MoveToFront(element)
		return
	}
	s.recencyByStorage[storageKey] = s.recency.PushFront(recencyEntry{storageKey: storageKey})
}

func (s *Store[K, V]) setEncodedValueLocked(storageKey string, encoded []byte) {
	s.entries[storageKey] = encoded
	s.touchStorageKeyLocked(storageKey)
}

func (s *Store[K, V]) prepareAdmissionLocked(nowUnix int64) int {
	if s.maxEntries == 0 {
		return 0
	}
	for storageKey, raw := range s.entries {
		record, err := envelope.Unmarshal(raw)
		if err != nil || (record.ExpiresAtUnixNano > 0 && nowUnix >= record.ExpiresAtUnixNano) {
			s.deleteStorageKeyLocked(storageKey)
		}
	}
	if len(s.entries) < s.maxEntries {
		return 0
	}
	lru := s.recency.Back()
	if lru == nil {
		return 0
	}
	entry, ok := lru.Value.(recencyEntry)
	if !ok {
		return 0
	}
	s.deleteStorageKeyLocked(entry.storageKey)
	return 1
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
