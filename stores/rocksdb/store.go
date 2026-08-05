//go:build rocksdb

package rocksdb

import (
	"context"
	"encoding/base64"
	"errors"
	"reflect"
	"strings"
	"sync"
	"time"

	gocache "github.com/goliatone/go-cache"
	"github.com/goliatone/go-cache/codec"
	"github.com/goliatone/go-cache/internal/envelope"
	"github.com/linxGnu/grocksdb"
)

const (
	defaultNamespace = "gocache"
	backendName      = "rocksdb"
)

var errNoPathOrDB = errors.New("rocksdb: either path or db must be provided")

// Store implements gocache over RocksDB.
type Store[K comparable, V any] struct {
	db      *grocksdb.DB
	ownsDB  bool
	path    string
	options *grocksdb.Options
	ro      *grocksdb.ReadOptions
	wo      *grocksdb.WriteOptions

	namespace  string
	codec      codec.Codec[V]
	keyEncoder gocache.KeyEncoder[K]
	observer   gocache.Observer
	now        func() time.Time

	mu sync.Mutex
}

// NewStore creates a RocksDB store.
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

	store.ro = grocksdb.NewDefaultReadOptions()
	store.wo = grocksdb.NewDefaultWriteOptions()
	store.wo.SetSync(true)

	if store.db == nil {
		if strings.TrimSpace(store.path) == "" {
			return nil, errNoPathOrDB
		}
		store.options = grocksdb.NewDefaultOptions()
		store.options.SetCreateIfMissing(true)
		db, err := grocksdb.OpenDb(store.options, store.path)
		if err != nil {
			return nil, err
		}
		store.db = db
		store.ownsDB = true
	}
	return store, nil
}

// Close closes owned resources.
func (s *Store[K, V]) Close() error {
	if s.db != nil && s.ownsDB {
		s.db.Close()
	}
	if s.ro != nil {
		s.ro.Destroy()
	}
	if s.wo != nil {
		s.wo.Destroy()
	}
	if s.options != nil {
		s.options.Destroy()
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

	slice, err := s.db.Get(s.ro, []byte(dataKey))
	if err != nil {
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return zero, false, err
	}
	defer slice.Free()
	if !slice.Exists() {
		s.observe(ctx, gocache.OperationGetMiss, dataKey, start, nil)
		return zero, false, nil
	}
	raw := append([]byte(nil), slice.Data()...)

	record, err := envelope.Unmarshal(raw)
	if err != nil {
		s.mu.Lock()
		_ = s.deleteDataKeyAndLogicalIndex(dataKey)
		s.mu.Unlock()
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return zero, false, err
	}
	if record.ExpiresAtUnixNano > 0 && s.now().UnixNano() >= record.ExpiresAtUnixNano {
		s.mu.Lock()
		_ = s.deleteDataKeyAndLogicalIndex(dataKey)
		s.mu.Unlock()
		s.observe(ctx, gocache.OperationGetMiss, dataKey, start, nil)
		return zero, false, nil
	}
	value, err := s.codec.Unmarshal(record.Payload)
	if err != nil {
		s.mu.Lock()
		_ = s.deleteDataKeyAndLogicalIndex(dataKey)
		s.mu.Unlock()
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return zero, false, err
	}

	s.observe(ctx, gocache.OperationGetHit, dataKey, start, nil)
	return value, true, nil
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
	wb := grocksdb.NewWriteBatch()
	defer wb.Destroy()
	wb.Put([]byte(dataKey), encoded)
	if err := s.setLogicalIndexInBatch(wb, key, dataKey); err != nil {
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return err
	}
	if err := s.db.Write(s.wo, wb); err != nil {
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

	s.mu.Lock()
	defer s.mu.Unlock()
	slice, err := s.db.Get(s.ro, []byte(dataKey))
	if err != nil {
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return false, err
	}
	if !slice.Exists() {
		slice.Free()
		return false, nil
	}
	raw := append([]byte(nil), slice.Data()...)
	slice.Free()
	record, err := envelope.Unmarshal(raw)
	if err != nil {
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return false, err
	}
	if record.ExpiresAtUnixNano > 0 && s.now().UnixNano() >= record.ExpiresAtUnixNano {
		if err := s.deleteDataKeyAndLogicalIndex(dataKey); err != nil {
			s.observe(ctx, gocache.OperationError, dataKey, start, err)
			return false, err
		}
		return false, nil
	}
	if err := s.db.Put(s.wo, []byte(dataKey), encoded); err != nil {
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return false, err
	}
	s.observe(ctx, gocache.OperationSet, dataKey, start, nil)
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
	s.mu.Lock()
	err = s.deleteDataKeyAndLogicalIndex(dataKey)
	s.mu.Unlock()
	if err != nil {
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return err
	}
	s.observe(ctx, gocache.OperationDelete, dataKey, start, nil)
	return nil
}

// Clear removes all namespaced keys.
func (s *Store[K, V]) Clear(ctx context.Context) error {
	start := time.Now()
	ctx = normalizeContext(ctx)
	if err := ctx.Err(); err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	keys, err := s.collectKeysByPrefix(ctx, s.namespacePrefix())
	if err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return err
	}
	if err := s.deleteKeys(keys); err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return err
	}
	s.observe(ctx, gocache.OperationDelete, "", start, nil)
	return nil
}

// PurgeExpired removes expired records and returns removed count.
func (s *Store[K, V]) PurgeExpired(ctx context.Context) (int, error) {
	start := time.Now()
	ctx = normalizeContext(ctx)
	if err := ctx.Err(); err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	keys, err := s.collectKeysByPrefix(ctx, s.dataPrefix())
	if err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return 0, err
	}
	nowUnix := s.now().UnixNano()
	expired := make([]string, 0)
	for _, key := range keys {
		slice, err := s.db.Get(s.ro, []byte(key))
		if err != nil {
			s.observe(ctx, gocache.OperationError, key, start, err)
			return len(expired), err
		}
		if !slice.Exists() {
			slice.Free()
			continue
		}
		raw := append([]byte(nil), slice.Data()...)
		slice.Free()

		record, err := envelope.Unmarshal(raw)
		if err != nil || (record.ExpiresAtUnixNano > 0 && nowUnix >= record.ExpiresAtUnixNano) {
			expired = append(expired, key)
		}
	}
	if err := s.deleteDataKeysAndLogicalIndex(expired); err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return len(expired), err
	}
	s.observe(ctx, gocache.OperationDelete, "", start, nil)
	return len(expired), nil
}

// SetIfAbsent stores only when key is missing or expired.
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
	s.mu.Lock()
	defer s.mu.Unlock()
	slice, err := s.db.Get(s.ro, []byte(dataKey))
	if err != nil {
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return false, err
	}
	if slice.Exists() {
		raw := append([]byte(nil), slice.Data()...)
		slice.Free()
		record, err := envelope.Unmarshal(raw)
		if err == nil && (record.ExpiresAtUnixNano == 0 || s.now().UnixNano() < record.ExpiresAtUnixNano) {
			return false, nil
		}
		if err := s.deleteDataKeyAndLogicalIndex(dataKey); err != nil {
			s.observe(ctx, gocache.OperationError, dataKey, start, err)
			return false, err
		}
	} else {
		slice.Free()
	}
	if err := s.Set(ctx, key, value, ttl); err != nil {
		return false, err
	}
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
	s.mu.Lock()
	defer s.mu.Unlock()
	dataKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		dataKey, err := s.dataKey(key)
		if err != nil {
			s.observe(ctx, gocache.OperationError, "", start, err)
			return err
		}
		dataKeys = append(dataKeys, dataKey)
	}
	if err := s.deleteDataKeysAndLogicalIndex(dataKeys); err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return err
	}
	s.observe(ctx, gocache.OperationDelete, "", start, nil)
	return nil
}

// DeleteByKeyPrefix deletes logical keys by typed prefix.
func (s *Store[K, V]) DeleteByKeyPrefix(ctx context.Context, prefix K) error {
	logicalPrefix, ok := logicalStringKey(prefix)
	if !ok {
		return gocache.ErrLogicalPrefixUnsupported
	}
	return s.deleteByLogicalPrefix(ctx, logicalPrefix)
}

// DeleteByPrefix deletes logical keys by prefix.
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
	s.mu.Lock()
	defer s.mu.Unlock()
	keys, err := s.collectDataKeysByLogicalPrefix(ctx, prefix)
	if err != nil {
		s.observe(ctx, gocache.OperationError, prefix, start, err)
		return err
	}
	if err := s.deleteDataKeysAndLogicalIndex(keys); err != nil {
		s.observe(ctx, gocache.OperationError, prefix, start, err)
		return err
	}
	s.observe(ctx, gocache.OperationDelete, prefix, start, nil)
	return nil
}

func (s *Store[K, V]) dataKey(key K) (string, error) {
	encoded, err := s.keyEncoder.EncodeKey(key)
	if err != nil {
		return "", err
	}
	return s.dataPrefix() + encoded, nil
}

// EncodeKey returns the deterministic encoded logical key used by this backend.
func (s *Store[K, V]) EncodeKey(key K) (string, error) {
	return s.keyEncoder.EncodeKey(key)
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

func (s *Store[K, V]) collectKeysByPrefix(ctx context.Context, prefix string) ([]string, error) {
	it := s.db.NewIterator(s.ro)
	defer it.Close()
	keys := make([]string, 0)
	for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key := it.Key()
		keys = append(keys, string(append([]byte(nil), key.Data()...)))
		key.Free()
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	return keys, nil
}

func (s *Store[K, V]) deleteKeys(keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	wb := grocksdb.NewWriteBatch()
	defer wb.Destroy()
	for _, key := range keys {
		wb.Delete([]byte(key))
	}
	return s.db.Write(s.wo, wb)
}

func (s *Store[K, V]) collectDataKeysByLogicalPrefix(ctx context.Context, prefix string) ([]string, error) {
	it := s.db.NewIterator(s.ro)
	defer it.Close()
	keys := make(map[string]struct{})
	keyPrefix := []byte(s.logicalToDataPrefix(prefix))
	for it.Seek(keyPrefix); it.ValidForPrefix(keyPrefix); it.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		value := it.Value()
		dataKey := string(append([]byte(nil), value.Data()...))
		value.Free()
		if dataKey == "" {
			continue
		}
		keys[dataKey] = struct{}{}
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(keys))
	for key := range keys {
		out = append(out, key)
	}
	return out, nil
}

func (s *Store[K, V]) setLogicalIndexInBatch(wb *grocksdb.WriteBatch, key K, dataKey string) error {
	logical, ok := logicalStringKey(key)
	if !ok {
		wb.Delete([]byte(s.dataToLogicalKey(dataKey)))
		return nil
	}
	wb.Put([]byte(s.dataToLogicalKey(dataKey)), []byte(logical))
	wb.Put([]byte(s.logicalToDataKey(logical, dataKey)), []byte(dataKey))
	return nil
}

func (s *Store[K, V]) logicalByDataKey(dataKey string) (string, bool, error) {
	slice, err := s.db.Get(s.ro, []byte(s.dataToLogicalKey(dataKey)))
	if err != nil {
		return "", false, err
	}
	defer slice.Free()
	if !slice.Exists() {
		return "", false, nil
	}
	return string(append([]byte(nil), slice.Data()...)), true, nil
}

func (s *Store[K, V]) deleteDataKeyAndLogicalIndex(dataKey string) error {
	wb := grocksdb.NewWriteBatch()
	defer wb.Destroy()
	wb.Delete([]byte(dataKey))
	wb.Delete([]byte(s.dataToLogicalKey(dataKey)))
	logical, ok, err := s.logicalByDataKey(dataKey)
	if err != nil {
		return err
	}
	if ok {
		wb.Delete([]byte(s.logicalToDataKey(logical, dataKey)))
	}
	return s.db.Write(s.wo, wb)
}

func (s *Store[K, V]) deleteDataKeysAndLogicalIndex(keys []string) error {
	for _, key := range keys {
		if err := s.deleteDataKeyAndLogicalIndex(key); err != nil {
			return err
		}
	}
	return nil
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
