package valkey

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"time"

	gocache "github.com/goliatone/go-cache"
	"github.com/goliatone/go-cache/codec"
	"github.com/goliatone/go-cache/internal/envelope"
	vk "github.com/valkey-io/valkey-go"
)

const (
	defaultNamespace = "gocache"
	backendName      = "valkey"
)

var (
	// ErrTransientNetwork is returned when valkey operations fail with network-transient failures.
	ErrTransientNetwork = errors.New("valkey: transient network error")
)

// Store implements gocache backends over valkey.
type Store[K comparable, V any] struct {
	client       vk.Client
	ownsClient   bool
	clientOption vk.ClientOption
	addresses    []string
	namespace    string

	codec      codec.Codec[V]
	keyEncoder gocache.KeyEncoder[K]
	observer   gocache.Observer
}

// NewStore creates a valkey store.
func NewStore[K comparable, V any](opts ...Option[K, V]) (*Store[K, V], error) {
	store := &Store[K, V]{
		namespace: defaultNamespace,
		codec:     codec.NewJSONCodec[V](),
		observer:  gocache.NopObserver(),
		addresses: []string{defaultAddress},
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

	if store.client == nil {
		option := store.clientOption
		if len(option.InitAddress) == 0 {
			option.InitAddress = append([]string(nil), store.addresses...)
		}
		// Keep server compatibility broad; this backend does not rely on client-side caching.
		option.DisableCache = true
		client, err := vk.NewClient(option)
		if err != nil {
			return nil, fmt.Errorf("valkey: create client: %w", err)
		}
		store.client = client
		store.ownsClient = true
	}

	return store, nil
}

// Close closes the owned client.
func (s *Store[K, V]) Close() error {
	if s.ownsClient && s.client != nil {
		s.client.Close()
	}
	return nil
}

func (s *Store[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	start := time.Now()
	var zero V
	ctx = normalizeContext(ctx)
	if err := ctx.Err(); err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return zero, false, err
	}

	dataKey, err := s.dataKey(key)
	if err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return zero, false, err
	}

	result := s.client.Do(ctx, s.client.B().Get().Key(dataKey).Build())
	raw, err := result.AsBytes()
	if err != nil {
		if vk.IsValkeyNil(err) {
			s.observe(ctx, gocache.OperationGetMiss, dataKey, start, nil)
			return zero, false, nil
		}
		err = mapError(err)
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return zero, false, err
	}

	record, err := envelope.Unmarshal(raw)
	if err != nil {
		_ = s.removeDataKeyAndMetadata(ctx, dataKey)
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return zero, false, err
	}
	if record.ExpiresAtUnixNano > 0 && time.Now().UnixNano() >= record.ExpiresAtUnixNano {
		_ = s.removeDataKeyAndMetadata(ctx, dataKey)
		s.observe(ctx, gocache.OperationGetMiss, dataKey, start, nil)
		return zero, false, nil
	}

	value, err := s.codec.Unmarshal(record.Payload)
	if err != nil {
		_ = s.removeDataKeyAndMetadata(ctx, dataKey)
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
		expiresAt = time.Now().Add(ttl).UnixNano()
	}
	encoded, err := envelope.Marshal(payload, expiresAt)
	if err != nil {
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return err
	}

	var setErr error
	if ttl > 0 {
		setErr = s.client.Do(ctx, s.client.B().Set().Key(dataKey).Value(vk.BinaryString(encoded)).PxMilliseconds(maxInt64(ttl.Milliseconds(), 1)).Build()).Error()
	} else {
		setErr = s.client.Do(ctx, s.client.B().Set().Key(dataKey).Value(vk.BinaryString(encoded)).Build()).Error()
	}
	if setErr != nil {
		err = mapError(setErr)
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return err
	}

	if err := s.client.Do(ctx, s.client.B().Sadd().Key(s.keysIndexKey()).Member(dataKey).Build()).Error(); err != nil {
		err = mapError(err)
		if rollbackErr := s.removeDataKeyAndMetadata(ctx, dataKey); rollbackErr != nil {
			err = errors.Join(err, fmt.Errorf("valkey: rollback after index update failure: %w", mapError(rollbackErr)))
		}
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return err
	}
	if err := s.addLogicalIndex(ctx, key, dataKey); err != nil {
		err = mapError(err)
		if rollbackErr := s.removeDataKeyAndMetadata(ctx, dataKey); rollbackErr != nil {
			err = errors.Join(err, fmt.Errorf("valkey: rollback after logical index update failure: %w", mapError(rollbackErr)))
		}
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return err
	}

	s.observe(ctx, gocache.OperationSet, dataKey, start, nil)
	return nil
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
	if err := s.removeDataKeyAndMetadata(ctx, dataKey); err != nil {
		err = mapError(err)
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return err
	}
	s.observe(ctx, gocache.OperationDelete, dataKey, start, nil)
	return nil
}

// Clear deletes all namespaced cache keys and indices.
func (s *Store[K, V]) Clear(ctx context.Context) error {
	start := time.Now()
	ctx = normalizeContext(ctx)
	if err := ctx.Err(); err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return err
	}

	keys, err := s.scanDataKeys(ctx, s.dataPrefix()+"*")
	if err != nil {
		err = mapError(err)
		s.observe(ctx, gocache.OperationError, "", start, err)
		return err
	}
	for _, key := range keys {
		if err := s.removeDataKeyAndMetadata(ctx, key); err != nil {
			err = mapError(err)
			s.observe(ctx, gocache.OperationError, key, start, err)
			return err
		}
	}
	metadataKeys, err := s.scanKeys(ctx, s.namespace+":__*")
	if err != nil {
		err = mapError(err)
		s.observe(ctx, gocache.OperationError, "", start, err)
		return err
	}
	for _, key := range metadataKeys {
		if err := s.client.Do(ctx, s.client.B().Del().Key(key).Build()).Error(); err != nil && !vk.IsValkeyNil(err) {
			err = mapError(err)
			s.observe(ctx, gocache.OperationError, key, start, err)
			return err
		}
	}
	s.observe(ctx, gocache.OperationDelete, "", start, nil)
	return nil
}

// InvalidateKeys deletes the provided keys.
func (s *Store[K, V]) InvalidateKeys(ctx context.Context, keys []K) error {
	start := time.Now()
	ctx = normalizeContext(ctx)
	if err := ctx.Err(); err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return err
	}
	for _, key := range keys {
		dataKey, err := s.dataKey(key)
		if err != nil {
			s.observe(ctx, gocache.OperationError, "", start, err)
			return err
		}
		if err := s.removeDataKeyAndMetadata(ctx, dataKey); err != nil {
			err = mapError(err)
			s.observe(ctx, gocache.OperationError, dataKey, start, err)
			return err
		}
	}
	s.observe(ctx, gocache.OperationDelete, "", start, nil)
	return nil
}

// DeleteByKeyPrefix deletes data keys by typed logical-key prefix.
func (s *Store[K, V]) DeleteByKeyPrefix(ctx context.Context, prefix K) error {
	logicalPrefix, ok := logicalStringKey(prefix)
	if !ok {
		return gocache.ErrLogicalPrefixUnsupported
	}
	return s.deleteByLogicalPrefix(ctx, logicalPrefix)
}

// DeleteByPrefix deletes data keys with matching logical-key prefix.
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

	keys, err := s.scanKeys(ctx, s.logicalSetPrefix()+escapeMatchLiteral(prefix)+"*")
	if err != nil {
		err = mapError(err)
		s.observe(ctx, gocache.OperationError, prefix, start, err)
		return err
	}
	for _, logicalSetKey := range keys {
		dataKeys, err := s.client.Do(ctx, s.client.B().Smembers().Key(logicalSetKey).Build()).AsStrSlice()
		if err != nil && !vk.IsValkeyNil(err) {
			err = mapError(err)
			s.observe(ctx, gocache.OperationError, logicalSetKey, start, err)
			return err
		}
		for _, dataKey := range dataKeys {
			if err := s.removeDataKeyAndMetadata(ctx, dataKey); err != nil {
				err = mapError(err)
				s.observe(ctx, gocache.OperationError, dataKey, start, err)
				return err
			}
		}
		if err := s.client.Do(ctx, s.client.B().Del().Key(logicalSetKey).Build()).Error(); err != nil && !vk.IsValkeyNil(err) {
			err = mapError(err)
			s.observe(ctx, gocache.OperationError, logicalSetKey, start, err)
			return err
		}
	}
	s.observe(ctx, gocache.OperationDelete, prefix, start, nil)
	return nil
}

// SetIfAbsent sets key when it does not exist.
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

	payload, err := s.codec.Marshal(value)
	if err != nil {
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return false, err
	}
	var expiresAt int64
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl).UnixNano()
	}
	encoded, err := envelope.Marshal(payload, expiresAt)
	if err != nil {
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return false, err
	}

	var result vk.ValkeyResult
	if ttl > 0 {
		result = s.client.Do(ctx, s.client.B().Set().Key(dataKey).Value(vk.BinaryString(encoded)).Nx().PxMilliseconds(maxInt64(ttl.Milliseconds(), 1)).Build())
	} else {
		result = s.client.Do(ctx, s.client.B().Set().Key(dataKey).Value(vk.BinaryString(encoded)).Nx().Build())
	}
	if err := result.Error(); err != nil {
		if vk.IsValkeyNil(err) {
			return false, nil
		}
		err = mapError(err)
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return false, err
	}

	if err := s.client.Do(ctx, s.client.B().Sadd().Key(s.keysIndexKey()).Member(dataKey).Build()).Error(); err != nil {
		err = mapError(err)
		if rollbackErr := s.removeDataKeyAndMetadata(ctx, dataKey); rollbackErr != nil {
			err = errors.Join(err, fmt.Errorf("valkey: rollback after index update failure: %w", mapError(rollbackErr)))
		}
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return false, err
	}
	if err := s.addLogicalIndex(ctx, key, dataKey); err != nil {
		err = mapError(err)
		if rollbackErr := s.removeDataKeyAndMetadata(ctx, dataKey); rollbackErr != nil {
			err = errors.Join(err, fmt.Errorf("valkey: rollback after logical index update failure: %w", mapError(rollbackErr)))
		}
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return false, err
	}

	s.observe(ctx, gocache.OperationSet, dataKey, start, nil)
	return true, nil
}

// PurgeExpired reconciles stale index entries where the underlying data key no longer exists.
func (s *Store[K, V]) PurgeExpired(ctx context.Context) (int, error) {
	start := time.Now()
	ctx = normalizeContext(ctx)
	if err := ctx.Err(); err != nil {
		s.observe(ctx, gocache.OperationError, "", start, err)
		return 0, err
	}

	dataKeys, err := s.client.Do(ctx, s.client.B().Smembers().Key(s.keysIndexKey()).Build()).AsStrSlice()
	if err != nil {
		if vk.IsValkeyNil(err) {
			return 0, nil
		}
		err = mapError(err)
		s.observe(ctx, gocache.OperationError, "", start, err)
		return 0, err
	}

	removed := 0
	nowUnix := time.Now().UnixNano()
	for _, dataKey := range dataKeys {
		result := s.client.Do(ctx, s.client.B().Get().Key(dataKey).Build())
		raw, err := result.AsBytes()
		if err != nil {
			if vk.IsValkeyNil(err) {
				if err := s.removeMetadataOnly(ctx, dataKey); err != nil {
					err = mapError(err)
					s.observe(ctx, gocache.OperationError, dataKey, start, err)
					return removed, err
				}
				removed++
				continue
			}
			err = mapError(err)
			s.observe(ctx, gocache.OperationError, dataKey, start, err)
			return removed, err
		}

		record, decodeErr := envelope.Unmarshal(raw)
		if decodeErr != nil || (record.ExpiresAtUnixNano > 0 && nowUnix >= record.ExpiresAtUnixNano) {
			if err := s.removeDataKeyAndMetadata(ctx, dataKey); err != nil {
				err = mapError(err)
				s.observe(ctx, gocache.OperationError, dataKey, start, err)
				return removed, err
			}
			removed++
		}
	}
	s.observe(ctx, gocache.OperationDelete, "", start, nil)
	return removed, nil
}

// AddTagsForKey associates tags with the provided typed logical key.
func (s *Store[K, V]) AddTagsForKey(ctx context.Context, key K, tags []string) error {
	dataKey, err := s.dataKey(key)
	if err != nil {
		return err
	}
	return s.addTagsByDataKey(ctx, dataKey, tags)
}

// AddTags associates tags with the provided key.
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
	exists, err := s.client.Do(ctx, s.client.B().Exists().Key(dataKey).Build()).AsInt64()
	if err != nil {
		err = mapError(err)
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return err
	}
	if exists == 0 {
		return nil
	}

	validTags := make([]string, 0, len(tags))
	for _, tag := range tags {
		tag = strings.TrimSpace(tag)
		if tag == "" {
			continue
		}
		validTags = append(validTags, tag)
	}
	if len(validTags) == 0 {
		return nil
	}

	for _, tag := range validTags {
		if err := s.client.Do(ctx, s.client.B().Sadd().Key(s.tagSetKey(tag)).Member(dataKey).Build()).Error(); err != nil {
			err = mapError(err)
			s.observe(ctx, gocache.OperationError, dataKey, start, err)
			return err
		}
	}
	if err := s.client.Do(ctx, s.client.B().Sadd().Key(s.keyTagsSetKey(dataKey)).Member(validTags...).Build()).Error(); err != nil {
		err = mapError(err)
		s.observe(ctx, gocache.OperationError, dataKey, start, err)
		return err
	}
	return nil
}

// InvalidateTags invalidates all keys associated with any of the provided tags.
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
	for _, tag := range tags {
		tag = strings.TrimSpace(tag)
		if tag == "" {
			continue
		}
		keys, err := s.client.Do(ctx, s.client.B().Smembers().Key(s.tagSetKey(tag)).Build()).AsStrSlice()
		if err != nil && !vk.IsValkeyNil(err) {
			err = mapError(err)
			s.observe(ctx, gocache.OperationError, "", start, err)
			return err
		}
		for _, key := range keys {
			keysToDelete[key] = struct{}{}
		}
	}

	for key := range keysToDelete {
		if err := s.removeDataKeyAndMetadata(ctx, key); err != nil {
			err = mapError(err)
			s.observe(ctx, gocache.OperationError, key, start, err)
			return err
		}
	}
	for _, tag := range tags {
		tag = strings.TrimSpace(tag)
		if tag == "" {
			continue
		}
		if err := s.client.Do(ctx, s.client.B().Del().Key(s.tagSetKey(tag)).Build()).Error(); err != nil && !vk.IsValkeyNil(err) {
			err = mapError(err)
			s.observe(ctx, gocache.OperationError, "", start, err)
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

func (s *Store[K, V]) dataPrefix() string {
	return s.namespace + ":data:"
}

func (s *Store[K, V]) keysIndexKey() string {
	return s.namespace + ":__keys"
}

func (s *Store[K, V]) logicalSetPrefix() string {
	return s.namespace + ":__logical:"
}

func (s *Store[K, V]) logicalSetKey(logical string) string {
	return s.logicalSetPrefix() + logical
}

func (s *Store[K, V]) logicalByDataKey(dataKey string) string {
	return s.namespace + ":__logical_of:" + dataKey
}

func (s *Store[K, V]) tagSetKey(tag string) string {
	return s.namespace + ":__tag:" + tag
}

func (s *Store[K, V]) keyTagsSetKey(dataKey string) string {
	return s.namespace + ":__keytags:" + dataKey
}

func (s *Store[K, V]) removeMetadataOnly(ctx context.Context, dataKey string) error {
	tags, err := s.client.Do(ctx, s.client.B().Smembers().Key(s.keyTagsSetKey(dataKey)).Build()).AsStrSlice()
	if err != nil && !vk.IsValkeyNil(err) {
		return err
	}
	for _, tag := range tags {
		if err := s.client.Do(ctx, s.client.B().Srem().Key(s.tagSetKey(tag)).Member(dataKey).Build()).Error(); err != nil && !vk.IsValkeyNil(err) {
			return err
		}
	}
	logicalRaw, err := s.client.Do(ctx, s.client.B().Get().Key(s.logicalByDataKey(dataKey)).Build()).AsBytes()
	if err != nil && !vk.IsValkeyNil(err) {
		return err
	}
	if len(logicalRaw) > 0 {
		logical := string(logicalRaw)
		if err := s.client.Do(ctx, s.client.B().Srem().Key(s.logicalSetKey(logical)).Member(dataKey).Build()).Error(); err != nil && !vk.IsValkeyNil(err) {
			return err
		}
	}
	if err := s.client.Do(ctx, s.client.B().Del().Key(s.logicalByDataKey(dataKey)).Build()).Error(); err != nil && !vk.IsValkeyNil(err) {
		return err
	}
	if err := s.client.Do(ctx, s.client.B().Del().Key(s.keyTagsSetKey(dataKey)).Build()).Error(); err != nil && !vk.IsValkeyNil(err) {
		return err
	}
	if err := s.client.Do(ctx, s.client.B().Srem().Key(s.keysIndexKey()).Member(dataKey).Build()).Error(); err != nil && !vk.IsValkeyNil(err) {
		return err
	}
	return nil
}

func (s *Store[K, V]) removeDataKeyAndMetadata(ctx context.Context, dataKey string) error {
	if err := s.client.Do(ctx, s.client.B().Del().Key(dataKey).Build()).Error(); err != nil && !vk.IsValkeyNil(err) {
		return err
	}
	return s.removeMetadataOnly(ctx, dataKey)
}

func (s *Store[K, V]) scanDataKeys(ctx context.Context, pattern string) ([]string, error) {
	return s.scanKeys(ctx, pattern)
}

func (s *Store[K, V]) scanKeys(ctx context.Context, pattern string) ([]string, error) {
	keys := make(map[string]struct{})
	nodes := s.client.Nodes()
	if len(nodes) == 0 {
		nodes = map[string]vk.Client{"default": s.client}
	}
	for _, node := range nodes {
		var cursor uint64
		for {
			entry, err := node.Do(ctx, node.B().Scan().Cursor(cursor).Match(pattern).Count(256).Build()).AsScanEntry()
			if err != nil {
				return nil, err
			}
			for _, key := range entry.Elements {
				keys[key] = struct{}{}
			}
			cursor = entry.Cursor
			if cursor == 0 {
				break
			}
		}
	}
	out := make([]string, 0, len(keys))
	for key := range keys {
		out = append(out, key)
	}
	return out, nil
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

func mapError(err error) error {
	if err == nil {
		return nil
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return fmt.Errorf("%w: %v", ErrTransientNetwork, err)
	}
	var netErr net.Error
	if errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary()) {
		return fmt.Errorf("%w: %v", ErrTransientNetwork, err)
	}
	return err
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func escapeMatchLiteral(value string) string {
	if value == "" {
		return value
	}
	var b strings.Builder
	b.Grow(len(value) * 2)
	for _, ch := range value {
		switch ch {
		case '*', '?', '[', ']', '\\':
			b.WriteByte('\\')
		}
		b.WriteRune(ch)
	}
	return b.String()
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

func (s *Store[K, V]) addLogicalIndex(ctx context.Context, key K, dataKey string) error {
	logical, ok := logicalStringKey(key)
	if !ok {
		return nil
	}
	if err := s.client.Do(ctx, s.client.B().Set().Key(s.logicalByDataKey(dataKey)).Value(logical).Build()).Error(); err != nil {
		return err
	}
	if err := s.client.Do(ctx, s.client.B().Sadd().Key(s.logicalSetKey(logical)).Member(dataKey).Build()).Error(); err != nil {
		_ = s.client.Do(ctx, s.client.B().Del().Key(s.logicalByDataKey(dataKey)).Build()).Error()
		return err
	}
	return nil
}
