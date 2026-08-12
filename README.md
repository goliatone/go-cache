## go-cache

`go-cache` provides a generic cache contract, read-through helpers, shared serialization primitives, and multiple backend implementations.

### Core contract

```go
type Cache[K comparable, V any] interface {
    Get(ctx context.Context, key K) (V, bool, error)
    Set(ctx context.Context, key K, value V, ttl time.Duration) error
    Delete(ctx context.Context, key K) error
}
```

Optional capabilities:

- `Clearable`
- `Purgeable`
- `SetIfAbsentCache`
- `SetIfPresentCache`
- `TypedPrefixInvalidator[K]`
- `BatchInvalidator`
- `TypedTagRegistry[K]`
- `PrefixInvalidator` (deprecated compatibility)
- `TagRegistry` (deprecated compatibility)

### Read-through helper

Use `GetOrFetch` for basic read-through behavior, or `GetOrFetchWithOptions` for:

- read/write error policy
- optional singleflight dedupe
- TTL parity with backend policy (`ttl <= 0` is a non-expiring write, not a write skip)

### Shared primitives

- `codec.Codec[V]` and default `codec.JSONCodec[V]` for value serialization
- deterministic key encoding via `KeyEncoder[K]`
- versioned storage envelope format in `internal/envelope`
- optional instrumentation via `Observer` and `Observation`

### Backend selection matrix

| Backend                            | Use when                                  | Persistence     | Runtime dependency        | Notes                                                        |
| ---------------------------------- | ----------------------------------------- | --------------- | ------------------------- | ------------------------------------------------------------ |
| `stores/memory`                    | Single-process, fastest local cache       | No              | None                      | Optional hard capacity with deterministic LRU eviction       |
| `stores/valkey`                    | Shared/distributed cache across instances | External Valkey | Valkey server             | Supports namespacing and integration tests via `VALKEY_ADDR` |
| `stores/pebble`                    | Embedded persistent cache without cgo     | Yes (disk)      | None (Go-native)          | Primary persistent backend                                   |
| `stores/rocksdb` (`-tags rocksdb`) | Teams requiring RocksDB specifically      | Yes (disk)      | Native RocksDB libs + cgo | Optional backend behind build tag                            |

### Capability support matrix

| Capability                       | Memory | Valkey | Pebble | RocksDB (`-tags rocksdb`) |
| -------------------------------- | ------ | ------ | ------ | ------------------------- |
| `Clearable`                      | Yes    | Yes    | Yes    | Yes                       |
| `Purgeable`                      | Yes    | Yes    | Yes    | Yes                       |
| `SetIfAbsentCache`               | Yes    | Yes    | Yes    | Yes                       |
| `SetIfPresentCache`              | Yes    | Yes    | Yes    | Yes                       |
| `BatchInvalidator`               | Yes    | Yes    | Yes    | Yes                       |
| `TypedPrefixInvalidator[K]`      | Yes    | Yes    | Yes    | Yes                       |
| `TypedTagRegistry[K]`            | Yes    | Yes    | Yes    | No                        |
| `PrefixInvalidator` (deprecated) | Yes    | Yes    | Yes    | Yes                       |
| `TagRegistry` (deprecated)       | Yes    | Yes    | Yes    | No                        |

### Behavior policy

The following behaviors are intentionally consistent across helpers and backends:

- TTL: `Set(..., ttl)` with `ttl <= 0` stores a non-expiring entry.
- Helper TTL parity: `GetOrFetch*` does not skip writes for `ttl <= 0`; it writes non-expiring values.
- Decode failures: `Get` returns an error and does not return stale/partial values.
- Context cancellation: cancellable operations (`Get`, `Set`, `Delete`, invalidation methods) return context errors when canceled.
- Conditional update: `SetIfPresent` replaces only a live entry, resets its TTL, preserves logical/tag metadata, and reports a missing or expired entry without creating it. Successful updates emit the existing `set` observer operation.
- Logical-key semantics: typed invalidation/tag interfaces operate on logical keys, not encoded storage keys.

### Key encoding policy

Storage keys are normalized to deterministic strings.

- For string-like keys (`K ~string`), no custom encoder is required.
- For non-string keys, a deterministic `KeyEncoder[K]` is required.

Example:

```go
encoder := gocache.KeyEncoderFunc[int](func(key int) (string, error) {
    return fmt.Sprintf("user:%d", key), nil
})

store, err := memory.NewStore[int, MyValue](
    memory.WithKeyEncoder[int, MyValue](encoder),
)
```

Use the same key encoder strategy across services/backends to avoid cross-backend key drift.

### Instrumentation observer hooks

Attach an observer with backend `WithObserver(...)` options.

Event operation names:

- `get_hit`
- `get_miss`
- `set`
- `delete`
- `error`
- `evict` (bounded memory backend; aggregate count with no key)

`Observation` fields:

- `Backend`: backend identifier (`memory`, `valkey`, `pebble`, `rocksdb`)
- `Operation`: one of the operation names above
- `Key`: storage/logical key used by the backend path
- `Err`: optional error for failed operations
- `Latency`: operation duration
- `Count`: entries affected by an aggregate operation
- `Occupancy` / `Capacity`: bounded memory usage without exposing keys or values

Bound the memory backend with a positive maximum when cache cardinality is not
intrinsically finite:

```go
store, err := memory.NewStore[string, MyValue](
    memory.WithMaxEntries[string, MyValue](10_000),
)
```

New-key admission removes expired entries first. If the store is still full,
it evicts the least recently used live entry; successful gets and writes update
recency. Eviction observations intentionally leave `Key` empty.

### CI verification matrix

GitHub Actions workflow: `.github/workflows/ci.yml`

- unit/core tests
- backend conformance matrix (`memory`, `valkey`, `pebble`)
- valkey service-backed integration conformance
- pebble restart durability tests
- optional rocksdb tagged job (`-tags rocksdb`)
- race checks for memory and shared core paths

### Local quality checks

The taskfile pins and installs the Go quality tools when they are missing:

```bash
./taskfile go:tools:all
```

Use `./taskfile go:quality:pr` for format, test, and change-scoped lint checks. The
full local gate also applies Go formatting/fixes, runs tests and race tests, and
checks new lint, vulnerability, and gosec findings:

```bash
./taskfile go:quality:all
```

Use `./taskfile go:lint:report` to inspect the full lint backlog and
`./taskfile go:quality:baseline` for a non-mutating full check against the
committed lint and gosec baselines. Refresh baselines only after intentionally
accepting or cleaning up repository-wide findings with
`./taskfile go:baseline:update:all`.

### Operational caveats

- Prefix invalidation cost:
  - Valkey uses scan/delete and can be expensive on large namespaces.
  - LSM backends (Pebble/RocksDB) rely on prefix iteration and batched deletes; large keyspaces increase I/O.
- Tag index overhead:
  - Tagging maintains additional index records (extra writes, extra storage/memory).
- Envelope versioning strategy:
  - Stored values use a versioned envelope (`version`, `expires_at_unix_nano`, payload).
  - Current version is `1`; unknown versions fail decode with an explicit error.

### RocksDB notes

- Requires native RocksDB headers and libraries (`rocksdb/c.h`, linked RocksDB runtime).
- Compile/run with build tag:

```bash
go test -tags rocksdb ./stores/rocksdb
```

If headers/libs are missing, tagged builds fail at cgo compile time.

### Scope

This package is intended for reusable cache abstractions and helpers. It is not a replacement for domain-specific operational stores (idempotency leases, queue state, chunk-upload sessions).
