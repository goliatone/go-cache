// Package storelock coordinates mutations performed by multiple store wrappers
// over the same embedded database handle.
package storelock

import "sync"

// Registry owns reference-counted mutexes keyed by a comparable database
// handle. Callers must release every acquired lease when the wrapper closes.
type Registry[K comparable] struct {
	mu      sync.Mutex
	entries map[K]*entry
}

type entry struct {
	mu   sync.Mutex
	refs int
}

// Lease is a shared mutation lock for one database handle.
type Lease[K comparable] struct {
	registry *Registry[K]
	key      K
	entry    *entry
	once     sync.Once
}

// Acquire returns a lease that shares its mutex with every lease for key.
func (r *Registry[K]) Acquire(key K) *Lease[K] {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.entries == nil {
		r.entries = make(map[K]*entry)
	}
	shared := r.entries[key]
	if shared == nil {
		shared = &entry{}
		r.entries[key] = shared
	}
	shared.refs++
	return &Lease[K]{registry: r, key: key, entry: shared}
}

// Lock acquires the database-scoped mutation lock.
func (l *Lease[K]) Lock() {
	if l != nil && l.entry != nil {
		l.entry.mu.Lock()
	}
}

// Unlock releases the database-scoped mutation lock.
func (l *Lease[K]) Unlock() {
	if l != nil && l.entry != nil {
		l.entry.mu.Unlock()
	}
}

// Release drops the lease reference. It is safe to call more than once.
func (l *Lease[K]) Release() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		if l.registry == nil || l.entry == nil {
			return
		}
		l.registry.mu.Lock()
		defer l.registry.mu.Unlock()
		current := l.registry.entries[l.key]
		if current != l.entry {
			return
		}
		current.refs--
		if current.refs == 0 {
			delete(l.registry.entries, l.key)
		}
	})
}
