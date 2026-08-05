package storelock

import (
	"testing"
	"time"
)

func TestRegistrySerializesLeasesForSameKey(t *testing.T) {
	var registry Registry[string]
	first := registry.Acquire("db")
	second := registry.Acquire("db")
	defer first.Release()
	defer second.Release()

	first.Lock()
	acquired := make(chan struct{})
	go func() {
		second.Lock()
		close(acquired)
		second.Unlock()
	}()

	select {
	case <-acquired:
		t.Fatal("same-key lease acquired while first lease held the lock")
	case <-time.After(25 * time.Millisecond):
	}
	first.Unlock()

	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("same-key lease did not acquire after unlock")
	}
}

func TestRegistryDoesNotSerializeDifferentKeys(t *testing.T) {
	var registry Registry[string]
	first := registry.Acquire("db-a")
	second := registry.Acquire("db-b")
	defer first.Release()
	defer second.Release()

	first.Lock()
	defer first.Unlock()

	acquired := make(chan struct{})
	go func() {
		second.Lock()
		close(acquired)
		second.Unlock()
	}()

	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("different-key lease was unnecessarily blocked")
	}
}

func TestRegistryReleaseIsIdempotentAndAllowsReuse(t *testing.T) {
	var registry Registry[string]
	lease := registry.Acquire("db")
	lease.Release()
	lease.Release()

	replacement := registry.Acquire("db")
	replacement.Lock()
	replacement.Unlock()
	replacement.Release()
}
