package valkey

import (
	"errors"
	"strings"

	gocache "github.com/goliatone/go-cache"
	"github.com/goliatone/go-cache/codec"
	vk "github.com/valkey-io/valkey-go"
)

const defaultAddress = "127.0.0.1:6379"

// Option configures the valkey store.
type Option[K comparable, V any] func(*Store[K, V]) error

// WithClient sets an existing valkey client; store will not close it.
func WithClient[K comparable, V any](client vk.Client) Option[K, V] {
	return func(store *Store[K, V]) error {
		if client == nil {
			return errors.New("valkey: nil client")
		}
		store.client = client
		store.ownsClient = false
		return nil
	}
}

// WithClientOption sets client creation options used when no client is injected.
func WithClientOption[K comparable, V any](option vk.ClientOption) Option[K, V] {
	return func(store *Store[K, V]) error {
		store.clientOption = option
		return nil
	}
}

// WithAddress sets a single valkey address.
func WithAddress[K comparable, V any](address string) Option[K, V] {
	return func(store *Store[K, V]) error {
		address = strings.TrimSpace(address)
		if address == "" {
			return errors.New("valkey: empty address")
		}
		store.addresses = []string{address}
		return nil
	}
}

// WithAddresses sets one or more valkey addresses.
func WithAddresses[K comparable, V any](addresses []string) Option[K, V] {
	return func(store *Store[K, V]) error {
		clean := make([]string, 0, len(addresses))
		for _, address := range addresses {
			address = strings.TrimSpace(address)
			if address == "" {
				continue
			}
			clean = append(clean, address)
		}
		if len(clean) == 0 {
			return errors.New("valkey: no valid addresses")
		}
		store.addresses = clean
		return nil
	}
}

// WithNamespace sets a namespacing prefix for all valkey keys.
func WithNamespace[K comparable, V any](namespace string) Option[K, V] {
	return func(store *Store[K, V]) error {
		namespace = strings.TrimSpace(namespace)
		if namespace == "" {
			return errors.New("valkey: empty namespace")
		}
		store.namespace = namespace
		return nil
	}
}

// WithCodec sets the value codec.
func WithCodec[K comparable, V any](c codec.Codec[V]) Option[K, V] {
	return func(store *Store[K, V]) error {
		if c == nil {
			return errors.New("valkey: nil codec")
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

// WithObserver sets instrumentation observer.
func WithObserver[K comparable, V any](observer gocache.Observer) Option[K, V] {
	return func(store *Store[K, V]) error {
		store.observer = observer
		return nil
	}
}
