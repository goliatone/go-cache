package gocache

import (
	"errors"
	"fmt"
	"reflect"
)

var (
	// ErrNilKeyEncoderFunc indicates a nil KeyEncoderFunc was used.
	ErrNilKeyEncoderFunc = errors.New("gocache: nil key encoder func")
	// ErrKeyEncoderRequired indicates non-string key types must provide a key encoder.
	ErrKeyEncoderRequired = errors.New("gocache: key encoder required for non-string key type")
	// ErrInvalidStringKeyType indicates a default string encoder received a non-string value.
	ErrInvalidStringKeyType = errors.New("gocache: key is not string-compatible")
)

// KeyEncoder encodes cache keys into deterministic storage-safe strings.
type KeyEncoder[K comparable] interface {
	EncodeKey(K) (string, error)
}

// KeyEncoderFunc adapts a function to KeyEncoder.
type KeyEncoderFunc[K comparable] func(K) (string, error)

func (f KeyEncoderFunc[K]) EncodeKey(key K) (string, error) {
	if f == nil {
		return "", ErrNilKeyEncoderFunc
	}
	return f(key)
}

type defaultStringKeyEncoder[K comparable] struct{}

func (defaultStringKeyEncoder[K]) EncodeKey(key K) (string, error) {
	value := reflect.ValueOf(key)
	if !value.IsValid() || value.Kind() != reflect.String {
		return "", fmt.Errorf("%w: %T", ErrInvalidStringKeyType, key)
	}
	return value.String(), nil
}

type stringKeyEncoder[K ~string] struct{}

func (stringKeyEncoder[K]) EncodeKey(key K) (string, error) {
	return string(key), nil
}

// NewStringKeyEncoder returns a key encoder for string-typed keys.
func NewStringKeyEncoder[K ~string]() KeyEncoder[K] {
	return stringKeyEncoder[K]{}
}

// ResolveKeyEncoder resolves a key encoder for type K.
// If encoder is nil and K is string-compatible, a default string encoder is used.
func ResolveKeyEncoder[K comparable](encoder KeyEncoder[K]) (KeyEncoder[K], error) {
	if encoder != nil {
		return encoder, nil
	}
	if keyTypeIsString[K]() {
		return defaultStringKeyEncoder[K]{}, nil
	}
	var zero K
	return nil, fmt.Errorf("%w: %T", ErrKeyEncoderRequired, zero)
}

// EncodeStorageKey encodes a key using the supplied encoder or default string path.
func EncodeStorageKey[K comparable](key K, encoder KeyEncoder[K]) (string, error) {
	resolved, err := ResolveKeyEncoder(encoder)
	if err != nil {
		return "", err
	}
	return resolved.EncodeKey(key)
}

func keyTypeIsString[K comparable]() bool {
	var zero K
	typ := reflect.TypeOf(zero)
	return typ != nil && typ.Kind() == reflect.String
}
