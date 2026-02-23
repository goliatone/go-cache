package gocache

import (
	"errors"
	"testing"
)

type customStringKey string

func TestResolveKeyEncoderStringTypes(t *testing.T) {
	strEncoder, err := ResolveKeyEncoder[string](nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got, err := strEncoder.EncodeKey("abc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "abc" {
		t.Fatalf("expected abc, got %q", got)
	}

	customEncoder, err := ResolveKeyEncoder[customStringKey](nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got, err = customEncoder.EncodeKey(customStringKey("xyz"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "xyz" {
		t.Fatalf("expected xyz, got %q", got)
	}
}

func TestResolveKeyEncoderRequiresCustomEncoderForNonString(t *testing.T) {
	_, err := ResolveKeyEncoder[int](nil)
	if !errors.Is(err, ErrKeyEncoderRequired) {
		t.Fatalf("expected ErrKeyEncoderRequired, got %v", err)
	}
}

func TestEncodeStorageKeyUsesCustomEncoderDeterministically(t *testing.T) {
	calls := 0
	encoder := KeyEncoderFunc[int](func(v int) (string, error) {
		calls++
		return "key:42", nil
	})

	first, err := EncodeStorageKey(42, encoder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	second, err := EncodeStorageKey(42, encoder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if first != "key:42" || second != "key:42" {
		t.Fatalf("unexpected outputs: %q %q", first, second)
	}
	if calls != 2 {
		t.Fatalf("expected 2 calls, got %d", calls)
	}
}

func TestNilKeyEncoderFunc(t *testing.T) {
	var fn KeyEncoderFunc[int]
	_, err := fn.EncodeKey(1)
	if !errors.Is(err, ErrNilKeyEncoderFunc) {
		t.Fatalf("expected ErrNilKeyEncoderFunc, got %v", err)
	}
}
