package envelope

import (
	"bytes"
	"errors"
	"testing"
)

func TestEnvelopeRoundTrip(t *testing.T) {
	raw, err := Marshal([]byte("payload"), 12345)
	if err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}
	record, err := Unmarshal(raw)
	if err != nil {
		t.Fatalf("unexpected unmarshal error: %v", err)
	}

	if record.Version != CurrentVersion {
		t.Fatalf("expected version %d, got %d", CurrentVersion, record.Version)
	}
	if record.ExpiresAtUnixNano != 12345 {
		t.Fatalf("expected expiry 12345, got %d", record.ExpiresAtUnixNano)
	}
	if !bytes.Equal(record.Payload, []byte("payload")) {
		t.Fatalf("unexpected payload: %q", string(record.Payload))
	}
}

func TestEnvelopeUnmarshalRejectsTooShort(t *testing.T) {
	_, err := Unmarshal([]byte{1, 2, 3})
	if !errors.Is(err, ErrInvalidEnvelope) {
		t.Fatalf("expected ErrInvalidEnvelope, got %v", err)
	}
}

func TestEnvelopeUnmarshalRejectsUnsupportedVersion(t *testing.T) {
	raw, err := MarshalWithVersion(CurrentVersion+1, []byte("x"), 1)
	if err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}
	_, err = Unmarshal(raw)
	if !errors.Is(err, ErrUnsupportedEnvelopeVersion) {
		t.Fatalf("expected ErrUnsupportedEnvelopeVersion, got %v", err)
	}
}

func TestEnvelopeMarshalRejectsVersionZero(t *testing.T) {
	_, err := MarshalWithVersion(0, []byte("x"), 0)
	if !errors.Is(err, ErrInvalidEnvelope) {
		t.Fatalf("expected ErrInvalidEnvelope, got %v", err)
	}
}
