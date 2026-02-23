package envelope

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	// CurrentVersion is the active on-disk envelope version.
	CurrentVersion uint8 = 1
	// HeaderSize is the fixed envelope header size in bytes.
	HeaderSize = 9
)

var (
	// ErrInvalidEnvelope indicates malformed envelope bytes.
	ErrInvalidEnvelope = errors.New("gocache: invalid envelope")
	// ErrUnsupportedEnvelopeVersion indicates unknown envelope format version.
	ErrUnsupportedEnvelopeVersion = errors.New("gocache: unsupported envelope version")
)

// Record represents a decoded cache record envelope.
type Record struct {
	Version           uint8
	ExpiresAtUnixNano int64
	Payload           []byte
}

// Marshal encodes payload and expiration using CurrentVersion.
func Marshal(payload []byte, expiresAtUnixNano int64) ([]byte, error) {
	return MarshalWithVersion(CurrentVersion, payload, expiresAtUnixNano)
}

// MarshalWithVersion encodes payload with the provided envelope version.
func MarshalWithVersion(version uint8, payload []byte, expiresAtUnixNano int64) ([]byte, error) {
	if version == 0 {
		return nil, fmt.Errorf("%w: version 0", ErrInvalidEnvelope)
	}
	out := make([]byte, HeaderSize+len(payload))
	out[0] = version
	binary.BigEndian.PutUint64(out[1:HeaderSize], uint64(expiresAtUnixNano))
	copy(out[HeaderSize:], payload)
	return out, nil
}

// Unmarshal decodes envelope bytes.
func Unmarshal(raw []byte) (Record, error) {
	var record Record
	if len(raw) < HeaderSize {
		return record, fmt.Errorf("%w: too short", ErrInvalidEnvelope)
	}
	record.Version = raw[0]
	if record.Version != CurrentVersion {
		return record, fmt.Errorf("%w: %d", ErrUnsupportedEnvelopeVersion, record.Version)
	}
	record.ExpiresAtUnixNano = int64(binary.BigEndian.Uint64(raw[1:HeaderSize]))
	payload := raw[HeaderSize:]
	record.Payload = make([]byte, len(payload))
	copy(record.Payload, payload)
	return record, nil
}
