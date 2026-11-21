package sqlite

import (
	"fmt"
	"slices"
)

// Codec maps comparable identifiers to stable, canonical bytes and back.
// Equal values must have identical encodings, and decoding an encoding must
// reproduce the original value.
type Codec[T comparable] interface {
	Encode(T) ([]byte, error)
	Decode([]byte) (T, error)
}

// StringCodec encodes Go strings without changing their bytes.
type StringCodec struct{}

func (StringCodec) Encode(value string) ([]byte, error) { return []byte(value), nil }
func (StringCodec) Decode(data []byte) (string, error)  { return string(data), nil }

func (s *Store[EntityID, StateID]) encodeEntity(entity EntityID) ([]byte, error) {
	encoded, err := s.entityCodec.Encode(entity)
	if err != nil {
		return nil, fmt.Errorf("encode entity: %w", err)
	}
	encoded = slices.Clone(encoded)
	if encoded == nil {
		encoded = []byte{}
	}
	decoded, err := s.entityCodec.Decode(slices.Clone(encoded))
	if err != nil {
		return nil, fmt.Errorf("decode entity: %w", err)
	}
	if decoded != entity {
		return nil, fmt.Errorf("entity codec does not round-trip")
	}
	return encoded, nil
}
