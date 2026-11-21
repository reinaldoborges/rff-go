package sqlite

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
