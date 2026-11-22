package runtime

import (
	"errors"
	"sync"

	"github.com/reinaldoborges/rff-go/internal/nilcheck"
)

var ErrDuplicateCodec = errors.New("codec version already registered")
var ErrCodecNotFound = errors.New("codec version not found")

// CodecRegistry resolves exact application-owned codec identities and versions.
// Its zero value is ready to use.
type CodecRegistry[T any] struct {
	mu     sync.RWMutex
	codecs map[CodecVersion]Codec[T]
}

func (r *CodecRegistry[T]) Register(version CodecVersion, codec Codec[T]) error {
	if version.Name == "" || version.Version == "" || nilcheck.IsNil(codec) {
		return ErrInvalidRequest
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.codecs == nil {
		r.codecs = make(map[CodecVersion]Codec[T])
	}
	if _, exists := r.codecs[version]; exists {
		return ErrDuplicateCodec
	}
	r.codecs[version] = codec
	return nil
}

// Resolve implements CodecResolver using the exact identity and version.
func (r *CodecRegistry[T]) Resolve(version CodecVersion) (Codec[T], error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	codec, ok := r.codecs[version]
	if !ok {
		return nil, ErrCodecNotFound
	}
	return codec, nil
}
