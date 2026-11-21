package runtime

import (
	"fmt"

	"github.com/reinaldoborges/rff-go/internal/nilcheck"
)

func resolveCodec[T any](resolver CodecResolver[T], version CodecVersion) (Codec[T], error) {
	codec, err := resolver(version)
	if err != nil {
		return nil, fmt.Errorf("%w: codec %q version %q: %w", ErrCompatibility, version.Name, version.Version, err)
	}
	if nilcheck.IsNil(codec) {
		return nil, fmt.Errorf("%w: codec %q version %q is unavailable", ErrCompatibility, version.Name, version.Version)
	}
	return codec, nil
}
