package runtime_test

import (
	"context"
	"errors"
	"testing"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestTypedNilCodecIsUnavailable(t *testing.T) {
	f := newAcceptanceFixture(t)
	f.codecNil.Store(true)
	if _, _, err := f.acceptor.Accept(context.Background(), f.request); !errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("expected a compatibility error for nil codec, got %v", err)
	}
}

func TestCodecRegistryRejectsDuplicatesAndResolvesExactVersions(t *testing.T) {
	var registry transition.CodecRegistry[string]
	version1 := transition.CodecVersion{Name: "input", Version: "1"}
	version2 := transition.CodecVersion{Name: "input", Version: "2"}
	codec := testStringCodec{}
	if err := registry.Register(version1, codec); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(version2, codec); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(version1, codec); !errors.Is(err, transition.ErrDuplicateCodec) {
		t.Fatalf("duplicate codec was accepted: %v", err)
	}
	var nilCodec *testStringCodec
	if err := registry.Register(transition.CodecVersion{Name: "input", Version: "3"}, nilCodec); !errors.Is(err, transition.ErrInvalidRequest) {
		t.Fatalf("nil codec was registered: %v", err)
	}
	if _, err := registry.Resolve(version2); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Resolve(transition.CodecVersion{Name: "input", Version: "3"}); !errors.Is(err, transition.ErrCodecNotFound) {
		t.Fatalf("unknown codec version resolved: %v", err)
	}
}

type testStringCodec struct{}

func (testStringCodec) Encode(_ context.Context, value string) ([]byte, error) {
	return []byte(value), nil
}
func (testStringCodec) Decode(_ context.Context, data []byte) (string, error) {
	return string(data), nil
}
func (testStringCodec) Equal(_ context.Context, data []byte, value string) (bool, error) {
	return string(data) == value, nil
}
