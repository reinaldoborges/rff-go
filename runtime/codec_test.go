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
