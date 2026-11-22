package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestTypedInputResolutionCancellationIsNotCodecIncompatibility(t *testing.T) {
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "ingress.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resolutions := 0
	ingress, err := transition.NewInputIngress[string](store, "channel",
		func(transition.CodecVersion) (transition.Codec[string], error) {
			resolutions++
			if resolutions == 2 {
				cancel()
				return nil, context.Canceled
			}
			return stringValueCodec{}, nil
		}, func(context.Context, transition.InputDelivery[string]) (transition.InputOutcome, error) {
			t.Fatal("canceled input reached the host")
			return transition.InputOutcome{}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	_, err = ingress.Submit(ctx, transition.InputRequest[string]{
		ID: "input", WorkflowID: "workflow", Kind: transition.Command, Value: "value",
		Codec: transition.CodecVersion{Name: "input", Version: "1"},
	})
	if !errors.Is(err, context.Canceled) || errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("canceled resolution was mistaken for unavailable behavior: %v", err)
	}
}
