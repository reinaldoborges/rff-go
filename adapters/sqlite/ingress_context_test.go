package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type cancelInputDecode struct {
	stringValueCodec
	cancel context.CancelFunc
}

func (c cancelInputDecode) Decode(ctx context.Context, _ []byte) (string, error) {
	c.cancel()
	return "", ctx.Err()
}

type cancelInputEqual struct {
	stringValueCodec
	cancel context.CancelFunc
}

func (c cancelInputEqual) Equal(ctx context.Context, _ []byte, _ string) (bool, error) {
	c.cancel()
	return false, ctx.Err()
}

func TestTypedInputCancellationIsNotCodecIncompatibility(t *testing.T) {
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
	ref := transition.CodecVersion{Name: "input", Version: "1"}
	ingress, err := transition.NewInputIngress[string](store, "channel",
		func(transition.CodecVersion) (transition.Codec[string], error) {
			return cancelInputDecode{cancel: cancel}, nil
		}, func(context.Context, transition.InputDelivery[string]) (transition.InputOutcome, error) {
			t.Fatal("canceled input reached the host")
			return transition.InputOutcome{}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	_, err = ingress.Submit(ctx, transition.InputRequest[string]{
		ID: "input", WorkflowID: "workflow", Kind: transition.Command, Value: "value", Codec: ref,
	})
	if !errors.Is(err, context.Canceled) || errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("canceled decode was mistaken for unavailable behavior: %v", err)
	}
	record, err := store.ReadInput(context.Background(), "input")
	if err != nil || record.Outcome != nil {
		t.Fatalf("canceled input was not left recoverable: %+v, %v", record, err)
	}
}

func TestTypedInputReplayCancellationIsNotCodecIncompatibility(t *testing.T) {
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "ingress.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	handler := func(context.Context, transition.InputDelivery[string]) (transition.InputOutcome, error) {
		return transition.InputOutcome{Receipt: "receipt"}, nil
	}
	ref := transition.CodecVersion{Name: "input", Version: "1"}
	request := transition.InputRequest[string]{
		ID: "input", WorkflowID: "workflow", Kind: transition.Command, Value: "value", Codec: ref,
	}
	ingress, err := transition.NewInputIngress[string](store, "channel",
		func(transition.CodecVersion) (transition.Codec[string], error) { return stringValueCodec{}, nil }, handler)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ingress.Submit(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ingress, err = transition.NewInputIngress[string](store, "channel",
		func(transition.CodecVersion) (transition.Codec[string], error) {
			return cancelInputEqual{cancel: cancel}, nil
		}, handler)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ingress.Submit(ctx, request)
	if !errors.Is(err, context.Canceled) || errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("canceled equality was mistaken for unavailable behavior: %v", err)
	}
}
