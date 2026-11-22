package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestTypedInputRecoversAndReplaysDurableOutcome(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ingress.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return stringValueCodec{}, nil }
	unknown := errors.New("response lost after host commit")
	hostReceipts := make(map[string]transition.InputOutcome)
	calls := 0
	handler := func(_ context.Context, input transition.InputDelivery[string]) (transition.InputOutcome, error) {
		calls++
		if prior, ok := hostReceipts[input.ID]; ok {
			return prior, nil
		}
		if input.WorkflowID != "membership" || input.Value != "accept" || input.Kind != transition.HumanDecision {
			t.Fatalf("incorrect typed input delivery: %+v", input)
		}
		hostReceipts[input.ID] = transition.InputOutcome{Receipt: "execution-1"}
		return transition.InputOutcome{}, unknown
	}
	newIngress := func() *transition.InputIngress[string] {
		t.Helper()
		ingress, err := transition.NewInputIngress[string](store, "membership", resolve, handler)
		if err != nil {
			t.Fatal(err)
		}
		return ingress
	}
	request := transition.InputRequest[string]{
		ID: "decision-1", WorkflowID: "membership", Kind: transition.HumanDecision, Value: "accept",
		Codec: transition.CodecVersion{Name: "decision", Version: "1"},
	}
	if _, err := newIngress().Submit(ctx, request); !errors.Is(err, unknown) {
		t.Fatalf("unknown host outcome not returned: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	ingress := newIngress()
	if err := ingress.Reconcile(ctx); err != nil {
		t.Fatalf("recover pending input: %v", err)
	}
	outcome, err := ingress.Submit(ctx, request)
	if err != nil || outcome.Receipt != "execution-1" || calls != 2 || len(hostReceipts) != 1 {
		t.Fatalf("replay did not return prior outcome: outcome=%+v calls=%d error=%v", outcome, calls, err)
	}
	request.Value = "decline"
	if _, err := ingress.Submit(ctx, request); !errors.Is(err, transition.ErrConflict) || calls != 2 {
		t.Fatalf("changed input did not conflict: calls=%d error=%v", calls, err)
	}
}
