package runtimecontract

import (
	"context"
	"errors"
	"testing"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

// RunInputIngress checks durable input recovery and semantic replay through its port.
func RunInputIngress(t *testing.T, newStore func(*testing.T) transition.InputStore) {
	t.Helper()
	ctx := context.Background()
	store := newStore(t)
	unavailable := false
	resolve := func(version transition.CodecVersion) (transition.Codec[string], error) {
		if version.Name != "input" || (version.Version != "1" && version.Version != "2") ||
			version.Version == "1" && unavailable {
			return nil, transition.ErrCodecNotFound
		}
		return stringCodec{}, nil
	}
	lost := errors.New("host response lost")
	receipts := make(map[string]transition.InputOutcome)
	expected := map[string]transition.InputDelivery[string]{
		"a": {ID: "a", WorkflowID: "one", Kind: transition.HumanDecision, Value: "accept"},
		"b": {ID: "b", WorkflowID: "two", Kind: transition.ExternalEvent, Value: "expire"},
		"c": {ID: "c", WorkflowID: "three", Kind: transition.Command, Value: "start"},
	}
	calls := 0
	ingress, err := transition.NewInputIngress[string](store, "workflow", resolve,
		func(_ context.Context, input transition.InputDelivery[string]) (transition.InputOutcome, error) {
			if input != expected[input.ID] {
				t.Fatalf("incorrect typed input delivery: %+v", input)
			}
			calls++
			if receipt, ok := receipts[input.ID]; ok {
				return receipt, nil
			}
			receipts[input.ID] = transition.InputOutcome{Receipt: "receipt-" + input.ID}
			return transition.InputOutcome{}, lost
		})
	if err != nil {
		t.Fatal(err)
	}
	first := transition.InputRequest[string]{
		ID: "a", WorkflowID: "one", Kind: transition.HumanDecision, Value: "accept",
		Codec: transition.CodecVersion{Name: "input", Version: "1"},
	}
	second := transition.InputRequest[string]{
		ID: "b", WorkflowID: "two", Kind: transition.ExternalEvent, Value: "expire",
		Codec: transition.CodecVersion{Name: "input", Version: "2"},
	}
	third := transition.InputRequest[string]{
		ID: "c", WorkflowID: "three", Kind: transition.Command, Value: "start",
		Codec: transition.CodecVersion{Name: "input", Version: "2"},
	}
	for _, request := range []transition.InputRequest[string]{first, second, third} {
		if _, err := ingress.Submit(ctx, request); !errors.Is(err, lost) {
			t.Fatalf("unknown host outcome was not reported for %q: %v", request.ID, err)
		}
	}
	unavailable = true
	if err := ingress.Reconcile(ctx); !errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("unavailable input codec was not reported: %v", err)
	}
	for _, request := range []transition.InputRequest[string]{second, third} {
		record, err := store.ReadInput(ctx, request.ID)
		if err != nil || record.Outcome == nil || record.Outcome.Receipt != "receipt-"+request.ID {
			t.Fatalf("incompatible input prevented independent recovery: %+v, %v", record, err)
		}
	}
	record, err := store.ReadInput(ctx, first.ID)
	if err != nil || record.Outcome != nil {
		t.Fatalf("unavailable input did not remain pending: %+v, %v", record, err)
	}
	unavailable = false
	if err := ingress.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	outcome, err := ingress.Submit(ctx, first)
	if err != nil || outcome.Receipt != "receipt-a" || calls != 6 || len(receipts) != 3 {
		t.Fatalf("input recovery or replay duplicated host effects: outcome=%+v calls=%d error=%v", outcome, calls, err)
	}
	first.Value = "decline"
	if _, err := ingress.Submit(ctx, first); !errors.Is(err, transition.ErrConflict) || calls != 6 {
		t.Fatalf("changed replay did not conflict: calls=%d error=%v", calls, err)
	}
	unavailable = true
	if _, err := ingress.Submit(ctx, first); !errors.Is(err, transition.ErrCompatibility) || calls != 6 {
		t.Fatalf("terminal replay with unavailable codec was not rejected: calls=%d error=%v", calls, err)
	}
}
