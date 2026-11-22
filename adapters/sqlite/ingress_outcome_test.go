package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestTypedInputPersistsDefinitiveRejectionAndRequiresComparableReplay(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "rejection.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	available := true
	resolve := func(transition.CodecVersion) (transition.Codec[string], error) {
		if !available {
			return nil, errors.New("codec unavailable")
		}
		return stringValueCodec{}, nil
	}
	calls := 0
	ingress, err := transition.NewInputIngress[string](store, "tasks", resolve, func(context.Context, transition.InputDelivery[string]) (transition.InputOutcome, error) {
		calls++
		return transition.InputOutcome{Failure: &fsm.Failure{Code: "invalid-transition"}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	request := transition.InputRequest[string]{
		ID: "input", WorkflowID: "task", Kind: transition.ExternalEvent, Value: "cancel",
		Codec: transition.CodecVersion{Name: "event", Version: "1"},
	}
	outcome, err := ingress.Submit(ctx, request)
	if err != nil || outcome.Failure == nil || outcome.Failure.Code != "invalid-transition" {
		t.Fatalf("definitive rejection not returned: outcome=%+v error=%v", outcome, err)
	}
	outcome, err = ingress.Submit(ctx, request)
	if err != nil || outcome.Failure == nil || outcome.Failure.Code != "invalid-transition" || calls != 1 {
		t.Fatalf("definitive rejection was not replayed: outcome=%+v calls=%d error=%v", outcome, calls, err)
	}
	available = false
	if _, err := ingress.Submit(ctx, request); !errors.Is(err, transition.ErrCompatibility) || calls != 1 {
		t.Fatalf("unverifiable replay was not rejected: calls=%d error=%v", calls, err)
	}
}
