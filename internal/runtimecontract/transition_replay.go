package runtimecontract

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

// RunTransitionReplay checks event identity through the typed acceptance port.
func RunTransitionReplay(t *testing.T, newStore func(*testing.T) Store) {
	t.Helper()
	ctx := context.Background()
	store := newStore(t)
	selections := 0
	machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
		ID: "machine", Version: "1", InitialStates: []string{"open"},
		Handlers: map[string]fsm.Handler[string, string, string, string]{
			"open": fsm.NoopHandler[string, string, string, string]{},
			"done": fsm.NoopHandler[string, string, string, string]{},
		},
		SelectInitial: func(string) (fsm.Selection[string], error) {
			return fsm.Selection[string]{}, errors.New("unexpected initialization")
		},
		SelectTransition: func(string, string, string) (fsm.Selection[string], error) {
			selections++
			return fsm.Selection[string]{Target: "done", Policy: fsm.Policy{
				MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour,
			}}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var machines fsm.Registry[string, string, string, string]
	if err := machines.Register(machine); err != nil {
		t.Fatal(err)
	}
	resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return stringCodec{}, nil }
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	acceptor, err := transition.NewAcceptor[string, string, string, string](store, &machines, resolve, resolve, func() (string, error) {
		return "execution", nil
	}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	ref := transition.CodecVersion{Name: "string", Version: "1"}
	request := transition.Request[string, string, string, string]{
		Entity: "entity", Key: "key", Operation: transition.Transition, MachineID: "machine", MachineVersion: "1",
		Previous: "open", HasPrevious: true, Event: "finish", HasEvent: true, EventCodec: ref,
		Input: "payload", InputCodec: ref, Notification: transition.NotifyInherit,
	}
	accepted, created, err := acceptor.Accept(ctx, request)
	if err != nil || !created {
		t.Fatalf("transition was not accepted: created=%v error=%v", created, err)
	}
	replayed, created, err := acceptor.Accept(ctx, request)
	if err != nil || created || replayed.ID != accepted.ID {
		t.Fatalf("identical event did not replay: %+v created=%v error=%v", replayed, created, err)
	}
	request.Event = "other"
	if _, _, err := acceptor.Accept(ctx, request); !errors.Is(err, transition.ErrConflict) || selections != 1 {
		t.Fatalf("changed event did not conflict before selection: selections=%d error=%v", selections, err)
	}
}
