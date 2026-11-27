package runtime_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestAcceptorRejectsInvalidGeneratedExecutionID(t *testing.T) {
	machine, err := fsm.NewMachine(fsm.Config[string, string, string, int]{
		ID: "task", Version: "1", InitialStates: []string{"open"},
		Handlers: map[string]fsm.Handler[string, string, string, int]{"open": fsm.NoopHandler[string, string, string, int]{}},
		SelectInitial: func(int) (fsm.Selection[string], error) {
			return fsm.Selection[string]{Target: "open", Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Second, OverallTimeout: time.Minute}}, nil
		},
		SelectTransition: func(string, string, int) (fsm.Selection[string], error) { return fsm.Selection[string]{}, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	var registry fsm.Registry[string, string, string, int]
	if err := registry.Register(machine); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"", string([]byte{0xff}), strings.Repeat("x", 257), strings.Repeat("é", 129)} {
		store := newMemoryStore()
		acceptor, err := transition.NewAcceptor[string, string, string, int](store, &registry,
			func(transition.CodecVersion) (transition.Codec[int], error) { return intCodec{}, nil },
			func(transition.CodecVersion) (transition.Codec[string], error) { return stringCodec{}, nil },
			func() (string, error) { return id, nil }, time.Now)
		if err != nil {
			t.Fatal(err)
		}
		_, created, err := acceptor.Accept(context.Background(), transition.Request[string, string, string, int]{
			Entity: "entity", Key: "key", Operation: transition.Initialize, MachineID: "task", MachineVersion: "1",
			InputCodec: transition.CodecVersion{Name: "input", Version: "1"}, Notification: transition.NotifyInherit,
		})
		if !errors.Is(err, transition.ErrInvalidDurableID) || created {
			t.Fatalf("generated ID %q: err=%v created=%v", id, err, created)
		}
	}
}
