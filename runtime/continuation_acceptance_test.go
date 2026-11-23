package runtime_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestAcceptancePinsTargetContinuationIndependentlyOfNotification(t *testing.T) {
	policy := fsm.Policy{MaxAttempts: 3, AttemptTimeout: time.Second, OverallTimeout: time.Minute, RetryDelays: []time.Duration{time.Second}}
	executionPolicy := fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Second, OverallTimeout: time.Minute}
	policies := map[string]fsm.Policy{"due": policy}
	machine, err := fsm.NewMachine(fsm.Config[string, string, string, int]{
		ID: "plan", Version: "v1", InitialStates: []string{"due"},
		Handlers: map[string]fsm.Handler[string, string, string, int]{
			"due": fsm.NoopHandler[string, string, string, int]{},
			"end": fsm.NoopHandler[string, string, string, int]{},
		},
		ContinuationPolicies: policies,
		SelectInitial: func(int) (fsm.Selection[string], error) {
			return fsm.Selection[string]{Target: "due", Policy: executionPolicy, Notify: true}, nil
		},
		SelectTransition: func(_ string, event string, _ int) (fsm.Selection[string], error) {
			if event == "enter" {
				return fsm.Selection[string]{Target: "due", Policy: executionPolicy, Notify: true}, nil
			}
			return fsm.Selection[string]{Target: "end", Policy: executionPolicy, Notify: true}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	policy.RetryDelays[0] = 9 * time.Second
	delete(policies, "due")
	var registry fsm.Registry[string, string, string, int]
	if err := registry.Register(machine); err != nil {
		t.Fatal(err)
	}
	ids := 0
	acceptor, err := transition.NewAcceptor[string, string, string, int](
		newMemoryStore(), &registry,
		func(transition.CodecVersion) (transition.Codec[int], error) { return intCodec{}, nil },
		func(transition.CodecVersion) (transition.Codec[string], error) { return stringCodec{}, nil },
		func() (string, error) { ids++; return fmt.Sprintf("origin-%d", ids), nil }, time.Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	execution, _, err := acceptor.Accept(context.Background(), transition.Request[string, string, string, int]{
		Entity: "entity", Key: "init", Operation: transition.Initialize, MachineID: "plan", MachineVersion: "v1",
		InputCodec: transition.CodecVersion{Name: "input", Version: "v1"}, Notification: transition.NotifyDisable,
	})
	if err != nil {
		t.Fatal(err)
	}
	if execution.Notify || execution.Continuation == nil || execution.Continuation.MachineVersion != "v1" ||
		execution.Continuation.MachineID != "plan" || execution.Continuation.Policy.MaxAttempts != 3 ||
		execution.Continuation.Policy.RetryDelays[0] != time.Second {
		t.Fatalf("continuation was not pinned independently of notification: %+v", execution)
	}
	for _, tt := range []struct {
		name, event string
		enabled     bool
	}{
		{name: "opted", event: "enter", enabled: true},
		{name: "not opted", event: "finish", enabled: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			record, _, err := acceptor.Accept(context.Background(), transition.Request[string, string, string, int]{
				Entity: tt.name, Key: tt.name, Operation: transition.Transition, MachineID: "plan", MachineVersion: "v1",
				Previous: "due", HasPrevious: true, Event: tt.event, HasEvent: true,
				EventCodec: transition.CodecVersion{Name: "event", Version: "v1"},
				InputCodec: transition.CodecVersion{Name: "input", Version: "v1"}, Notification: transition.NotifyEnable,
			})
			if err != nil || !record.Notify || (record.Continuation != nil) != tt.enabled {
				t.Fatalf("target continuation mismatch: %+v, %v", record, err)
			}
		})
	}
}
