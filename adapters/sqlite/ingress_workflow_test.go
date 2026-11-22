package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestTypedCommandStartsOneDurableWorkflow(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "workflow.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	selected := 0
	machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
		ID: "task", Version: "1", InitialStates: []string{"open"},
		Handlers: map[string]fsm.Handler[string, string, string, string]{"open": fsm.NoopHandler[string, string, string, string]{}},
		SelectInitial: func(string) (fsm.Selection[string], error) {
			selected++
			return fsm.Selection[string]{Target: "open", Policy: fsm.Policy{
				MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour,
			}}, nil
		},
		SelectTransition: func(string, string, string) (fsm.Selection[string], error) { return fsm.Selection[string]{}, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	var registry fsm.Registry[string, string, string, string]
	if err := registry.Register(machine); err != nil {
		t.Fatal(err)
	}
	ref := transition.CodecVersion{Name: "string", Version: "1"}
	resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return stringValueCodec{}, nil }
	acceptor, err := transition.NewAcceptor[string, string, string, string](
		store, &registry, resolve, resolve, func() (string, error) { return "execution", nil }, time.Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	ingress, err := transition.NewInputIngress[string](store, "task", resolve,
		func(ctx context.Context, input transition.InputDelivery[string]) (transition.InputOutcome, error) {
			execution, _, err := acceptor.Accept(ctx, transition.Request[string, string, string, string]{
				Entity: input.WorkflowID, Key: input.ID, Operation: transition.Initialize,
				MachineID: "task", MachineVersion: "1", Input: input.Value, InputCodec: ref,
				Notification: transition.NotifyInherit,
			})
			if err != nil {
				return transition.InputOutcome{}, err
			}
			return transition.InputOutcome{Receipt: execution.ID}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	command := transition.InputRequest[string]{ID: "start", WorkflowID: "task-1", Kind: transition.Command, Value: "create", Codec: ref}
	for range 2 {
		outcome, err := ingress.Submit(ctx, command)
		if err != nil || outcome.Receipt != "execution" {
			t.Fatalf("start command failed: outcome=%+v error=%v", outcome, err)
		}
	}
	execution, err := store.ReadActiveExecution(ctx, "execution")
	if err != nil || execution.Entity != "task-1" || execution.Status != transition.Waiting || selected != 1 {
		t.Fatalf("command did not start one workflow: execution=%+v selections=%d error=%v", execution, selected, err)
	}
	command.Value = "different"
	if _, err := ingress.Submit(ctx, command); !errors.Is(err, transition.ErrConflict) || selected != 1 {
		t.Fatalf("changed command did not conflict: selections=%d error=%v", selected, err)
	}
}
