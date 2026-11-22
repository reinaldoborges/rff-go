// Package runtimecontract checks transition adapters through runtime ports.
package runtimecontract

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

// Store combines the public ports needed by the basic execution contract.
type Store interface {
	transition.AcceptanceStore[string, string]
	transition.CoordinatorStore[string, string]
	transition.HookActivityStore[string, string]
	transition.CommitStore[string, string]
	transition.InspectionStore
}

type stringCodec struct{}

func (stringCodec) Encode(_ context.Context, value string) ([]byte, error) { return []byte(value), nil }
func (stringCodec) Decode(_ context.Context, data []byte) (string, error)  { return string(data), nil }
func (stringCodec) Equal(_ context.Context, data []byte, value string) (bool, error) {
	return string(data) == value, nil
}

type handler struct {
	fsm.NoopHandler[string, string, string, string]
	calls *int
}

func (h handler) Enter(_ context.Context, input fsm.ExecutionContext[string, string, string, string]) error {
	if input.Input() != "payload" {
		return errors.New("wrong rehydrated input")
	}
	*h.calls++
	return nil
}

type host struct {
	writes  int
	current string
	receipt *transition.CommitReceipt
}

func (h *host) Commit(_ context.Context, _ transition.TransitionLease[string],
	execution transition.Execution[string, string],
) (transition.CommitReceipt, error) {
	if h.receipt != nil {
		return *h.receipt, nil
	}
	h.writes++
	h.current = execution.Target
	receipt := transition.CommitReceipt{
		ExecutionID: execution.ID, OperationID: execution.CommitOperationID, Outcome: transition.CommitSucceeded,
	}
	h.receipt = &receipt
	return receipt, nil
}

// Run checks a typed execution, durable commit and idempotent replay at runtime ports.
func Run(t *testing.T, newStore func(*testing.T) Store) {
	t.Helper()
	t.Run("typed execution and commit", func(t *testing.T) {
		ctx := context.Background()
		store := newStore(t)
		calls := 0
		machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
			ID: "machine", Version: "1", InitialStates: []string{"open"},
			Handlers: map[string]fsm.Handler[string, string, string, string]{"open": handler{calls: &calls}},
			SelectInitial: func(string) (fsm.Selection[string], error) {
				return fsm.Selection[string]{Target: "open", Policy: fsm.Policy{
					MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour,
				}}, nil
			},
			SelectTransition: func(string, string, string) (fsm.Selection[string], error) {
				return fsm.Selection[string]{}, errors.New("unexpected transition")
			},
			ClassifyFailure: func(error) (fsm.Failure, fsm.Disposition) {
				return fsm.Failure{Code: "failed"}, fsm.DispositionIntervention
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		var machines fsm.Registry[string, string, string, string]
		if err := machines.Register(machine); err != nil {
			t.Fatal(err)
		}
		var codecs transition.CodecRegistry[string]
		ref := transition.CodecVersion{Name: "input", Version: "1"}
		if err := codecs.Register(ref, stringCodec{}); err != nil {
			t.Fatal(err)
		}
		acceptor, err := transition.NewAcceptor[string, string, string, string](store, &machines, codecs.Resolve, codecs.Resolve,
			func() (string, error) { return "execution", nil }, time.Now)
		if err != nil {
			t.Fatal(err)
		}
		request := transition.Request[string, string, string, string]{
			Entity: "entity", Key: "key", Operation: transition.Initialize, MachineID: "machine", MachineVersion: "1",
			Input: "payload", InputCodec: ref, Notification: transition.NotifyInherit,
		}
		execution, created, err := acceptor.Accept(ctx, request)
		if err != nil || !created {
			t.Fatalf("accept: created=%v error=%v", created, err)
		}
		runner, err := transition.NewHookRunner[string, string, string, string](&machines, codecs.Resolve, codecs.Resolve)
		if err != nil {
			t.Fatal(err)
		}
		config := transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Minute, Clock: time.Now}
		coordinator, err := transition.NewCoordinator[string, string](store, runner, config)
		if err != nil {
			t.Fatal(err)
		}
		worker, err := transition.NewHookWorker[string, string, string, string](store, runner, transition.WorkerConfig{
			ID: "worker", Capacity: 1, LeaseTTL: time.Minute, RenewalInterval: time.Second, Clock: transition.SystemWorkerClock{},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		if err := worker.RunOne(ctx); err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		domain := &host{}
		commits, err := transition.NewCommitCoordinator[string, string](store, domain, config)
		if err != nil {
			t.Fatal(err)
		}
		if err := commits.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		record, err := store.InspectExecution(ctx, execution.ID)
		if err != nil || record.Status != transition.Complete || record.NotificationIntentID != "" ||
			domain.current != "open" || domain.writes != 1 || calls != 1 {
			t.Fatalf("execution did not complete exactly once at the host boundary: record=%+v host=%+v calls=%d error=%v",
				record, domain, calls, err)
		}
		replayed, created, err := acceptor.Accept(ctx, request)
		if err != nil || created || replayed.ID != execution.ID {
			t.Fatalf("execution replay changed: %+v created=%v error=%v", replayed, created, err)
		}
		request.Input = "changed"
		if _, _, err := acceptor.Accept(ctx, request); !errors.Is(err, transition.ErrConflict) {
			t.Fatalf("changed request did not conflict: %v", err)
		}
	})
}
