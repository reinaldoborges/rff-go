package runtimecontract

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type retrySelectionHandler struct {
	fsm.NoopHandler[string, string, string, string]
	calls    *[]string
	failures *[]fsm.Failure
}

func (h retrySelectionHandler) Enter(context.Context, fsm.ExecutionContext[string, string, string, string]) error {
	*h.calls = append(*h.calls, "enter")
	if len(*h.calls) == 1 {
		return errors.New("work-failed")
	}
	return nil
}

func (h retrySelectionHandler) Retry(_ context.Context, _ fsm.ExecutionContext[string, string, string, string], failure fsm.Failure) error {
	*h.calls = append(*h.calls, "retry")
	*h.failures = append(*h.failures, failure)
	if len(*h.failures) == 1 {
		return errors.New("retry-failed")
	}
	return nil
}

// RunRetrySelection checks failure recovery without reselecting state or policy.
func RunRetrySelection(t *testing.T, newStore func(*testing.T) ControlStore) {
	t.Helper()
	ctx := context.Background()
	store := newStore(t)
	clock := &timerClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	var calls []string
	var failures []fsm.Failure
	selections := 0
	policy := fsm.Policy{MaxAttempts: 5, AttemptTimeout: time.Minute, OverallTimeout: time.Hour, RetryDelays: []time.Duration{0}}
	machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
		ID: "machine", Version: "1", InitialStates: []string{"open"},
		Handlers: map[string]fsm.Handler[string, string, string, string]{"open": retrySelectionHandler{calls: &calls, failures: &failures}},
		SelectInitial: func(string) (fsm.Selection[string], error) {
			selections++
			return fsm.Selection[string]{Target: "open", Policy: policy}, nil
		},
		SelectTransition: func(string, string, string) (fsm.Selection[string], error) {
			return fsm.Selection[string]{}, errors.New("unexpected transition selection")
		},
		ClassifyFailure: func(err error) (fsm.Failure, fsm.Disposition) {
			return fsm.Failure{Code: err.Error()}, fsm.DispositionRetry
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
	acceptor, err := transition.NewAcceptor[string, string, string, string](store, &machines, resolve, resolve, func() (string, error) {
		return "execution", nil
	}, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = acceptor.Accept(ctx, transition.Request[string, string, string, string]{
		Entity: "entity", Key: "key", Operation: transition.Initialize, MachineID: "machine", MachineVersion: "1",
		Input: "payload", InputCodec: transition.CodecVersion{Name: "input", Version: "1"}, Notification: transition.NotifyInherit,
	})
	if err != nil {
		t.Fatal(err)
	}
	runner, err := transition.NewHookRunner[string, string, string, string](&machines, resolve, resolve)
	if err != nil {
		t.Fatal(err)
	}
	config := transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Hour, Clock: clock.Now}
	coordinator, err := transition.NewCoordinator[string, string](store, runner, config)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := transition.NewHookWorker[string, string, string, string](store, runner, transition.WorkerConfig{
		ID: "worker", Capacity: 1, LeaseTTL: time.Minute, RenewalInterval: time.Second, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	domain := &host{}
	commits, err := transition.NewCommitCoordinator[string, string](store, domain, config)
	if err != nil {
		t.Fatal(err)
	}
	for attempt := range 4 {
		if err := coordinator.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		if err := worker.RunOne(ctx); err != nil {
			t.Fatal(err)
		}
		if attempt < 3 {
			if err := commits.Reconcile(ctx); err != nil || domain.writes != 0 || domain.current != "" {
				t.Fatalf("failed initial work committed state: %+v, %v", domain, err)
			}
		}
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	progress, err := store.ReadHookProgress(ctx, "execution")
	if err != nil || len(progress.Attempts) != 4 ||
		progress.Attempts[0].Result == nil || progress.Attempts[0].Result.Failure.Code != "work-failed" ||
		progress.Attempts[1].Result == nil || progress.Attempts[1].Result.Failure.Code != "retry-failed" ||
		!reflect.DeepEqual(calls, []string{"enter", "retry", "retry", "enter"}) ||
		!reflect.DeepEqual(failures, []fsm.Failure{{Code: "work-failed"}, {Code: "work-failed"}}) ||
		progress.Attempts[1].Metadata.ActivityID != progress.Attempts[2].Metadata.ActivityID {
		t.Fatalf("failure or retry history changed: progress=%+v calls=%v failures=%v error=%v", progress, calls, failures, err)
	}
	record, err := store.ReadActiveExecution(ctx, "execution")
	if err != nil || record.Target != "open" || !reflect.DeepEqual(record.Policy, policy) || selections != 1 {
		t.Fatalf("retry changed selection or policy: %+v selections=%d error=%v", record, selections, err)
	}
	if err := commits.Reconcile(ctx); err != nil || domain.writes != 1 || domain.current != "open" {
		t.Fatalf("retried execution did not commit selected state: %+v, %v", domain, err)
	}
	completed, err := store.InspectExecution(ctx, "execution")
	if err != nil || completed.Status != transition.Complete {
		t.Fatalf("retried execution did not complete: %+v, %v", completed, err)
	}
}
