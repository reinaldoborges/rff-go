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
	transition.NotificationStore[string, string]
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
	t.Run("worker loss and stale fence", func(t *testing.T) {
		ctx := context.Background()
		store := newStore(t)
		now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		clock := func() time.Time { return now }
		_, _, err := store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			return &transition.Execution[string, string]{
				SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "key",
				Operation: transition.Initialize, Target: "open", Status: transition.Waiting,
				AcceptedAt: now, Deadline: now.Add(time.Hour), Policy: fsm.Policy{
					MaxAttempts: 3, AttemptTimeout: time.Minute, OverallTimeout: time.Hour,
					RetryDelays: []time.Duration{time.Second},
				},
			}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		coordinator, err := transition.NewCoordinator[string, string](store, unknownClassifier{}, transition.CoordinatorConfig{
			ID: "coordinator", LeaseTTL: time.Hour, Clock: clock,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		lost, err := store.ClaimHook(ctx, "lost-worker", clock, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		first, err := store.StartHook(ctx, lost, clock)
		if err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Second)
		if err := coordinator.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		progress, err := store.ReadHookProgress(ctx, "execution")
		if err != nil || len(progress.Attempts) != 1 || !progress.Attempts[0].Unknown || progress.BudgetUsed != 1 {
			t.Fatalf("lost attempt was not counted: %+v, %v", progress, err)
		}
		if err := coordinator.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Second)
		retry, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
		if err != nil || retry.ActivityID == lost.ActivityID {
			t.Fatalf("retry hook was not scheduled: %+v, %v", retry, err)
		}
		if _, err := store.StartHook(ctx, retry, clock); err != nil {
			t.Fatal(err)
		}
		if err := store.ReportHook(ctx, retry, transition.HookResult{Success: true}, clock); err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		redelivery, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
		if err != nil || redelivery.ActivityID != lost.ActivityID || redelivery.Generation <= lost.Generation {
			t.Fatalf("original activity was not fenced and redelivered: %+v, %v", redelivery, err)
		}
		second, err := store.StartHook(ctx, redelivery, clock)
		if err != nil || second.Attempt.Number <= first.Attempt.Number ||
			second.Attempt.ActivityIdempotencyKey != first.Attempt.ActivityIdempotencyKey {
			t.Fatalf("redelivery changed logical identity: first=%+v second=%+v error=%v", first, second, err)
		}
		if err := store.ReportHook(ctx, lost, transition.HookResult{Success: true}, clock); !errors.Is(err, transition.ErrStaleActivityLease) {
			t.Fatalf("stale worker report was accepted: %v", err)
		}
		if _, err := store.RenewHook(ctx, lost, clock, time.Minute); !errors.Is(err, transition.ErrStaleActivityLease) {
			t.Fatalf("stale worker renewal was accepted: %v", err)
		}
	})
	t.Run("definitive rejection recovers without commit", func(t *testing.T) {
		ctx := context.Background()
		store := newStore(t)
		now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		clock := func() time.Time { return now }
		_, _, err := store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			return &transition.Execution[string, string]{
				SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "key",
				Operation: transition.Initialize, Target: "open", Status: transition.Waiting, Notify: true,
				AcceptedAt: now, Deadline: now.Add(time.Hour), Policy: fsm.Policy{
					MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour,
				},
			}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		config := transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Hour, Clock: clock}
		coordinator, err := transition.NewCoordinator[string, string](store, unknownClassifier{}, config)
		if err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		work, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.StartHook(ctx, work, clock); err != nil {
			t.Fatal(err)
		}
		if err := store.ReportHook(ctx, work, transition.HookResult{Success: true}, clock); err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		commits, err := transition.NewCommitCoordinator[string, string](store, rejectionHost{}, config)
		if err != nil {
			t.Fatal(err)
		}
		if err := commits.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		record, err := store.InspectExecution(ctx, "execution")
		if err != nil || record.Status != transition.Aborting || record.CommitReceipt == nil ||
			record.CommitReceipt.Outcome != transition.CommitDenied || record.NotificationIntentID != "" {
			t.Fatalf("rejection did not enter recovery without notification: %+v, %v", record, err)
		}
		recovery, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		_, call, err := store.InspectHook(ctx, recovery, clock)
		if err != nil || call.Kind != transition.HookRecover || call.Failure == nil || call.Failure.Code != "invariant" {
			t.Fatalf("recovery did not receive rejection data: %+v, %v", call, err)
		}
		if _, err := store.StartHook(ctx, recovery, clock); err != nil {
			t.Fatal(err)
		}
		if err := store.ReportHook(ctx, recovery, transition.HookResult{Success: true}, clock); err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		record, err = store.InspectExecution(ctx, "execution")
		if err != nil || record.Status != transition.Aborted || record.CommitReceipt == nil ||
			record.CommitReceipt.Outcome != transition.CommitDenied {
			t.Fatalf("rejected execution was not durably aborted: %+v, %v", record, err)
		}
	})
	t.Run("notification handoff reconciles lost response", func(t *testing.T) {
		ctx := context.Background()
		store := newStore(t)
		now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		clock := func() time.Time { return now }
		_, _, err := store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			return &transition.Execution[string, string]{
				SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "key",
				Operation: transition.Initialize, Target: "open", Status: transition.Waiting, Notify: true,
				AcceptedAt: now, Deadline: now.Add(time.Hour), Policy: fsm.Policy{
					MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour,
				},
			}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		config := transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Hour, Clock: clock}
		coordinator, err := transition.NewCoordinator[string, string](store, unknownClassifier{}, config)
		if err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		work, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.StartHook(ctx, work, clock); err != nil {
			t.Fatal(err)
		}
		if err := store.ReportHook(ctx, work, transition.HookResult{Success: true}, clock); err != nil {
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
		handoff := &lostHandoff{}
		notifications, err := transition.NewNotificationCoordinator[string, string](store, handoff, config)
		if err != nil {
			t.Fatal(err)
		}
		if err := notifications.Reconcile(ctx); err == nil {
			t.Fatal("lost handoff response was not reported")
		}
		pending, err := store.InspectExecution(ctx, "execution")
		if err != nil || pending.Status != transition.Notifying || pending.NotificationIntentID == "" {
			t.Fatalf("unknown handoff prematurely completed: %+v, %v", pending, err)
		}
		if err := notifications.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		completed, err := store.InspectExecution(ctx, "execution")
		if err != nil || completed.Status != transition.Complete || handoff.writes != 1 ||
			handoff.intent.ID != pending.NotificationIntentID || domain.writes != 1 {
			t.Fatalf("handoff was duplicated or not completed: %+v handoff=%+v domain=%+v error=%v", completed, handoff, domain, err)
		}
	})
}

type unknownClassifier struct{}

func (unknownClassifier) ClassifyHookFailure(_ transition.Execution[string, string], cause error) (transition.HookResult, error) {
	if !errors.Is(cause, transition.ErrAttemptUnknown) {
		return transition.HookResult{}, cause
	}
	return transition.HookResult{Failure: fsm.Failure{Code: "unknown"}, Disposition: fsm.DispositionRetry}, nil
}

type rejectionHost struct{}

func (rejectionHost) Commit(_ context.Context, _ transition.TransitionLease[string],
	execution transition.Execution[string, string],
) (transition.CommitReceipt, error) {
	return transition.CommitReceipt{
		ExecutionID: execution.ID, OperationID: execution.CommitOperationID,
		Outcome: transition.CommitDenied, Failure: &fsm.Failure{Code: "invariant"},
	}, nil
}

type lostHandoff struct {
	intent *transition.NotificationIntent[string, string]
	writes int
}

func (h *lostHandoff) Handoff(_ context.Context, _ transition.TransitionLease[string], intent transition.NotificationIntent[string, string]) error {
	if h.intent != nil {
		if *h.intent != intent {
			return transition.ErrConflict
		}
		return nil
	}
	h.intent = &intent
	h.writes++
	return errors.New("lost handoff response")
}
