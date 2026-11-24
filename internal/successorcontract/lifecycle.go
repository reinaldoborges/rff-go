package successorcontract

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type lifecycleIdleStore struct {
	transition.LifecycleStore[string, string]
}

func (lifecycleIdleStore) ActiveExecutionIDs(context.Context) ([]string, error) { return nil, nil }
func (lifecycleIdleStore) ClaimHook(context.Context, string, func() time.Time, time.Duration) (transition.HookLease, error) {
	return transition.HookLease{}, transition.ErrNoHookActivity
}

type lifecycleIdleHost struct{}

func (lifecycleIdleHost) ContinuationCommitSupported() bool { return true }

func (lifecycleIdleHost) Commit(context.Context, transition.TransitionLease[string], transition.Execution[string, string]) (transition.CommitReceipt, error) {
	return transition.CommitReceipt{}, errors.New("unexpected commit")
}

type lifecycleDriverStore interface {
	DriverStore
	transition.ContinuationLifecycleStore[string, string]
}

// RunLifecycleContinuation exercises the supervisor against real durable
// successor and continuation stores after a lost acceptance response.
func RunLifecycleContinuation(t *testing.T, store lifecycleDriverStore, host *Host) {
	t.Helper()
	ctx := context.Background()
	marker := Setup(t, store, host, "lifecycle").Origin
	clock := func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }
	_, _, err := store.Accept(ctx, marker.Entity, "competing", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "competing", Entity: marker.Entity, Key: "competing",
			Operation: transition.Initialize, Target: "due", Status: transition.Waiting,
			MachineID: "plan", MachineVersion: "1", Policy: marker.Policy, AcceptedAt: clock(), Deadline: clock().Add(time.Hour),
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	competing, err := store.AcquireTransitionLease(ctx, marker.Entity, "competing", "owner", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, competing, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: clock()}); err != nil {
		t.Fatal(err)
	}
	hook, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, hook, clock); err != nil {
		t.Fatal(err)
	}
	if err := store.ReportHook(ctx, hook, transition.HookResult{Success: true}, clock); err != nil {
		t.Fatal(err)
	}
	operation, err := store.MarkWorkSuccess(ctx, competing, clock)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
		ID: "plan", Version: "1", InitialStates: []string{"due"},
		Handlers: map[string]fsm.Handler[string, string, string, string]{
			"due": fsm.NoopHandler[string, string, string, string]{}, "paid": fsm.NoopHandler[string, string, string, string]{},
		},
		ContinuationPolicies: map[string]fsm.Policy{"due": marker.Policy},
		SelectInitial: func(string) (fsm.Selection[string], error) {
			return fsm.Selection[string]{Target: "due", Policy: marker.Policy}, nil
		},
		SelectTransition: func(string, string, string) (fsm.Selection[string], error) {
			return fsm.Selection[string]{Target: "paid", Policy: marker.Policy}, nil
		},
		Continue: func(context.Context, string, string) (fsm.ContinuationDecision[string, string], error) {
			calls.Add(1)
			return fsm.ContinuationDecision[string, string]{Continue: true, Event: "pay", Input: "payload"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var registry fsm.Registry[string, string, string, string]
	if err := registry.Register(machine); err != nil {
		t.Fatal(err)
	}
	codec := func(transition.CodecVersion) (transition.Codec[string], error) { return stringCodec{}, nil }
	acceptor, err := transition.NewAcceptor(store, &registry, codec, codec, func() (string, error) { return "lifecycle-successor", nil }, clock)
	if err != nil {
		t.Fatal(err)
	}
	runner, err := transition.NewHookRunner(&registry, codec, codec)
	if err != nil {
		t.Fatal(err)
	}
	newLifecycle := func() *transition.Lifecycle[string, string, string, string] {
		l, err := transition.NewLifecycle[string, string, string, string](lifecycleIdleStore{}, runner, runner, lifecycleIdleHost{}, transition.LifecycleConfig{
			Coordinator: transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
			Worker: transition.WorkerConfig{ID: "worker", Capacity: 1, LeaseTTL: time.Second,
				RenewalInterval: 100 * time.Millisecond, Clock: transition.SystemWorkerClock{}},
			PollInterval:  10 * time.Millisecond,
			ClassifyError: func(error) transition.PathErrorClass { return transition.PathErrorRecoverable },
		})
		if err != nil {
			t.Fatal(err)
		}
		err = l.EnableContinuation(store, &registry, transition.ContinuationDriverConfig[string, string, string, string]{
			Clock: clock, LeaseDuration: time.Second,
			EventCodec: transition.CodecVersion{Name: "event", Version: "1"}, InputCodec: transition.CodecVersion{Name: "input", Version: "1"},
			ResolveEvent: codec, ResolveInput: codec, Acceptor: acceptor, Host: &driverHost{Host: host, clock: clock},
			OriginRevision: func(context.Context, transition.ContinuationMarker[string, string]) ([]byte, error) {
				return []byte("revision-1"), nil
			},
		}, nil, "continuation")
		if err != nil {
			t.Fatal(err)
		}
		return l
	}
	host.LoseReply = true
	l := newLifecycle()
	if err := l.Start(ctx); err != nil {
		t.Fatalf("reservation wait is not a path failure: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	var progress transition.ContinuationProgress
	for {
		progress, err = store.ReadContinuation(ctx, marker.ExecutionID)
		if err != nil || !progress.PausedAt.IsZero() || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	progress, err = store.ReadContinuation(ctx, marker.ExecutionID)
	if err != nil || progress.Attempts != 1 || progress.PausedAt.IsZero() || calls.Load() != 1 {
		t.Fatalf("reservation wait spent another attempt: %+v calls=%d err=%v", progress, calls.Load(), err)
	}
	if err := l.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	l = newLifecycle()
	if err := l.Start(ctx); err != nil {
		t.Fatal(err)
	}
	progress, err = store.ReadContinuation(ctx, marker.ExecutionID)
	if err != nil || progress.Attempts != 1 || progress.PausedAt.IsZero() || calls.Load() != 1 {
		t.Fatalf("restart spent a parked attempt: %+v calls=%d err=%v", progress, calls.Load(), err)
	}
	if err := store.RecordCommitReceipt(ctx, competing, clock, transition.CommitReceipt{
		ExecutionID: "competing", OperationID: operation, Outcome: transition.CommitSucceeded,
	}); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for {
		progress, err = store.ReadContinuation(ctx, marker.ExecutionID)
		if err != nil {
			t.Fatal(err)
		}
		if progress.Phase == transition.ContinuationAccepted {
			if progress.Attempts != 1 || calls.Load() != 1 {
				t.Fatalf("reconciliation duplicated work: %+v calls=%d", progress, calls.Load())
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lost response not reconciled: %+v, lifecycle=%+v, error=%v", progress, l.Status(), l.Err())
		}
		time.Sleep(time.Millisecond)
	}
	if l.Err() == nil {
		t.Fatal("lost acceptance response was not surfaced")
	}
	if err := l.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	l = newLifecycle()
	if err := l.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer l.Shutdown(ctx)
	progress, err = store.ReadContinuation(ctx, marker.ExecutionID)
	if err != nil || progress.Phase != transition.ContinuationAccepted || progress.Attempts != 1 || calls.Load() != 1 {
		t.Fatalf("restart duplicated accepted continuation: %+v calls=%d err=%v", progress, calls.Load(), err)
	}
}
