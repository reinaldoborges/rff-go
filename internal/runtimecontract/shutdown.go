package runtimecontract

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type shutdownTicker struct{ ticks chan time.Time }

func (t shutdownTicker) Ticks() <-chan time.Time { return t.ticks }
func (shutdownTicker) Stop()                     {}

type shutdownClock struct {
	timerClock
	ticks chan time.Time
}

func (c *shutdownClock) NewTicker(time.Duration) transition.WorkerTicker {
	return shutdownTicker{c.ticks}
}

func (*shutdownClock) WithTimeout(ctx context.Context, _ time.Duration) (context.Context, context.CancelFunc) {
	return context.WithCancel(ctx)
}

type renewalObserver struct {
	Store
	renewed chan error
}

func (s renewalObserver) RenewHook(ctx context.Context, lease transition.HookLease, clock func() time.Time, ttl time.Duration) (transition.HookLease, error) {
	renewed, err := s.Store.RenewHook(ctx, lease, clock, ttl)
	s.renewed <- err
	return renewed, err
}

type shutdownHandler struct {
	fsm.NoopHandler[string, string, string, string]
	attempts chan fsm.AttemptMetadata
}

func (h shutdownHandler) Enter(ctx context.Context, execution fsm.ExecutionContext[string, string, string, string]) error {
	attempt := execution.Attempt()
	h.attempts <- attempt
	if attempt.Number == 1 {
		<-ctx.Done()
	}
	return nil
}

// RunShutdownRecovery checks shutdown, lease expiry and redelivery through runtime ports.
func RunShutdownRecovery(t *testing.T, newStore func(*testing.T) Store) {
	t.Helper()
	ctx := context.Background()
	store := newStore(t)
	clock := &shutdownClock{timerClock: timerClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}, ticks: make(chan time.Time, 1)}
	attempts := make(chan fsm.AttemptMetadata, 2)
	selections := 0
	machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
		ID: "machine", Version: "1", InitialStates: []string{"open"},
		Handlers: map[string]fsm.Handler[string, string, string, string]{"open": shutdownHandler{attempts: attempts}},
		SelectInitial: func(string) (fsm.Selection[string], error) {
			selections++
			return fsm.Selection[string]{Target: "open", Policy: fsm.Policy{
				MaxAttempts: 3, AttemptTimeout: time.Minute, OverallTimeout: time.Hour,
				RetryDelays: []time.Duration{0},
			}}, nil
		},
		SelectTransition: func(string, string, string) (fsm.Selection[string], error) {
			return fsm.Selection[string]{}, errors.New("unexpected transition")
		},
		ClassifyFailure: func(error) (fsm.Failure, fsm.Disposition) {
			return fsm.Failure{Code: "unknown"}, fsm.DispositionRetry
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
		func() (string, error) { return "execution", nil }, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := acceptor.Accept(ctx, transition.Request[string, string, string, string]{
		Entity: "entity", Key: "key", Operation: transition.Initialize, MachineID: "machine", MachineVersion: "1",
		Input: "payload", InputCodec: ref, Notification: transition.NotifyInherit,
	}); err != nil {
		t.Fatal(err)
	}
	runner, err := transition.NewHookRunner[string, string, string, string](&machines, codecs.Resolve, codecs.Resolve)
	if err != nil {
		t.Fatal(err)
	}
	config := transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Hour, Clock: clock.Now}
	coordinator, err := transition.NewCoordinator[string, string](store, runner, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	observed := renewalObserver{Store: store, renewed: make(chan error, 1)}
	worker, err := transition.NewHookWorker[string, string, string, string](observed, runner, transition.WorkerConfig{
		ID: "first-worker", Capacity: 1, LeaseTTL: time.Minute, RenewalInterval: time.Second, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	workCtx, cancelWork := context.WithCancel(ctx)
	t.Cleanup(cancelWork)
	workDone := make(chan error, 1)
	go func() { workDone <- worker.RunOne(workCtx) }()
	var first fsm.AttemptMetadata
	select {
	case first = <-attempts:
	case err := <-workDone:
		t.Fatalf("worker stopped before starting the hook: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not start the hook")
	}
	shutdownCtx, cancelShutdown := context.WithCancel(ctx)
	t.Cleanup(cancelShutdown)
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- worker.Shutdown(shutdownCtx) }()
	timeout := time.After(5 * time.Second)
	for !worker.Status().Draining {
		select {
		case <-timeout:
			t.Fatal("worker did not begin draining")
		default:
			runtime.Gosched()
		}
	}
	if err := worker.RunOne(ctx); !errors.Is(err, transition.ErrWorkerDraining) {
		t.Fatalf("draining worker accepted work: %v", err)
	}
	clock.advance(30 * time.Second)
	clock.ticks <- clock.Now()
	select {
	case err := <-observed.renewed:
		if err != nil {
			t.Fatalf("lease renewal during drain failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not renew during drain")
	}
	cancelShutdown()
	if err := <-shutdownDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown did not report cancellation: %v", err)
	}
	if err := <-workDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown did not cancel active work: %v", err)
	}
	progress, err := store.ReadHookProgress(ctx, "execution")
	if err != nil || len(progress.Attempts) != 1 || progress.Attempts[0].Result != nil {
		t.Fatalf("shutdown fabricated a hook outcome: %+v, %v", progress, err)
	}
	clock.advance(30 * time.Second)
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	progress, err = store.ReadHookProgress(ctx, "execution")
	if err != nil || len(progress.Attempts) != 1 || progress.Attempts[0].Unknown {
		t.Fatalf("renewed lease expired early: %+v, %v", progress, err)
	}
	clock.advance(30 * time.Second)
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	progress, err = store.ReadHookProgress(ctx, "execution")
	if err != nil || len(progress.Attempts) != 1 || !progress.Attempts[0].Unknown {
		t.Fatalf("cancelled attempt was not recovered as unknown: %+v, %v", progress, err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	replacement, err := transition.NewHookWorker[string, string, string, string](store, runner, transition.WorkerConfig{
		ID: "replacement", Capacity: 1, LeaseTTL: time.Minute, RenewalInterval: time.Second, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := replacement.RunOne(ctx); err != nil {
		t.Fatalf("retry hook: %v", err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if err := replacement.RunOne(ctx); err != nil {
		t.Fatalf("redelivered hook: %v", err)
	}
	var second fsm.AttemptMetadata
	select {
	case second = <-attempts:
	default:
		t.Fatal("replacement worker did not redeliver the entry hook")
	}
	if second.Number <= first.Number || second.ActivityIdempotencyKey != first.ActivityIdempotencyKey ||
		second.LeaseGeneration <= first.LeaseGeneration {
		t.Fatalf("redelivery changed identity or failed to fence the old worker: first=%+v second=%+v", first, second)
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
	record, err := store.InspectExecution(ctx, "execution")
	if err != nil || record.Status != transition.Complete || domain.writes != 1 || domain.current != "open" || selections != 1 {
		t.Fatalf("recovered execution changed selection or commit: %+v host=%+v selections=%d error=%v", record, domain, selections, err)
	}
}
