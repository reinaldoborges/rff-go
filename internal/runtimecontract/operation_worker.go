package runtimecontract

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type concurrentOperationHandler struct {
	fsm.NoopHandler[string, string, string, string]
	started chan string
	release <-chan struct{}
}

type lostOperationReplyStore struct {
	OperationStore
	lost atomic.Bool
}

func (s *lostOperationReplyStore) CompleteTrackedOperation(ctx context.Context, lease transition.HookLease, clock func() time.Time, identity transition.OperationIdentity, result transition.OperationResult) error {
	err := s.OperationStore.CompleteTrackedOperation(ctx, lease, clock, identity, result)
	if err == nil && s.lost.CompareAndSwap(false, true) {
		return errors.New("injected lost persistence reply")
	}
	return err
}

func (h concurrentOperationHandler) Enter(ctx context.Context, _ fsm.ExecutionContext[string, string, string, string]) error {
	codec := transition.CodecVersion{Name: "result", Version: "1"}
	req := transition.OperationRequest{
		Kind: "read", BehaviorVersion: "1", ComparatorVersion: "1", ResultCodec: codec,
		Preflight:      func(context.Context) error { return nil },
		ValidateInput:  func(context.Context, []byte) error { return nil },
		EqualInput:     func(_ context.Context, a, b []byte) (bool, error) { return bytes.Equal(a, b), nil },
		ValidateResult: func(context.Context, transition.OperationResult) error { return nil },
	}
	done := make(chan error, 2)
	for _, key := range []string{"record-a:read", "record-b:read"} {
		go func() {
			request := req
			request.Key = key
			_, err := transition.TrackOperation(ctx, request, func(ctx context.Context, id transition.OperationIdentity) (transition.OperationResult, error) {
				h.started <- id.Key
				select {
				case <-h.release:
					return transition.OperationResult{Codec: codec, Data: []byte(id.Key)}, nil
				case <-ctx.Done():
					return transition.OperationResult{}, ctx.Err()
				}
			})
			done <- err
		}()
	}
	if err := errors.Join(<-done, <-done); err != nil {
		return err
	}
	req.Key = "known-decline"
	result, err := transition.TrackOperation(ctx, req, func(context.Context, transition.OperationIdentity) (transition.OperationResult, error) {
		return transition.OperationResult{Codec: codec, FailureCode: "declined"}, nil
	})
	if !errors.Is(err, transition.ErrOperationFailed) || result.FailureCode != "declined" {
		return errors.New("definitive failure was not available to the hook")
	}
	return nil
}

// RunConcurrentOperations checks distinct identities can dispatch concurrently in one hook.
func RunConcurrentOperations(t *testing.T, newStore func(*testing.T) OperationStore) {
	t.Helper()
	store := newStore(t)
	started := make(chan string, 2)
	release := make(chan struct{})
	handler := concurrentOperationHandler{started: started, release: release}
	machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
		ID: "machine", Version: "1", InitialStates: []string{"open"},
		Handlers: map[string]fsm.Handler[string, string, string, string]{"open": handler},
		SelectInitial: func(string) (fsm.Selection[string], error) {
			return fsm.Selection[string]{Target: "open", Policy: fsm.Policy{MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour}}, nil
		},
		SelectTransition: func(string, string, string) (fsm.Selection[string], error) {
			return fsm.Selection[string]{}, errors.New("unused")
		},
		ClassifyFailure: func(error) (fsm.Failure, fsm.Disposition) {
			return fsm.Failure{Code: "failed"}, fsm.DispositionIntervention
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var registry fsm.Registry[string, string, string, string]
	if err := registry.Register(machine); err != nil {
		t.Fatal(err)
	}
	resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return stringCodec{}, nil }
	acceptor, err := transition.NewAcceptor[string, string, string, string](store, &registry, resolve, resolve, func() (string, error) { return "execution", nil }, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = acceptor.Accept(context.Background(), transition.Request[string, string, string, string]{
		Entity: "entity", Key: "request", Operation: transition.Initialize, MachineID: "machine", MachineVersion: "1",
		Input: "payload", InputCodec: transition.CodecVersion{Name: "string", Version: "1"}, Notification: transition.NotifyInherit,
	})
	if err != nil {
		t.Fatal(err)
	}
	runner, err := transition.NewHookRunner[string, string, string, string](&registry, resolve, resolve)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := transition.NewCoordinator[string, string](store, runner, transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Hour, Clock: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	wrapped := &lostOperationReplyStore{OperationStore: store}
	worker, err := transition.NewHookWorker[string, string, string, string](wrapped, runner, transition.WorkerConfig{
		ID: "worker", Capacity: 1, LeaseTTL: time.Minute, RenewalInterval: time.Second, Clock: transition.SystemWorkerClock{},
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- worker.RunOne(context.Background()) }()
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			close(release)
			<-done
			t.Fatal("distinct operations did not dispatch concurrently")
		}
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !wrapped.lost.Load() {
		t.Fatal("result persistence response fault was not exercised")
	}
	progress, err := store.ReadHookProgress(context.Background(), "execution")
	if err != nil || !progress.Completed || !progress.Result.Success {
		t.Fatalf("concurrent operations did not finish: %+v, %v", progress, err)
	}
	if err := coordinator.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	execution, err := store.ReadActiveExecution(context.Background(), "execution")
	if err != nil || execution.CommitOperationID == "" {
		t.Fatalf("handled definitive failure blocked work success: %+v, %v", execution, err)
	}
}
