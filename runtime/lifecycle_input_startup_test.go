package runtime_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type cancelClaimStore struct {
	lifecycleTestStore
	entered  chan struct{}
	canceled chan struct{}
}

func (s *cancelClaimStore) ClaimHook(ctx context.Context, _ string, _ func() time.Time, _ time.Duration) (transition.HookLease, error) {
	close(s.entered)
	<-ctx.Done()
	close(s.canceled)
	return transition.HookLease{}, ctx.Err()
}

func TestLifecycleInputStartupCancellationStopsClaimsImmediately(t *testing.T) {
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "input.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var pending atomic.Bool
	pending.Store(true)
	entered := make(chan struct{})
	release := make(chan struct{})
	ingress, err := transition.NewInputIngress[string](store, "blocked",
		func(transition.CodecVersion) (transition.Codec[string], error) { return lifecycleInputCodec{}, nil },
		func(context.Context, transition.InputDelivery[string]) (transition.InputOutcome, error) {
			if pending.Load() {
				return transition.InputOutcome{}, errors.New("pending")
			}
			close(entered)
			<-release
			return transition.InputOutcome{}, errors.New("unfinished")
		})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = ingress.Submit(context.Background(), transition.InputRequest[string]{ID: "id", WorkflowID: "order", Kind: transition.Command, Value: "buy", Codec: transition.CodecVersion{Name: "order", Version: "1"}})
	pending.Store(false)
	scans := &cancelClaimStore{entered: make(chan struct{}), canceled: make(chan struct{})}
	runner, _, _ := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
	l, err := transition.NewLifecycle[string, string, string, int](scans, runner, runner, lifecycleTestHost{}, transition.LifecycleConfig{
		Coordinator:  transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
		Worker:       transition.WorkerConfig{ID: "worker", Capacity: 1, LeaseTTL: time.Second, RenewalInterval: time.Millisecond, Clock: transition.SystemWorkerClock{}},
		PollInterval: 10 * time.Millisecond, Inputs: []transition.LifecycleInput{ingress},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan error, 1)
	go func() { started <- l.Start(ctx) }()
	defer func() { cancel(); close(release); <-started; _ = l.Shutdown(context.Background()) }()
	for _, ch := range []<-chan struct{}{entered, scans.entered} {
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatal("startup path did not start")
		}
	}
	cancel()
	select {
	case <-scans.canceled:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("startup cancellation left a worker claim running")
	}
}
