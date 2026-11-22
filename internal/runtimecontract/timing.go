package runtimecontract

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type expiredAttemptClock struct{ *timerClock }

func (*expiredAttemptClock) WithTimeout(ctx context.Context, _ time.Duration) (context.Context, context.CancelFunc) {
	return context.WithDeadline(ctx, time.Unix(1, 0))
}

// RunTiming checks attempt and overall deadline handling through runtime ports.
func RunTiming(t *testing.T, newStore func(*testing.T) Store) {
	t.Helper()
	for _, overall := range []bool{false, true} {
		name := "attempt timeout"
		if overall {
			name = "overall deadline"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := newStore(t)
			clock := &timerClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
			now := clock.Now()
			_, _, err := store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
				return &transition.Execution[string, string]{
					SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "key",
					Operation: transition.Initialize, MachineID: "machine", MachineVersion: "1", Target: "open",
					Input:  transition.EncodedValue{Codec: transition.CodecVersion{Name: "input", Version: "1"}, Data: []byte("payload")},
					Status: transition.Waiting, AcceptedAt: now, Deadline: now.Add(time.Hour),
					Policy: fsm.Policy{MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
				}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
				ID: "machine", Version: "1", InitialStates: []string{"open"},
				Handlers:      map[string]fsm.Handler[string, string, string, string]{"open": fsm.NoopHandler[string, string, string, string]{}},
				SelectInitial: func(string) (fsm.Selection[string], error) { return fsm.Selection[string]{}, nil },
				SelectTransition: func(string, string, string) (fsm.Selection[string], error) {
					return fsm.Selection[string]{}, nil
				},
				ClassifyFailure: func(err error) (fsm.Failure, fsm.Disposition) {
					if errors.Is(err, transition.ErrTransitionDeadline) {
						return fsm.Failure{Code: "overall-deadline"}, fsm.DispositionIntervention
					}
					if errors.Is(err, context.DeadlineExceeded) {
						return fsm.Failure{Code: "attempt-timeout"}, fsm.DispositionIntervention
					}
					return fsm.Failure{Code: "unexpected"}, fsm.DispositionIntervention
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
			runner, err := transition.NewHookRunner[string, string, string, string](&machines, resolve, resolve)
			if err != nil {
				t.Fatal(err)
			}
			coordinator, err := transition.NewCoordinator[string, string](store, runner, transition.CoordinatorConfig{
				ID: "coordinator", LeaseTTL: 2 * time.Hour, Clock: clock.Now,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := coordinator.Reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			worker, err := transition.NewHookWorker[string, string, string, string](store, runner, transition.WorkerConfig{
				ID: "worker", Capacity: 1, LeaseTTL: time.Minute, RenewalInterval: time.Second,
				Clock: &expiredAttemptClock{clock},
			})
			if err != nil {
				t.Fatal(err)
			}
			if overall {
				clock.advance(time.Hour)
				if err := worker.RunOne(ctx); !errors.Is(err, transition.ErrTransitionDeadline) {
					t.Fatalf("overall deadline did not prevent an attempt: %v", err)
				}
			} else if err := worker.RunOne(ctx); err != nil {
				t.Fatal(err)
			}
			if err := coordinator.Reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			execution, err := store.InspectExecution(ctx, "execution")
			want := "attempt-timeout"
			attempts := 1
			if overall {
				want, attempts = "overall-deadline", 0
			}
			if err != nil || execution.Status != transition.NeedsIntervention || execution.Failure == nil || execution.Failure.Code != want {
				t.Fatalf("timing failure was not classified: %+v, %v", execution, err)
			}
			progress, err := store.ReadHookProgress(ctx, "execution")
			if err != nil || len(progress.Attempts) != attempts {
				t.Fatalf("timing failure consumed the wrong attempts: %+v, %v", progress, err)
			}
		})
	}
}
