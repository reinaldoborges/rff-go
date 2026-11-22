package runtimecontract

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type compatibilityHandler struct {
	handler
}

func (h compatibilityHandler) Transition(_ context.Context, input fsm.ExecutionContext[string, string, string, string]) error {
	if input.Input() != "payload" {
		return errors.New("wrong rehydrated input")
	}
	*h.calls++
	return nil
}

// RunUnsupportedBehavior checks isolation and restoration of pinned dependencies.
func RunUnsupportedBehavior(t *testing.T, newStore func(*testing.T) CompatibilityStore) {
	t.Helper()
	for _, missing := range []transition.CompatibilityComponent{
		transition.CompatibilityHandler, transition.CompatibilityClassifier, transition.CompatibilityInputCodec, transition.CompatibilityEventCodec,
	} {
		t.Run(string(missing), func(t *testing.T) {
			ctx := context.Background()
			store := newStore(t)
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			clock := &timerClock{now: now}
			calls := 0
			target := "open"
			if missing == transition.CompatibilityHandler {
				target = "missing"
			}
			for _, item := range []struct{ id, version, target string }{{"a", "1", target}, {"b", "2", "open"}} {
				_, _, err := store.Accept(ctx, item.id, "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
					execution := &transition.Execution[string, string]{
						SchemaVersion: transition.CurrentSchemaVersion, ID: item.id, Entity: item.id, Key: "key",
						Operation: transition.Initialize, MachineID: "machine", MachineVersion: item.version, Target: item.target,
						Input:  transition.EncodedValue{Codec: transition.CodecVersion{Name: "input", Version: item.version}, Data: []byte("payload")},
						Status: transition.Waiting, AcceptedAt: now, Deadline: now.Add(time.Hour),
						Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
					}
					if missing == transition.CompatibilityEventCodec && item.id == "a" {
						execution.Operation, execution.Previous, execution.HasPrevious = transition.Transition, "open", true
						execution.Event = transition.EncodedValue{Codec: transition.CodecVersion{Name: "event", Version: "1"}, Data: []byte("start")}
					}
					return execution, nil
				})
				if err != nil {
					t.Fatal(err)
				}
				lease, err := store.AcquireTransitionLease(ctx, item.id, item.id, "coordinator", clock.Now, time.Hour)
				if err != nil {
					t.Fatal(err)
				}
				kind := transition.HookEnter
				if missing == transition.CompatibilityEventCodec && item.id == "a" {
					kind = transition.HookTransition
				}
				if _, err := store.DispatchHook(ctx, lease, clock.Now, transition.HookDispatch{Kind: kind, DueAt: now}); err != nil {
					t.Fatal(err)
				}
			}
			available := false
			resolve := func(ref transition.CodecVersion) (transition.Codec[string], error) {
				if !available && ref.Version == "1" &&
					(missing == transition.CompatibilityInputCodec && ref.Name == "input" ||
						missing == transition.CompatibilityEventCodec && ref.Name == "event") {
					return nil, transition.ErrCodecNotFound
				}
				return stringCodec{}, nil
			}
			newRunner := func(restored bool) *transition.HookRunner[string, string, string, string] {
				t.Helper()
				var registry fsm.Registry[string, string, string, string]
				for _, version := range []string{"1", "2"} {
					handlers := map[string]fsm.Handler[string, string, string, string]{"open": compatibilityHandler{handler{calls: &calls}}}
					if restored || missing != transition.CompatibilityHandler || version != "1" {
						handlers["missing"] = compatibilityHandler{handler{calls: &calls}}
					}
					classify := func(error) (fsm.Failure, fsm.Disposition) {
						return fsm.Failure{Code: "failed"}, fsm.DispositionIntervention
					}
					if !restored && missing == transition.CompatibilityClassifier && version == "1" {
						classify = nil
					}
					machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
						ID: "machine", Version: version, InitialStates: []string{"open"}, Handlers: handlers,
						SelectInitial: func(string) (fsm.Selection[string], error) { return fsm.Selection[string]{}, nil },
						SelectTransition: func(string, string, string) (fsm.Selection[string], error) {
							return fsm.Selection[string]{}, nil
						},
						ClassifyFailure: classify,
					})
					if err != nil {
						t.Fatal(err)
					}
					if err := registry.Register(machine); err != nil {
						t.Fatal(err)
					}
				}
				runner, err := transition.NewHookRunner[string, string, string, string](&registry, resolve, resolve)
				if err != nil {
					t.Fatal(err)
				}
				return runner
			}
			runner := newRunner(false)
			newWorker := func(runner *transition.HookRunner[string, string, string, string]) *transition.HookWorker[string, string, string, string] {
				t.Helper()
				worker, err := transition.NewHookWorker[string, string, string, string](store, runner, transition.WorkerConfig{
					ID: "worker", Capacity: 1, LeaseTTL: time.Minute, RenewalInterval: time.Second, Clock: clock,
				})
				if err != nil {
					t.Fatal(err)
				}
				return worker
			}
			worker := newWorker(runner)
			if err := worker.RunOne(ctx); !errors.Is(err, transition.ErrCompatibility) {
				t.Fatalf("missing %s was not reported: %v", missing, err)
			}
			blocked, err := store.ReadActiveExecution(ctx, "a")
			if err != nil || blocked.Status != transition.Waiting || blocked.Compatibility == nil || blocked.Compatibility.Component != missing {
				t.Fatalf("missing %s did not block affected work: %+v, %v", missing, blocked, err)
			}
			_, _, err = store.Accept(ctx, "a", "other", func(view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
				if !view.Active {
					t.Fatal("blocked execution lost its reservation")
				}
				return nil, transition.ErrBusy
			})
			if !errors.Is(err, transition.ErrBusy) {
				t.Fatalf("missing %s lost its reservation: %v", missing, err)
			}
			if err := worker.RunOne(ctx); err != nil || calls != 1 {
				t.Fatalf("missing %s blocked compatible work: calls=%d error=%v", missing, calls, err)
			}
			available = true
			runner = newRunner(true)
			manager, err := transition.NewCompatibilityCoordinator(store, runner, transition.CoordinatorConfig{
				ID: "coordinator", LeaseTTL: time.Hour, Clock: clock.Now,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := manager.Reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			restored, err := store.ReadActiveExecution(ctx, "a")
			if err != nil || restored.Status != transition.Waiting || restored.Compatibility != nil || !restored.Deadline.Equal(blocked.Deadline) {
				t.Fatalf("restoring %s changed phase or deadline: %+v, %v", missing, restored, err)
			}
			if err := newWorker(runner).RunOne(ctx); err != nil || calls != 2 {
				t.Fatalf("restoring %s did not resume work: calls=%d error=%v", missing, calls, err)
			}
		})
	}
}
