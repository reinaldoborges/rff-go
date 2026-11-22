package runtimecontract

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type versionedCodec struct{ prefix string }

func (c versionedCodec) Encode(_ context.Context, value string) ([]byte, error) {
	return []byte(c.prefix + value), nil
}

func (c versionedCodec) Decode(_ context.Context, data []byte) (string, error) {
	value := string(data)
	if !strings.HasPrefix(value, c.prefix) {
		return "", errors.New("wrong codec version")
	}
	return strings.TrimPrefix(value, c.prefix), nil
}

func (c versionedCodec) Equal(ctx context.Context, data []byte, value string) (bool, error) {
	decoded, err := c.Decode(ctx, data)
	return decoded == value, err
}

type versionedHandler struct {
	fsm.NoopHandler[string, string, string, string]
	version string
	seen    map[string]string
}

func (h versionedHandler) Enter(_ context.Context, input fsm.ExecutionContext[string, string, string, string]) error {
	h.seen[h.version] = input.Input()
	return errors.New("classify with pinned behavior")
}

// RunVersions checks concurrent pinned machines, codecs, hooks and classifiers.
func RunVersions(t *testing.T, newStore func(*testing.T) Store) {
	t.Helper()
	store := newStore(t)
	ctx := context.Background()
	clock := &timerClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	seen := make(map[string]string)
	var registry fsm.Registry[string, string, string, string]
	var codecs transition.CodecRegistry[string]
	for _, version := range []string{"1", "2"} {
		machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
			ID: "machine", Version: version, InitialStates: []string{"open"},
			Handlers: map[string]fsm.Handler[string, string, string, string]{
				"open": versionedHandler{version: version, seen: seen},
			},
			SelectInitial: func(string) (fsm.Selection[string], error) {
				return fsm.Selection[string]{Target: "open", Policy: fsm.Policy{
					MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour,
				}}, nil
			},
			SelectTransition: func(string, string, string) (fsm.Selection[string], error) { return fsm.Selection[string]{}, nil },
			ClassifyFailure: func(error) (fsm.Failure, fsm.Disposition) {
				return fsm.Failure{Code: "version-" + version}, fsm.DispositionIntervention
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := registry.Register(machine); err != nil {
			t.Fatal(err)
		}
		if err := registry.Register(machine); !errors.Is(err, fsm.ErrDuplicateMachine) {
			t.Fatalf("duplicate machine version was accepted: %v", err)
		}
		ref := transition.CodecVersion{Name: "input", Version: version}
		if err := codecs.Register(ref, versionedCodec{prefix: version + "|"}); err != nil {
			t.Fatal(err)
		}
		if err := codecs.Register(ref, versionedCodec{prefix: version + "|"}); !errors.Is(err, transition.ErrDuplicateCodec) {
			t.Fatalf("duplicate codec version was accepted: %v", err)
		}
	}
	nextID := 0
	acceptor, err := transition.NewAcceptor[string, string, string, string](store, &registry, codecs.Resolve, codecs.Resolve,
		func() (string, error) {
			nextID++
			return fmt.Sprintf("execution-%d", nextID), nil
		}, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	for _, version := range []string{"1", "2"} {
		_, created, err := acceptor.Accept(ctx, transition.Request[string, string, string, string]{
			Entity: "entity-" + version, Key: "key", Operation: transition.Initialize, MachineID: "machine", MachineVersion: version,
			Input: "payload-" + version, InputCodec: transition.CodecVersion{Name: "input", Version: version},
			Notification: transition.NotifyInherit,
		})
		if err != nil || !created {
			t.Fatalf("accept version %s: created=%v error=%v", version, created, err)
		}
	}
	runner, err := transition.NewHookRunner[string, string, string, string](&registry, codecs.Resolve, codecs.Resolve)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := transition.NewCoordinator[string, string](store, runner, transition.CoordinatorConfig{
		ID: "coordinator", LeaseTTL: time.Hour, Clock: clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	worker, err := transition.NewHookWorker[string, string, string, string](store, runner, transition.WorkerConfig{
		ID: "worker", Capacity: 1, LeaseTTL: time.Minute, RenewalInterval: time.Second, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, version := range []string{"1", "2"} {
		if err := worker.RunOne(ctx); err != nil {
			t.Fatal(err)
		}
		progress, err := store.ReadHookProgress(ctx, "execution-"+version)
		if err != nil || !progress.Completed || progress.Result.Failure.Code != "version-"+version || seen[version] != "payload-"+version {
			t.Fatalf("pinned version behaved incorrectly: progress=%+v seen=%v error=%v", progress, seen, err)
		}
	}
}
