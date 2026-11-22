package sqlite_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
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
func (c versionedCodec) Equal(ctx context.Context, data []byte, incoming string) (bool, error) {
	value, err := c.Decode(ctx, data)
	return value == incoming, err
}

type versionedFailureHandler struct {
	fsm.NoopHandler[string, string, string, string]
	version string
	seen    map[string]string
}

func (h versionedFailureHandler) Enter(_ context.Context, input fsm.ExecutionContext[string, string, string, string]) error {
	h.seen[h.version] = input.Input()
	return errors.New("classified by pinned behavior")
}

func TestConcurrentMachineAndCodecVersionsStayPinned(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "versions.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	seen := make(map[string]string)
	var registry fsm.Registry[string, string, string, string]
	for _, version := range []string{"1", "2"} {
		version := version
		machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
			ID: "task", Version: version, InitialStates: []string{"open"},
			Handlers: map[string]fsm.Handler[string, string, string, string]{
				"open": versionedFailureHandler{version: version, seen: seen},
			},
			SelectInitial: func(string) (fsm.Selection[string], error) {
				return fsm.Selection[string]{Target: "open", Policy: fsm.Policy{
					MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour,
				}}, nil
			},
			SelectTransition: func(string, string, string) (fsm.Selection[string], error) {
				return fsm.Selection[string]{}, errors.New("unused transition")
			},
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
	}
	var codecs transition.CodecRegistry[string]
	for _, version := range []string{"1", "2"} {
		ref := transition.CodecVersion{Name: "input", Version: version}
		if err := codecs.Register(ref, versionedCodec{prefix: version + "|"}); err != nil {
			t.Fatal(err)
		}
		if err := codecs.Register(ref, versionedCodec{prefix: version + "|"}); !errors.Is(err, transition.ErrDuplicateCodec) {
			t.Fatalf("duplicate codec version was accepted: %v", err)
		}
	}
	resolve := codecs.Resolve
	id := 0
	acceptor, err := transition.NewAcceptor[string, string, string, string](store, &registry, resolve, resolve, func() (string, error) {
		id++
		return fmt.Sprintf("execution-%d", id), nil
	}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	for _, version := range []string{"1", "2"} {
		_, created, err := acceptor.Accept(ctx, transition.Request[string, string, string, string]{
			Entity: "entity-" + version, Key: "key", Operation: transition.Initialize, MachineID: "task", MachineVersion: version,
			Input: "payload-" + version, InputCodec: transition.CodecVersion{Name: "input", Version: version},
			Notification: transition.NotifyInherit,
		})
		if err != nil || !created {
			t.Fatalf("accept version %s: created=%v error=%v", version, created, err)
		}
	}
	runner, err := transition.NewHookRunner[string, string, string, string](&registry, resolve, resolve)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := transition.NewCoordinator[string, string](store, runner, transition.CoordinatorConfig{
		ID: "coordinator", LeaseTTL: time.Minute, Clock: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	worker, err := transition.NewHookWorker[string, string, string, string](store, runner, transition.WorkerConfig{
		ID: "worker", Capacity: 1, LeaseTTL: time.Minute, RenewalInterval: time.Second, Clock: transition.SystemWorkerClock{},
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
			t.Fatalf("wrong pinned behavior for version %s: progress=%+v seen=%v error=%v", version, progress, seen, err)
		}
	}
}
