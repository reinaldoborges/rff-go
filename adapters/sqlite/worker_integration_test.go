package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type stringValueCodec struct{}

func (stringValueCodec) Encode(_ context.Context, value string) ([]byte, error) {
	return []byte(value), nil
}
func (stringValueCodec) Decode(_ context.Context, data []byte) (string, error) {
	return string(data), nil
}
func (stringValueCodec) Equal(_ context.Context, data []byte, value string) (bool, error) {
	return string(data) == value, nil
}

type recordingEntry struct {
	fsm.NoopHandler[string, string, string, string]
	calls *int
}

func (h recordingEntry) Enter(_ context.Context, execution fsm.ExecutionContext[string, string, string, string]) error {
	if execution.Input() != "payload" {
		return errors.New("wrong input")
	}
	*h.calls++
	return nil
}

func TestSQLiteWorkerRunsAcceptedExecution(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "worker.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	calls, selections := 0, 0
	machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
		ID: "task", Version: "1", InitialStates: []string{"open"},
		Handlers: map[string]fsm.Handler[string, string, string, string]{"open": recordingEntry{calls: &calls}},
		SelectInitial: func(string) (fsm.Selection[string], error) {
			selections++
			return fsm.Selection[string]{Target: "open", Policy: fsm.Policy{
				MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour,
			}}, nil
		},
		SelectTransition: func(string, string, string) (fsm.Selection[string], error) {
			return fsm.Selection[string]{}, errors.New("unused selector")
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
	ref := transition.CodecVersion{Name: "string", Version: "1"}
	resolve := func(version transition.CodecVersion) (transition.Codec[string], error) {
		if version != ref {
			return nil, errors.New("unknown codec")
		}
		return stringValueCodec{}, nil
	}
	acceptor, err := transition.NewAcceptor[string, string, string, string](
		store, &registry, resolve, resolve, func() (string, error) { return "execution", nil }, time.Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	request := transition.Request[string, string, string, string]{
		Entity: "entity", Key: "key", Operation: transition.Initialize, MachineID: "task", MachineVersion: "1",
		Input: "payload", InputCodec: ref, Notification: transition.NotifyInherit,
	}
	execution, created, err := acceptor.Accept(ctx, request)
	if err != nil || !created {
		t.Fatalf("accept failed: created=%v error=%v", created, err)
	}
	fence, err := store.AcquireTransitionLease(ctx, execution.Entity, execution.ID, "coordinator", time.Now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, fence, time.Now, transition.HookDispatch{Kind: transition.HookEnter, DueAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	runner, err := transition.NewHookRunner[string, string, string, string](&registry, resolve, resolve)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := transition.NewHookWorker[string, string, string, string](store, runner, transition.WorkerConfig{
		ID: "worker", Capacity: 1, LeaseTTL: time.Minute, RenewalInterval: time.Second, Clock: transition.SystemWorkerClock{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.RunOne(ctx); err != nil {
		t.Fatal(err)
	}
	if err := worker.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	progress, err := store.ReadHookProgress(ctx, execution.ID)
	if err != nil || !progress.Completed || !progress.Result.Success || calls != 1 {
		t.Fatalf("durable work success missing: progress=%+v calls=%d error=%v", progress, calls, err)
	}
	acceptor, err = transition.NewAcceptor[string, string, string, string](
		store, &registry, resolve, resolve, func() (string, error) { return "other", nil }, time.Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	replayed, created, err := acceptor.Accept(ctx, request)
	if err != nil || created || replayed.ID != execution.ID || replayed.Status != transition.Running || selections != 1 {
		t.Fatalf("reopened replay changed execution or reran selection: record=%+v created=%v selections=%d error=%v", replayed, created, selections, err)
	}
}
