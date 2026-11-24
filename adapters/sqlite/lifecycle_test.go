package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type lifecycleHandler struct {
	fsm.NoopHandler[string, string, string, string]
	started chan string
	blockA  <-chan struct{}
	blockB  <-chan struct{}
}

var errLifecycleStoreUnavailable = errors.New("temporary lifecycle store failure")

type lifecycleFaultStore struct {
	*sqlite.Store[string, string]
	failScan atomic.Bool
}

func (s *lifecycleFaultStore) ActiveExecutionIDs(ctx context.Context) ([]string, error) {
	if s.failScan.Load() {
		return nil, errLifecycleStoreUnavailable
	}
	return s.Store.ActiveExecutionIDs(ctx)
}

func (h lifecycleHandler) Enter(_ context.Context, e fsm.ExecutionContext[string, string, string, string]) error {
	h.started <- e.EntityID()
	switch e.EntityID() {
	case "a":
		<-h.blockA // Deliberately ignores cancellation.
	case "b":
		<-h.blockB
	}
	return nil
}

func TestEmbeddedLifecycleDurableProgressAndShutdown(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "lifecycle.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE domain_state (entity TEXT PRIMARY KEY, state TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	started, releaseA, releaseB := make(chan string, 8), make(chan struct{}), make(chan struct{})
	var closeA, closeB sync.Once
	t.Cleanup(func() {
		closeA.Do(func() { close(releaseA) })
		closeB.Do(func() { close(releaseB) })
	})
	machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
		ID: "task", Version: "1", InitialStates: []string{"open"},
		Handlers: map[string]fsm.Handler[string, string, string, string]{"open": lifecycleHandler{started: started, blockA: releaseA, blockB: releaseB}},
		SelectInitial: func(string) (fsm.Selection[string], error) {
			return fsm.Selection[string]{Target: "open", Policy: fsm.Policy{MaxAttempts: 2, AttemptTimeout: 10 * time.Second, OverallTimeout: time.Minute}}, nil
		},
		SelectTransition: func(string, string, string) (fsm.Selection[string], error) {
			return fsm.Selection[string]{}, errors.New("unused")
		},
		ClassifyFailure: func(error) (fsm.Failure, fsm.Disposition) {
			return fsm.Failure{Code: "unknown"}, fsm.DispositionIntervention
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
	resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return stringValueCodec{}, nil }
	runner, err := transition.NewHookRunner[string, string, string, string](&registry, resolve, resolve)
	if err != nil {
		t.Fatal(err)
	}
	newAcceptor := func() *transition.Acceptor[string, string, string, string] {
		n := 0
		a, err := transition.NewAcceptor[string, string, string, string](store, &registry, resolve, resolve, func() (string, error) {
			n++
			return time.Now().Format(time.RFC3339Nano) + string(rune('a'+n)), nil
		}, time.Now)
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	acceptor := newAcceptor()
	accept := func(entity string) {
		t.Helper()
		_, created, err := acceptor.Accept(ctx, transition.Request[string, string, string, string]{
			Entity: entity, Key: entity, Operation: transition.Initialize, MachineID: "task", MachineVersion: "1",
			Input: "payload", InputCodec: ref, Notification: transition.NotifyDisable,
		})
		if err != nil || !created {
			t.Fatalf("accept %s: created=%v error=%v", entity, created, err)
		}
	}
	accept("a")
	accept("b")
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	acceptor = newAcceptor()
	newLifecycle := func(faultStore *lifecycleFaultStore) *transition.Lifecycle[string, string, string, string] {
		t.Helper()
		host := &localCommitHost{store: store, clock: time.Now}
		l, err := transition.NewLifecycle[string, string, string, string](faultStore, runner, runner, host, transition.LifecycleConfig{
			Coordinator: transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: 300 * time.Millisecond, Clock: time.Now},
			Worker: transition.WorkerConfig{ID: "worker", Capacity: 2, LeaseTTL: 150 * time.Millisecond,
				RenewalInterval: 30 * time.Millisecond, Clock: transition.SystemWorkerClock{}},
			PollInterval: 20 * time.Millisecond,
			ClassifyError: func(err error) transition.PathErrorClass {
				if errors.Is(err, errLifecycleStoreUnavailable) {
					return transition.PathErrorRecoverable
				}
				return transition.PathErrorUnclassified
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return l
	}
	faultStore := &lifecycleFaultStore{Store: store}
	faultStore.failScan.Store(true)
	l := newLifecycle(faultStore)
	if err := l.Start(ctx); !errors.Is(err, errLifecycleStoreUnavailable) {
		t.Fatalf("initial failure not surfaced: %v", err)
	}
	if l.Status().Ready {
		t.Fatal("failed initial scan marked ready")
	}
	faultStore.failScan.Store(false)
	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case id := <-started:
			seen[id] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("concurrent hooks did not start: %v, error %v", seen, l.Err())
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for !l.Status().Ready && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if status := l.Status(); !status.Ready || status.Capacity.Available != 0 {
		t.Fatalf("busy hooks changed readiness or capacity incorrectly: %+v", status)
	}
	accept("c") // No wakeup: polling must discover it.
	closeB.Do(func() { close(releaseB) })
	select {
	case id := <-started:
		if id != "c" {
			t.Fatalf("unexpected hook %q", id)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("polling did not discover c: %v", l.Err())
	}
	waitComplete := func(entity string) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			execution, _, err := acceptor.Accept(ctx, transition.Request[string, string, string, string]{
				Entity: entity, Key: entity, Operation: transition.Initialize, MachineID: "task", MachineVersion: "1",
				Input: "payload", InputCodec: ref, Notification: transition.NotifyDisable,
			})
			if err == nil && execution.Status == transition.Complete {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("%s did not commit: %v", entity, l.Err())
	}
	waitComplete("b")
	waitComplete("c")
	shutdown, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
	defer cancel()
	if err := l.Shutdown(shutdown); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unfinished hook not reported: %v", err)
	}
	old := l
	accept("d")
	time.Sleep(200 * time.Millisecond)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	acceptor = newAcceptor()
	l = newLifecycle(&lifecycleFaultStore{Store: store})
	if err := l.Start(ctx); err != nil {
		t.Fatal(err)
	}
	waitComplete("d")
	deadline = time.Now().Add(5 * time.Second)
	for {
		execution, _, err := acceptor.Accept(ctx, transition.Request[string, string, string, string]{
			Entity: "a", Key: "a", Operation: transition.Initialize, MachineID: "task", MachineVersion: "1",
			Input: "payload", InputCodec: ref, Notification: transition.NotifyDisable,
		})
		if err == nil && execution.Status == transition.NeedsIntervention {
			progress, err := store.ReadHookProgress(ctx, execution.ID)
			if err != nil || len(progress.Attempts) != 1 || !progress.Attempts[0].Unknown || progress.BudgetUsed != 1 {
				t.Fatalf("unfinished attempt was not retained as unknown: %+v %v", progress, err)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("unfinished attempt was not recovered: %+v %v, lifecycle: %v", execution, err, l.Err())
		}
		time.Sleep(20 * time.Millisecond)
	}
	closeA.Do(func() { close(releaseA) })
	if err := old.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := l.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := l.Err(); err != nil {
		t.Fatal(err)
	}
	db, err = sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM domain_state WHERE entity IN ('b', 'c', 'd') AND state = 'open'`).Scan(&count); err != nil || count != 3 {
		t.Fatalf("domain commits missing: count=%d error=%v", count, err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM domain_state WHERE entity = 'a'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("unfinished hook committed: count=%d error=%v", count, err)
	}
}
