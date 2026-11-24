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

type notificationLifecycleHandler struct {
	fsm.NoopHandler[string, string, string, string]
	calls *atomic.Int64
}

func (h notificationLifecycleHandler) Enter(context.Context, fsm.ExecutionContext[string, string, string, string]) error {
	h.calls.Add(1)
	return nil
}

type notificationLifecycleCommit struct{ db *sql.DB }

type lostLifecycleHandoff struct {
	transition.NotificationHandoff[string, string]
	lost atomic.Bool
}

var errLostLifecycleHandoff = errors.New("lost handoff response")

func (h *lostLifecycleHandoff) Handoff(ctx context.Context, lease transition.TransitionLease[string], intent transition.NotificationIntent[string, string]) error {
	if err := h.NotificationHandoff.Handoff(ctx, lease, intent); err != nil {
		return err
	}
	if h.lost.CompareAndSwap(false, true) {
		return errLostLifecycleHandoff
	}
	return nil
}

func (h notificationLifecycleCommit) Commit(ctx context.Context, _ transition.TransitionLease[string], e transition.Execution[string, string]) (transition.CommitReceipt, error) {
	_, err := h.db.ExecContext(ctx, `INSERT OR IGNORE INTO domain_state (entity, state) VALUES (?, ?)`, e.Entity, e.Target)
	return transition.CommitReceipt{ExecutionID: e.ID, OperationID: e.CommitOperationID, Outcome: transition.CommitSucceeded}, err
}

func TestLifecycleNotificationHandoffRetryShutdownAndRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "lifecycle-notifications.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE domain_state (entity TEXT PRIMARY KEY, state TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	var hooks atomic.Int64
	machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
		ID: "notification-lifecycle", Version: "1", InitialStates: []string{"open"},
		Handlers: map[string]fsm.Handler[string, string, string, string]{"open": notificationLifecycleHandler{calls: &hooks}},
		SelectInitial: func(string) (fsm.Selection[string], error) {
			return fsm.Selection[string]{Target: "open", Policy: fsm.Policy{MaxAttempts: 2, AttemptTimeout: time.Second, OverallTimeout: time.Minute}}, nil
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
	resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return stringValueCodec{}, nil }
	runner, err := transition.NewHookRunner[string, string, string, string](&registry, resolve, resolve)
	if err != nil {
		t.Fatal(err)
	}
	newAcceptor := func() *transition.Acceptor[string, string, string, string] {
		a, err := transition.NewAcceptor[string, string, string, string](store, &registry, resolve, resolve,
			func() (string, error) { return time.Now().Format(time.RFC3339Nano), nil }, time.Now)
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	ref := transition.CodecVersion{Name: "string", Version: "1"}
	accept := func(entity string) transition.Execution[string, string] {
		t.Helper()
		execution, _, err := newAcceptor().Accept(ctx, transition.Request[string, string, string, string]{
			Entity: entity, Key: entity, Operation: transition.Initialize, MachineID: machine.ID(), MachineVersion: "1",
			Input: "payload", InputCodec: ref, Notification: transition.NotifyEnable,
		})
		if err != nil {
			t.Fatal(err)
		}
		return execution
	}
	first := accept("first")
	deliveries := make(chan transition.NotificationDelivery[string, string], 8)
	release := make(chan struct{})
	var closeRelease sync.Once
	defer closeRelease.Do(func() { close(release) })
	failure := transition.ErrNoNotificationActivity
	newLifecycle := func(enable, loseHandoff bool, deliver func(context.Context, transition.NotificationDelivery[string, string]) error) *transition.Lifecycle[string, string, string, string] {
		t.Helper()
		outbox, err := sqlite.NewNotificationOutbox(store, time.Now)
		if err != nil {
			t.Fatal(err)
		}
		l, err := transition.NewLifecycle[string, string, string, string](store, runner, runner, notificationLifecycleCommit{db}, transition.LifecycleConfig{
			Coordinator:  transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: 3 * time.Second, Clock: time.Now},
			Worker:       transition.WorkerConfig{ID: "hook", Capacity: 1, LeaseTTL: time.Second, RenewalInterval: 100 * time.Millisecond, Clock: transition.SystemWorkerClock{}},
			PollInterval: 10 * time.Millisecond,
			ClassifyError: func(err error) transition.PathErrorClass {
				if errors.Is(err, errLostLifecycleHandoff) {
					return transition.PathErrorRecoverable
				}
				return transition.PathErrorUnclassified
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if !enable {
			return l
		}
		var handoff transition.NotificationHandoff[string, string] = outbox
		if loseHandoff {
			handoff = &lostLifecycleHandoff{NotificationHandoff: outbox}
		}
		err = l.EnableNotifications(store, handoff, outbox, deliver, transition.LifecycleNotificationConfig{
			Capacity: 1, Worker: transition.NotificationWorkerConfig{ID: "delivery", LeaseTTL: time.Second,
				RenewalInterval: 100 * time.Millisecond, AttemptTimeout: 5 * time.Second, RetryDelay: 40 * time.Millisecond, Clock: transition.SystemWorkerClock{}},
		})
		if err != nil {
			t.Fatal(err)
		}
		return l
	}
	pre := newLifecycle(false, false, nil)
	if err := pre.Start(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		pending, err := store.InspectExecution(ctx, first.ID)
		if err == nil && pending.Status == transition.Notifying && pending.CommitReceipt != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("committed handoff was not pending: %+v %v", pending, err)
		}
		time.Sleep(time.Millisecond)
	}
	if err := pre.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	l := newLifecycle(true, true, func(_ context.Context, d transition.NotificationDelivery[string, string]) error {
		deliveries <- d
		if d.Attempt == 1 {
			return failure
		}
		<-release // Model an adapter that ignores cancellation.
		return nil
	})
	if err := l.Start(ctx); !errors.Is(err, errLostLifecycleHandoff) {
		t.Fatalf("startup handoff failure was not surfaced: %v", err)
	}
	waitDelivery := func() transition.NotificationDelivery[string, string] {
		t.Helper()
		select {
		case d := <-deliveries:
			return d
		case <-time.After(15 * time.Second):
			t.Fatalf("delivery did not start: %v %+v", l.Err(), l.Status())
			return transition.NotificationDelivery[string, string]{}
		}
	}
	d1, d2 := waitDelivery(), waitDelivery()
	if d1.Intent.ID == "" || d1.Intent.ID != d2.Intent.ID || d1.Attempt != 1 || d2.Attempt != 2 {
		t.Fatalf("unstable retry: %+v %+v", d1, d2)
	}
	if !errors.Is(l.Err(), failure) {
		t.Fatalf("delivery failure was not surfaced: %v", l.Err())
	}
	deadline = time.Now().Add(15 * time.Second)
	for {
		execution, err := store.InspectExecution(ctx, first.ID)
		if err == nil && execution.Status == transition.Complete {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("handoff did not complete transition: %+v %v", execution, err)
		}
		time.Sleep(time.Millisecond)
	}
	shutdown, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if err := l.Shutdown(shutdown); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("active delivery was not drained to deadline: %v", err)
	}
	secondShutdown, cancelSecond := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancelSecond()
	if err := l.Shutdown(secondShutdown); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("still-running delivery was forgotten: %v", err)
	}
	closeRelease.Do(func() { close(release) })
	if err := l.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	second := accept("second")
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	l = newLifecycle(true, false, func(_ context.Context, d transition.NotificationDelivery[string, string]) error {
		deliveries <- d
		return nil
	})
	if err := l.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer l.Shutdown(ctx)
	third := accept("third") // No wakeup: polling must discover new work.
	got := map[string]transition.NotificationDelivery[string, string]{}
	for len(got) < 3 {
		d := waitDelivery()
		got[d.Intent.ExecutionID] = d
	}
	if got[first.ID].Intent.ID != d1.Intent.ID || got[first.ID].Attempt < 3 || got[second.ID].Intent.ID == "" || got[third.ID].Intent.ID == "" {
		t.Fatalf("restart did not recover deliveries: %+v", got)
	}
	if hooks.Load() != 3 {
		t.Fatalf("delivery reran hooks: %d", hooks.Load())
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM domain_state WHERE state = 'open'`).Scan(&count); err != nil || count != 3 {
		t.Fatalf("domain commits changed: %d %v", count, err)
	}
}
