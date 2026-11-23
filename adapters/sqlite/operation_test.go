package sqlite_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/fsm"
	"github.com/reinaldoborges/rff-go/internal/runtimecontract"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestOperationProcessRestart(t *testing.T) {
	open := func(path string) (runtimecontract.RestartOperationStore, error) {
		return sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	}
	if os.Getenv(runtimecontract.OperationRestartPhase) != "" {
		runtimecontract.OperationRestartChild(t, open)
		return
	}
	for _, phase := range []string{"intent", "dispatch", "result"} {
		t.Run(phase, func(t *testing.T) {
			runtimecontract.RunOperationRestart(t, filepath.Join(t.TempDir(), "restart.db"), phase, open)
		})
	}
	t.Run("late-provider-call", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "late-restart.db")
		runtimecontract.RunOperationLateCallRestart(t, path, open)
	})
}

func TestMigrationOperationProcessRestart(t *testing.T) {
	open := func(path string) (runtimecontract.MigrationRestartStore, error) {
		return sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	}
	if os.Getenv(runtimecontract.MigrationRestartAddress) != "" {
		runtimecontract.MigrationRestartChild(t, open)
		return
	}
	runtimecontract.RunMigrationRestart(t, filepath.Join(t.TempDir(), "migration-restart.db"), open)
}

func TestOperationRecoveryProcessRestart(t *testing.T) {
	open := func(path string) (runtimecontract.RecoveryOperationRestartStore, error) {
		return sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	}
	if os.Getenv(runtimecontract.OperationRecoveryRestartAddress) != "" {
		runtimecontract.OperationRecoveryRestartChild(t, open)
		return
	}
	runtimecontract.RunOperationRecoveryRestart(t, filepath.Join(t.TempDir(), "recovery-restart.db"), open)
}

func TestOperationRecordIsolation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "isolation.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	open := func(path string) (runtimecontract.RestartOperationStore, error) {
		return sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	}
	runtimecontract.RunOperationIsolation(t, path, db, open, sqlite.ErrUnsupportedSchema)
}

type operationEntry struct {
	fsm.NoopHandler[string, string, string, string]
	run func(context.Context) error
}

type operationLostReplyStore struct {
	*sqlite.Store[string, string]
	lost atomic.Bool
}

func (s *operationLostReplyStore) CompleteTrackedOperation(ctx context.Context, lease transition.HookLease, clock func() time.Time, identity transition.OperationIdentity, result transition.OperationResult) error {
	if identity.Key == "write-fail" {
		return errors.New("injected result persistence failure")
	}
	err := s.Store.CompleteTrackedOperation(ctx, lease, clock, identity, result)
	if err == nil && s.lost.CompareAndSwap(false, true) {
		return errors.New("injected lost persistence response")
	}
	return err
}

func (h operationEntry) Enter(ctx context.Context, _ fsm.ExecutionContext[string, string, string, string]) error {
	return h.run(ctx)
}

func TestTrackedOperationReplaysWithinHook(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "operations.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	codec := transition.CodecVersion{Name: "result", Version: "1"}
	req := transition.OperationRequest{
		Key: "charge", Kind: "charge", BehaviorVersion: "1",
		ComparatorVersion: "bytes-v1", Input: []byte("amount=100"),
		ResultCodec:   codec,
		Preflight:     func(context.Context) error { return nil },
		ValidateInput: func(context.Context, []byte) error { return nil },
		EqualInput:    func(_ context.Context, a, b []byte) (bool, error) { return bytes.Equal(a, b), nil },
		ValidateResult: func(_ context.Context, result transition.OperationResult) error {
			if result.NoResult || result.Codec != codec || string(result.Data) != "receipt" {
				return errors.New("invalid result")
			}
			return nil
		},
	}
	dispatches := 0
	startedDetached := make(chan struct{})
	cancelledDetached := make(chan struct{})
	handler := operationEntry{run: func(ctx context.Context) error {
		dispatch := func(_ context.Context, id transition.OperationIdentity) (transition.OperationResult, error) {
			dispatches++
			if id.ExecutionID != "execution" || id.Key != "charge" {
				return transition.OperationResult{}, errors.New("incorrect identity")
			}
			return transition.OperationResult{Codec: codec, Data: []byte("receipt")}, nil
		}
		unavailable := req
		unavailable.Preflight = func(context.Context) error { return errors.New("codec unavailable") }
		if _, err := transition.TrackOperation(ctx, unavailable, dispatch); !errors.Is(err, transition.ErrCompatibility) {
			return errors.New("missing codec did not block")
		}
		cancelledCtx, cancel := context.WithCancel(ctx)
		cancelled := req
		cancelled.Preflight = func(ctx context.Context) error {
			cancel()
			return ctx.Err()
		}
		if _, err := transition.TrackOperation(cancelledCtx, cancelled, dispatch); !errors.Is(err, transition.ErrStaleActivityLease) {
			return errors.New("cancelled preflight did not report stale scope")
		}
		for i := 0; i < 2; i++ {
			result, err := transition.TrackOperation(ctx, req, dispatch)
			if err != nil {
				return err
			}
			if string(result.Data) != "receipt" {
				return errors.New("replay changed result")
			}
		}
		mutatingValidator := req
		mutatingValidator.ValidateResult = func(_ context.Context, result transition.OperationResult) error {
			result.Data[0] = 'X'
			return nil
		}
		if result, err := transition.TrackOperation(ctx, mutatingValidator, dispatch); err != nil || string(result.Data) != "receipt" {
			return errors.New("result validation changed the replayed value")
		}
		unavailableResult := req
		unavailableResult.ValidateResult = func(context.Context, transition.OperationResult) error { return errors.New("reference unavailable") }
		if _, err := transition.TrackOperation(ctx, unavailableResult, dispatch); !errors.Is(err, transition.ErrCompatibility) {
			return errors.New("unavailable replay result did not block")
		}
		changed := req
		changed.Input = []byte("amount=200")
		if _, err := transition.TrackOperation(ctx, changed, dispatch); !errors.Is(err, transition.ErrConflict) {
			return errors.New("changed input did not conflict")
		}
		decline := req
		decline.Key = "decline"
		declines := 0
		for i := 0; i < 2; i++ {
			result, err := transition.TrackOperation(ctx, decline, func(context.Context, transition.OperationIdentity) (transition.OperationResult, error) {
				declines++
				return transition.OperationResult{Codec: codec, FailureCode: "declined"}, nil
			})
			if !errors.Is(err, transition.ErrOperationFailed) || result.FailureCode != "declined" {
				return errors.New("definitive failure was not replayed")
			}
		}
		if declines != 1 {
			return errors.New("definitive failure was dispatched more than once")
		}
		missing := req
		missing.Key = "missing-result"
		missingCalls := 0
		missingDispatch := func(context.Context, transition.OperationIdentity) (transition.OperationResult, error) {
			missingCalls++
			return transition.OperationResult{Codec: codec}, nil
		}
		if _, err := transition.TrackOperation(ctx, missing, missingDispatch); !errors.Is(err, transition.ErrCompatibility) {
			return errors.New("missing result was treated as success")
		}
		if _, err := transition.TrackOperation(ctx, missing, missingDispatch); !errors.Is(err, transition.ErrOperationUnresolved) || missingCalls != 1 {
			return errors.New("missing result was redispatched")
		}
		writeFail := req
		writeFail.Key = "write-fail"
		writeCalls := 0
		writeDispatch := func(context.Context, transition.OperationIdentity) (transition.OperationResult, error) {
			writeCalls++
			return transition.OperationResult{Codec: codec, Data: []byte("receipt")}, nil
		}
		if _, err := transition.TrackOperation(ctx, writeFail, writeDispatch); !errors.Is(err, transition.ErrOperationUnresolved) {
			return errors.New("failed result persistence did not block")
		}
		if _, err := transition.TrackOperation(ctx, writeFail, writeDispatch); !errors.Is(err, transition.ErrOperationUnresolved) || writeCalls != 1 {
			return errors.New("failed result persistence was redispatched")
		}
		detached := req
		detached.Key = "detached"
		go func() {
			_, _ = transition.TrackOperation(context.WithoutCancel(ctx), detached, func(callCtx context.Context, _ transition.OperationIdentity) (transition.OperationResult, error) {
				close(startedDetached)
				<-callCtx.Done()
				close(cancelledDetached)
				return transition.OperationResult{}, callCtx.Err()
			})
		}()
		<-startedDetached
		return nil
	}}
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
	ref := transition.CodecVersion{Name: "string", Version: "1"}
	resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return stringValueCodec{}, nil }
	acceptor, err := transition.NewAcceptor[string, string, string, string](store, &registry, resolve, resolve, func() (string, error) { return "execution", nil }, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = acceptor.Accept(ctx, transition.Request[string, string, string, string]{
		Entity: "entity", Key: "request", Operation: transition.Initialize, MachineID: "machine", MachineVersion: "1", Input: "payload", InputCodec: ref, Notification: transition.NotifyInherit,
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
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	worker, err := transition.NewHookWorker[string, string, string, string](&operationLostReplyStore{Store: store}, runner, transition.WorkerConfig{
		ID: "worker", Capacity: 1, LeaseTTL: time.Minute, RenewalInterval: time.Second, Clock: transition.SystemWorkerClock{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.RunOne(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cancelledDetached:
	case <-time.After(time.Second):
		t.Fatal("tracked dispatch escaped the hook attempt context")
	}
	progress, err := store.ReadHookProgress(ctx, "execution")
	if err != nil || !progress.Completed || !progress.Result.Success || dispatches != 1 {
		t.Fatalf("tracked replay failed: progress=%+v dispatches=%d err=%v", progress, dispatches, err)
	}
}
