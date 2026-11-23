package runtimecontract

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

const MigrationRestartAddress = "RFF_MIGRATION_RESTART_ADDRESS"
const migrationRestartPhase = "RFF_MIGRATION_RESTART_PHASE"
const MigrationWriteCrashAddress = "RFF_MIGRATION_WRITE_CRASH_ADDRESS"

type MigrationRestartStore interface {
	OperationMigrationStore
	Close() error
}

func migrationRestartMigrator(t *testing.T, store MigrationRestartStore, crash bool) *transition.ExecutionMigrator[string, string, string, string] {
	t.Helper()
	machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
		ID: "machine", Version: "2", InitialStates: []string{"open"},
		Handlers:         map[string]fsm.Handler[string, string, string, string]{"open": fsm.NoopHandler[string, string, string, string]{}},
		SelectInitial:    func(string) (fsm.Selection[string], error) { return fsm.Selection[string]{}, nil },
		SelectTransition: func(string, string, string) (fsm.Selection[string], error) { return fsm.Selection[string]{}, nil },
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
	runner, err := transition.NewHookRunner[string, string, string, string](&registry, resolve, resolve)
	if err != nil {
		t.Fatal(err)
	}
	migrator, err := transition.NewExecutionMigrator[string, string, string, string](store, runner,
		func(_ context.Context, source transition.MigrationSource[string], _ transition.MigrationRequest) (transition.MigrationSnapshot[string], error) {
			return transition.MigrationSnapshot[string]{MachineID: "machine", MachineVersion: "2", Target: "open", Input: source.Input, Policy: source.Policy}, nil
		},
		func(_ context.Context, source transition.MigrationSource[string], _ transition.MigrationSnapshot[string]) error {
			if source.Continuation == nil || source.Continuation.MachineVersion != "1" || len(source.Operations) != 1 {
				return errors.New("original continuation or operation missing")
			}
			return nil
		},
		func(_ context.Context, source transition.MigrationSource[string], _ transition.MigrationSnapshot[string]) error {
			if source.Operations[0].Binding.Identity != restartBinding().Identity ||
				source.Operations[0].Binding.Provider != restartBinding().Provider ||
				source.Operations[0].Binding.BehaviorVersion != restartBinding().BehaviorVersion || !source.Operations[0].MayHaveDispatched {
				return errors.New("original unknown operation missing")
			}
			if crash {
				os.Exit(72)
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	return migrator
}

// MigrationRestartChild exits while validating inside the transaction, or
// immediately after the transaction commits.
func MigrationRestartChild(t *testing.T, open func(string) (MigrationRestartStore, error)) {
	t.Helper()
	store, err := open(os.Getenv(MigrationRestartAddress))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	lease, err := store.AcquireTransitionLease(context.Background(), "entity", "execution", "coordinator", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	migrator := migrationRestartMigrator(t, store, os.Getenv(migrationRestartPhase) == "crash")
	if _, err := migrator.Migrate(context.Background(), lease, clock, transition.MigrationRequest{ID: "migration", MachineID: "machine", MachineVersion: "2"}); err != nil {
		t.Fatal(err)
	}
	os.Exit(73)
}

// RunMigrationWriteCrash injects process loss at a database write boundary.
// The adapter-specific trigger must stop the child only after reaching the
// requested statement; it is removed before the store is reopened.
func RunMigrationWriteCrash(t *testing.T, address string, open func(string) (MigrationRestartStore, error), install func() error, crash func(*exec.Cmd) error, remove func() error) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	store, err := open(address)
	if err != nil {
		t.Fatal(err)
	}
	policy := fsm.Policy{MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour}
	_, _, err = store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "key",
			MachineID: "machine", MachineVersion: "1", Operation: transition.Initialize, Target: "open",
			Input:  transition.EncodedValue{Codec: transition.CodecVersion{Name: "input", Version: "1"}, Data: []byte("payload")},
			Status: transition.Waiting, Deadline: now.Add(time.Hour), Policy: policy,
			Continuation: &transition.ContinuationSnapshot{MachineID: "machine", MachineVersion: "1", Policy: policy},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	fence, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, fence, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, claim, clock); err != nil {
		t.Fatal(err)
	}
	binding := restartBinding()
	if _, started, err := store.BeginTrackedOperation(ctx, claim, clock, binding, operationInputEqual); err != nil || !started {
		t.Fatalf("start unknown call: started=%v err=%v", started, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := install(); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable, "-test.run=^TestMigrationWriteProcessCrash$")
	command.Env = append(os.Environ(), MigrationWriteCrashAddress+"="+address, MigrationRestartAddress+"="+address, migrationRestartPhase+"=commit")
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	if err := crash(command); err != nil {
		t.Fatalf("crash at write boundary: %v\n%s", err, output.String())
	}
	if err := remove(); err != nil {
		t.Fatal(err)
	}
	store, err = open(address)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	record, err := store.ReadActiveExecution(ctx, "execution")
	if err != nil || record.MachineVersion != "1" || len(record.Migrations) != 0 || record.Continuation == nil || record.Continuation.MachineVersion != "1" {
		t.Fatalf("crash left partial execution: %+v %v", record, err)
	}
	inspection, err := store.InspectTrackedOperation(ctx, binding.Identity)
	if err != nil || inspection.Binding.Provider != binding.Provider || inspection.Result != nil || !inspection.MayHaveDispatched {
		t.Fatalf("crash lost operation binding: %+v %v", inspection, err)
	}
	if _, err := store.RenewHook(ctx, claim, clock, time.Minute); err != nil {
		t.Fatalf("crash left partial hook fence: %v", err)
	}
	currentFence, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
	if err != nil || currentFence.Generation != fence.Generation {
		t.Fatalf("crash left partial transition fence: %+v %v", currentFence, err)
	}
	migrator := migrationRestartMigrator(t, store, false)
	if _, err := migrator.Migrate(ctx, fence, clock, transition.MigrationRequest{ID: "migration", MachineID: "machine", MachineVersion: "2"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = open(address)
	if err != nil {
		t.Fatal(err)
	}
	record, err = store.ReadActiveExecution(ctx, "execution")
	if err != nil || record.MachineVersion != "2" || len(record.Migrations) != 1 || record.Continuation.MachineVersion != "1" {
		t.Fatalf("retried migration was not complete: %+v %v", record, err)
	}
	if _, err := store.RenewHook(ctx, claim, clock, time.Minute); !errors.Is(err, transition.ErrStaleActivityLease) {
		t.Fatalf("committed migration did not fence hook: %v", err)
	}
	if _, err := store.RenewTransitionLease(ctx, fence, clock, time.Hour); !errors.Is(err, transition.ErrStaleLease) {
		t.Fatalf("committed migration did not fence coordinator: %v", err)
	}
}

// RunMigrationRestart checks atomic crash recovery and retained unknown calls.
func RunMigrationRestart(t *testing.T, address string, open func(string) (MigrationRestartStore, error)) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	store, err := open(address)
	if err != nil {
		t.Fatal(err)
	}
	policy := fsm.Policy{MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour, RetryDelays: []time.Duration{0}}
	_, _, err = store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "key",
			MachineID: "machine", MachineVersion: "1", Operation: transition.Initialize, Target: "open",
			Input:  transition.EncodedValue{Codec: transition.CodecVersion{Name: "input", Version: "1"}, Data: []byte("payload")},
			Status: transition.Waiting, Deadline: now.Add(time.Hour), Policy: policy,
			Continuation: &transition.ContinuationSnapshot{MachineID: "machine", MachineVersion: "1", Policy: policy},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	fence, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, fence, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, claim, clock); err != nil {
		t.Fatal(err)
	}
	equal := func(_ context.Context, a, b []byte) (bool, error) { return bytes.Equal(a, b), nil }
	binding := restartBinding()
	if _, started, err := store.BeginTrackedOperation(ctx, claim, clock, binding, equal); err != nil || !started {
		t.Fatalf("start unknown call: started=%v err=%v", started, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range []struct {
		phase string
		exit  int
	}{{"crash", 72}, {"commit", 73}} {
		command := exec.Command(executable, "-test.run=^TestMigrationOperationProcessRestart$")
		command.Env = append(os.Environ(), MigrationRestartAddress+"="+address, migrationRestartPhase+"="+step.phase)
		output, err := command.CombinedOutput()
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != step.exit {
			t.Fatalf("%s subprocess: %v\n%s", step.phase, err, output)
		}
		store, err = open(address)
		if err != nil {
			t.Fatal(err)
		}
		record, err := store.ReadActiveExecution(ctx, "execution")
		wantVersion, wantReceipts := "1", 0
		if step.phase == "commit" {
			wantVersion, wantReceipts = "2", 1
		}
		if err != nil || record.MachineVersion != wantVersion || len(record.Migrations) != wantReceipts ||
			record.Continuation == nil || record.Continuation.MachineVersion != "1" || !record.Deadline.Equal(now.Add(time.Hour)) {
			t.Fatalf("%s migration snapshot: %+v %v", step.phase, record, err)
		}
		inspection, err := store.InspectTrackedOperation(ctx, binding.Identity)
		if err != nil || inspection.Result != nil || !inspection.MayHaveDispatched ||
			inspection.Binding.Provider != binding.Provider || inspection.Binding.BehaviorVersion != binding.BehaviorVersion {
			t.Fatalf("%s lost unknown operation: %+v %v", step.phase, inspection, err)
		}
		if step.phase == "commit" {
			if err := store.CompleteTrackedOperation(ctx, claim, clock, binding.Identity, transition.OperationResult{Codec: binding.ResultCodec, Data: []byte("late")}); !errors.Is(err, transition.ErrStaleActivityLease) {
				t.Fatalf("old operation report not fenced: %v", err)
			}
			earlier := func() time.Time { return now.Add(-time.Second) }
			if err := store.CompleteTrackedOperation(ctx, claim, earlier, binding.Identity, transition.OperationResult{Codec: binding.ResultCodec, Data: []byte("late")}); !errors.Is(err, transition.ErrStaleActivityLease) {
				t.Fatalf("old operation report regained its fence after clock rollback: %v", err)
			}
			if _, err := store.DispatchHook(ctx, fence, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); !errors.Is(err, transition.ErrStaleLease) {
				t.Fatalf("old coordinator not fenced: %v", err)
			}
			newFence, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			failure := fsm.Failure{Code: "worker-lost"}
			if err := store.ResolveUnknownHook(ctx, newFence, clock, claim.ActivityID, claim.Generation, transition.HookResult{
				Failure: failure, Disposition: fsm.DispositionRetry,
			}); err != nil {
				t.Fatal(err)
			}
			if err := store.ReportHook(ctx, claim, transition.HookResult{Failure: failure, Disposition: fsm.DispositionRetry}, earlier); !errors.Is(err, transition.ErrStaleActivityLease) {
				t.Fatalf("resolved old hook regained its fence: %v", err)
			}
			if _, err := store.DispatchHook(ctx, newFence, clock, transition.HookDispatch{
				Kind: transition.HookRetry, RetryCycle: 1, DueAt: now, Failure: &failure,
			}); err != nil {
				t.Fatal(err)
			}
			replacement, err := store.ClaimHook(ctx, "replacement", clock, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.StartHook(ctx, replacement, clock); err != nil {
				t.Fatal(err)
			}
			if _, started, err := store.BeginTrackedOperation(ctx, replacement, clock, binding, equal); !errors.Is(err, transition.ErrOperationUnresolved) || started {
				t.Fatalf("migration allowed unsafe repeat: started=%v err=%v", started, err)
			}
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
