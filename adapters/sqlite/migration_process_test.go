package sqlite_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestMigrationCrashLeavesNoPartialSnapshotAndReplays(t *testing.T) {
	const childEnv = "RFF_MIGRATION_CRASH_PATH"
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := fixedClock(now)
	path := os.Getenv(childEnv)
	child := path != ""
	if !child {
		path = filepath.Join(t.TempDir(), "migration-crash.db")
	}
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	if !child {
		t.Cleanup(func() {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		})
		_, _, err = store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			return &transition.Execution[string, string]{
				SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "key",
				MachineID: "task", MachineVersion: "1", Operation: transition.Initialize, Target: "open",
				Input:  transition.EncodedValue{Codec: transition.CodecVersion{Name: "input", Version: "1"}, Data: []byte("payload")},
				Status: transition.Waiting, AcceptedAt: now, Deadline: now.Add(time.Hour),
				Policy: fsm.Policy{MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
			}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour); err != nil {
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		executable, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		command := exec.Command(executable, "-test.run=^TestMigrationCrashLeavesNoPartialSnapshotAndReplays$")
		command.Env = append(os.Environ(), childEnv+"="+path)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("migration subprocess failed: %v\n%s", err, output)
		}
		store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		record, err := store.ReadActiveExecution(ctx, "execution")
		if err != nil || record.MachineVersion != "1" || len(record.Migrations) != 0 || !record.Deadline.Equal(now.Add(time.Hour)) {
			t.Fatalf("crashed migration left a partial snapshot: %+v, %v", record, err)
		}
	}
	machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
		ID: "task", Version: "2", InitialStates: []string{"open"},
		Handlers:      map[string]fsm.Handler[string, string, string, string]{"open": fsm.NoopHandler[string, string, string, string]{}},
		SelectInitial: func(string) (fsm.Selection[string], error) { return fsm.Selection[string]{}, nil },
		SelectTransition: func(string, string, string) (fsm.Selection[string], error) {
			return fsm.Selection[string]{}, errors.New("unused transition")
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
	resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return stringValueCodec{}, nil }
	runner, err := transition.NewHookRunner[string, string, string, string](&registry, resolve, resolve)
	if err != nil {
		t.Fatal(err)
	}
	conversions := 0
	migrator, err := transition.NewExecutionMigrator[string, string, string, string](store, runner,
		func(_ context.Context, source transition.MigrationSource[string], _ transition.MigrationRequest) (transition.MigrationSnapshot[string], error) {
			if child {
				os.Exit(0)
			}
			conversions++
			return transition.MigrationSnapshot[string]{
				MachineID: "task", MachineVersion: "2", Target: "open", Input: source.Input, Event: source.Event, Policy: source.Policy,
			}, nil
		},
		func(context.Context, transition.MigrationSource[string], transition.MigrationSnapshot[string]) error {
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	request := transition.MigrationRequest{ID: "migration", MachineID: "task", MachineVersion: "2", Parameters: []byte("same")}
	receipt, err := migrator.Migrate(ctx, lease, clock, request)
	if err != nil || receipt.Request.ID != request.ID {
		t.Fatalf("migration after crash failed: %+v, %v", receipt, err)
	}
	if _, err := migrator.Migrate(ctx, lease, clock, request); err != nil || conversions != 1 {
		t.Fatalf("migration replay repeated conversion: conversions=%d error=%v", conversions, err)
	}
	request.Parameters = []byte("changed")
	if _, err := migrator.Migrate(ctx, lease, clock, request); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("changed migration replay did not conflict: %v", err)
	}
}
