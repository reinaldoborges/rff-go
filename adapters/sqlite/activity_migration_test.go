package sqlite_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestStoreMigrationPreservesTransitionLease(t *testing.T) {
	for _, migration := range []struct {
		name string
		sql  string
	}{
		{"version two", `DROP TABLE rff_inputs; DROP TABLE rff_timers; DROP TABLE rff_notification_work; DROP TABLE rff_hook_work; UPDATE rff_schema_version SET version = 2`},
		{"version three", `DROP TABLE rff_inputs; DROP TABLE rff_timers; DROP TABLE rff_notification_work; UPDATE rff_schema_version SET version = 3`},
		{"version four", `DROP TABLE rff_inputs; DROP TABLE rff_timers; UPDATE rff_schema_version SET version = 4`},
		{"version five", `DROP TABLE rff_inputs; DROP TABLE rff_timers; UPDATE rff_schema_version SET version = 5`},
		{"version six", `DROP TABLE rff_inputs; DROP TABLE rff_timers; UPDATE rff_schema_version SET version = 6`},
		{"version seven", `DROP TABLE rff_inputs; UPDATE rff_schema_version SET version = 7`},
		{"version eight", `UPDATE rff_schema_version SET version = 8`},
	} {
		t.Run(migration.name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "old.db")
			store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
			if err != nil {
				t.Fatal(err)
			}

			_, _, err = store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
				return &transition.Execution[string, string]{
					SchemaVersion: transition.CurrentSchemaVersion, ID: "execution",
					Entity: "entity", Key: "key", Status: transition.Waiting,
				}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			lease, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", fixedClock(now), time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("sqlite3", path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(migration.sql); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
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
			renewed, err := store.RenewTransitionLease(ctx, lease, fixedClock(now.Add(time.Second)), time.Minute)
			if err != nil || renewed.Generation != lease.Generation {
				t.Fatalf("migration lost transition fence: lease=%+v error=%v", renewed, err)
			}
		})
	}
}

func TestSchemaMigrationPreservesActiveExecutionAndAttemptHistory(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "active-old.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := fixedClock(now)
	machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
		ID: "task", Version: "1", InitialStates: []string{"open"},
		Handlers: map[string]fsm.Handler[string, string, string, string]{"open": fsm.NoopHandler[string, string, string, string]{}},
		SelectInitial: func(string) (fsm.Selection[string], error) {
			return fsm.Selection[string]{Target: "open", Policy: fsm.Policy{
				MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour,
			}}, nil
		},
		SelectTransition: func(string, string, string) (fsm.Selection[string], error) {
			return fsm.Selection[string]{}, nil
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
	newAcceptor := func() *transition.Acceptor[string, string, string, string] {
		t.Helper()
		acceptor, err := transition.NewAcceptor[string, string, string, string](store, &registry, resolve, resolve, func() (string, error) {
			return "execution", nil
		}, clock)
		if err != nil {
			t.Fatal(err)
		}
		return acceptor
	}
	request := transition.Request[string, string, string, string]{
		Entity: "entity", Key: "key", Operation: transition.Initialize, MachineID: "task", MachineVersion: "1",
		Input: "payload", InputCodec: transition.CodecVersion{Name: "input", Version: "1"}, Notification: transition.NotifyInherit,
	}
	if _, _, err := newAcceptor().Accept(ctx, request); err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, lease, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, claim, clock); err != nil {
		t.Fatal(err)
	}
	if err := store.ReportHook(ctx, claim, transition.HookResult{Success: true}, clock); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TABLE rff_inputs; UPDATE rff_schema_version SET version = 7`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
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
	record, created, err := newAcceptor().Accept(ctx, request)
	if err != nil || created || record.ID != "execution" || record.MachineVersion != "1" || record.Input.Codec.Version != "1" {
		t.Fatalf("migration lost execution identity or behavior pins: %+v created=%v error=%v", record, created, err)
	}
	progress, err := store.ReadHookProgress(ctx, "execution")
	if err != nil || !progress.Completed || len(progress.Attempts) != 1 || progress.Attempts[0].Result == nil || !progress.Attempts[0].Result.Success {
		t.Fatalf("migration lost attempt history: %+v, %v", progress, err)
	}
}
