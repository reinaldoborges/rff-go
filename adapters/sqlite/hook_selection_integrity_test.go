package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/fsm"
	"github.com/reinaldoborges/rff-go/internal/inputselectiontest"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestClaimHookDetectsAndQuarantinesProjectionMismatch(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "hook-integrity.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})

	now := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	_, _, err = store.Accept(ctx, "entity", "request", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion,
			ID:            "execution",
			Entity:        "entity",
			Key:           "request",
			Operation:     transition.Initialize,
			Target:        "ready",
			Status:        transition.Waiting,
			Deadline:      now.Add(time.Hour),
			Policy:        fsm.Policy{MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	fence, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", func() time.Time { return now }, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, fence, func() time.Time { return now }, transition.HookDispatch{
		Kind: transition.HookEnter, DueAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	admin, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := admin.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := admin.ExecContext(ctx, `UPDATE rff_hook_work
		SET dispatch_kind = 'transition'
		WHERE execution_id = 'execution'`); err != nil {
		t.Fatal(err)
	}

	if _, err := store.ClaimHook(ctx, "worker", func() time.Time { return now }, time.Minute); !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("ClaimHook error = %v, want integrity finding", err)
	}

	maintenance, err := sqlite.OpenIntegrityMaintenance(path, func(context.Context, string, string, string, int64, int64) error {
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := maintenance.Close(); err != nil {
			t.Error(err)
		}
	}()
	findings, err := maintenance.Findings(ctx, "operator", "inspect hook integrity")
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 || findings[0].Kind != "hook" || findings[0].Category != "projection_mismatch" ||
		findings[0].Locator <= 0 || findings[0].Revision <= 0 {
		t.Fatalf("unsafe or missing hook finding: %+v", findings)
	}
	if _, err := store.ClaimHook(ctx, "worker-2", func() time.Time { return now }, time.Minute); !errors.Is(err, transition.ErrNoHookActivity) {
		t.Fatalf("quarantined hook claim error = %v, want no eligible hook", err)
	}
	if _, err := admin.ExecContext(ctx, `UPDATE rff_hook_work SET dispatch_kind = 'retry' WHERE execution_id = 'execution'`); err != nil {
		t.Fatal(err)
	}
	if err := maintenance.RebuildHookProjection(ctx, "operator", "stale repair", findings[0]); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("stale projection observation error = %v, want conflict", err)
	}
	findings, err = maintenance.Findings(ctx, "operator", "refresh observation")
	if err != nil || len(findings) != 1 || findings[0].Revision <= 1 {
		t.Fatalf("updated hook observation = %+v, %v", findings, err)
	}
	if err := maintenance.RebuildHookProjection(ctx, "operator", "repair hook projection", findings[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimHook(ctx, "worker-3", func() time.Time { return now }, time.Minute); err != nil {
		t.Fatalf("claim after authorized projection rebuild: %v", err)
	}
	audit, err := maintenance.Audit(ctx, "operator", "inspect repair", findings[0].ID)
	if err != nil || len(audit) != 1 || audit[0].Action != "rebuild_hook" {
		t.Fatalf("projection rebuild audit = %+v, %v", audit, err)
	}
	inputselectiontest.CheckFindingCoexistence(t, store, admin, maintenance)
}

func TestClaimHookQuarantinesExecutionAndReservationDependencies(t *testing.T) {
	for _, kind := range []string{"execution", "reservation"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "hook-dependencies.db")
			store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := store.Close(); err != nil {
					t.Error(err)
				}
			})

			now := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
			for _, id := range []string{"a", "b"} {
				entity := "entity-" + id
				_, _, err := store.Accept(ctx, entity, "request", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
					return &transition.Execution[string, string]{
						SchemaVersion: transition.CurrentSchemaVersion,
						ID:            id,
						Entity:        entity,
						Key:           "request",
						Operation:     transition.Initialize,
						Target:        "ready",
						Status:        transition.Waiting,
						Deadline:      now.Add(time.Hour),
						Policy:        fsm.Policy{MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
					}, nil
				})
				if err != nil {
					t.Fatal(err)
				}
				fence, err := store.AcquireTransitionLease(ctx, entity, id, "coordinator", func() time.Time { return now }, time.Hour)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := store.DispatchHook(ctx, fence, func() time.Time { return now }, transition.HookDispatch{
					Kind: transition.HookEnter, DueAt: now,
				}); err != nil {
					t.Fatal(err)
				}
			}

			admin, err := sql.Open("sqlite3", path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := admin.Close(); err != nil {
					t.Error(err)
				}
			})
			switch kind {
			case "execution":
				_, err = admin.ExecContext(ctx, `UPDATE rff_executions
					SET record = replace(CAST(record AS TEXT), '"Status":"Waiting"', '"Status":"Complete"')
					WHERE id = 'a'`)
			case "reservation":
				_, err = admin.ExecContext(ctx, `UPDATE rff_active SET entity = ? WHERE execution_id = 'a'`, []byte("wrong-entity"))
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.ClaimHook(ctx, "worker", func() time.Time { return now }, time.Minute); !errors.Is(err, transition.ErrIntegrity) {
				t.Fatalf("ClaimHook error = %v, want integrity finding", err)
			}
			if _, err := store.InspectExecution(ctx, "a"); !errors.Is(err, transition.ErrIntegrity) {
				t.Fatalf("InspectExecution error = %v, want integrity quarantine", err)
			}
			maintenance, err := sqlite.OpenIntegrityMaintenance(path, func(context.Context, string, string, string, int64, int64) error {
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			defer maintenance.Close()
			findings, err := maintenance.Findings(ctx, "operator", "inspect dependency quarantine")
			if err != nil || len(findings) != 1 || findings[0].Kind != kind {
				t.Fatalf("dependency finding = %+v, %v", findings, err)
			}
			lease, err := store.ClaimHook(ctx, "worker-2", func() time.Time { return now }, time.Minute)
			if err != nil || lease.ExecutionID != "b" {
				t.Fatalf("independent hook claim = %+v, %v", lease, err)
			}
		})
	}
}
