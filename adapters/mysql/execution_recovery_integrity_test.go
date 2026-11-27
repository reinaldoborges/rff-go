package mysql_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/mysql"
	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestRestoreUnreadableExecutionPreservesStartedWorkAndEvidence(t *testing.T) {
	ctx := context.Background()
	db, config := newMySQLDatabase(t)
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}

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
	lease, err := store.ClaimHook(ctx, "worker", func() time.Time { return now }, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, lease, func() time.Time { return now }); err != nil {
		t.Fatal(err)
	}
	binding := transition.OperationBinding{
		Identity:          transition.OperationIdentity{ExecutionID: "execution", Key: "charge"},
		Kind:              "charge",
		BehaviorVersion:   "1",
		ComparatorVersion: "bytes-v1",
		Input:             []byte("amount=100"),
		ResultCodec:       transition.CodecVersion{Name: "result", Version: "1"},
	}
	equalInput := func(_ context.Context, left, right []byte) (bool, error) {
		return bytes.Equal(left, right), nil
	}
	if _, started, err := store.BeginTrackedOperation(ctx, lease, func() time.Time { return now }, binding, equalInput); err != nil || !started {
		t.Fatalf("begin downstream operation: started=%v, %v", started, err)
	}
	effectMayHaveOccurred := true

	var trustedRecord []byte
	if err := db.QueryRowContext(ctx, `SELECT record FROM rff_executions WHERE id = 'execution'`).Scan(&trustedRecord); err != nil {
		t.Fatal(err)
	}
	corruptRecord := []byte(`{"unreadable":`)
	if _, err := db.ExecContext(ctx, `UPDATE rff_executions SET record = ? WHERE id = 'execution'`, corruptRecord); err != nil {
		t.Fatal(err)
	}
	if err := store.BlockUnreadableExecution(ctx, "execution"); !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("BlockUnreadableExecution error = %v, want integrity finding", err)
	}
	if err := store.ReportHook(ctx, lease, transition.HookResult{Success: true}, func() time.Time { return now }); !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("ReportHook error = %v, want quarantined report", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{}); !errors.Is(err, mysql.ErrUnsupportedSchema) {
		t.Fatalf("normal initialization error = %v, want fail-closed unreadable record", err)
	}
	maintenance, err := mysql.OpenIntegrityMaintenance(config.FormatDSN(), func(context.Context, string, string, string, int64, int64) error {
		return nil
	})
	if err != nil {
		t.Fatalf("open worker-free maintenance after initialization failure: %v", err)
	}
	defer maintenance.Close()

	findings, err := maintenance.Findings(ctx, "operator", "restore trusted execution record")
	if err != nil || len(findings) != 1 || findings[0].Kind != "execution" ||
		findings[0].Category != "invalid_execution" || findings[0].Revision <= 0 ||
		!findings[0].DependenciesKnown {
		t.Fatalf("execution finding = %+v, %v", findings, err)
	}
	if err := maintenance.RestoreExecution(ctx, "operator", "restore from trusted backup", findings[0], trustedRecord); err != nil {
		t.Fatalf("restore trusted execution: %v", err)
	}
	evidence, err := maintenance.ArchivedExecutionEvidence(ctx, "operator", "inspect preserved evidence", findings[0].ID)
	if err != nil {
		t.Fatalf("read archived evidence through maintenance: %v", err)
	}
	if evidence.FindingID != findings[0].ID || evidence.Locator != findings[0].Locator ||
		evidence.Revision != findings[0].Revision || evidence.RecordDigest != sha256.Sum256(corruptRecord) ||
		!bytes.Equal(evidence.Record, corruptRecord) {
		t.Fatalf("archived raw evidence = %+v, want exact unreadable bytes", evidence)
	}
	audit, err := maintenance.Audit(ctx, "operator", "verify restoration audit", findings[0].ID)
	if err != nil || len(audit) != 1 || audit[0].Action != "restore_execution" {
		t.Fatalf("restoration audit = %+v, %v", audit, err)
	}

	store, err = mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatalf("normal initialization after trusted restoration: %v", err)
	}
	defer store.Close()
	if _, err := store.ClaimHook(ctx, "retry-worker", func() time.Time { return now }, time.Minute); !errors.Is(err, transition.ErrNoHookActivity) {
		t.Fatalf("claim after restoration = %v, want started hook to remain unclaimable", err)
	}
	progress, err := store.ReadHookProgress(ctx, "execution")
	if err != nil || len(progress.Attempts) != 1 || progress.Completed || !effectMayHaveOccurred {
		t.Fatalf("restored hook progress = %+v, %v; want original started attempt unchanged", progress, err)
	}
	operation, err := store.InspectTrackedOperation(ctx, binding.Identity)
	if err != nil || !operation.DispatchPending || !operation.MayHaveDispatched || operation.Result != nil {
		t.Fatalf("restored downstream operation = %+v, %v; want unresolved dispatch preserved", operation, err)
	}
	if _, started, err := store.BeginTrackedOperation(ctx, lease, func() time.Time { return now }, binding, equalInput); !errors.Is(err, transition.ErrOperationUnresolved) || started {
		t.Fatalf("restored unknown operation retry: started=%v, %v; want unresolved without redispatch", started, err)
	}
}

func TestRestoreExecutionRejectsStaleDependenciesAndFailedAudit(t *testing.T) {
	for _, mutation := range []string{"revision", "fence", "operation_fence", "unknown_dependency", "audit"} {
		t.Run(mutation, func(t *testing.T) {
			ctx := context.Background()
			db, config := newMySQLDatabase(t)
			store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
			if err != nil {
				t.Fatal(err)
			}
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
			lease, err := store.ClaimHook(ctx, "worker", func() time.Time { return now }, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.StartHook(ctx, lease, func() time.Time { return now }); err != nil {
				t.Fatal(err)
			}
			binding := transition.OperationBinding{
				Identity:          transition.OperationIdentity{ExecutionID: "execution", Key: "charge"},
				Kind:              "charge",
				BehaviorVersion:   "1",
				ComparatorVersion: "bytes-v1",
				Input:             []byte("amount=100"),
				ResultCodec:       transition.CodecVersion{Name: "result", Version: "1"},
			}
			if _, started, err := store.BeginTrackedOperation(ctx, lease, func() time.Time { return now }, binding,
				func(_ context.Context, left, right []byte) (bool, error) { return bytes.Equal(left, right), nil }); err != nil || !started {
				t.Fatalf("begin downstream operation: started=%v, %v", started, err)
			}

			var trustedRecord []byte
			if err := db.QueryRowContext(ctx, `SELECT record FROM rff_executions WHERE id = 'execution'`).Scan(&trustedRecord); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, `UPDATE rff_executions SET record = X'00' WHERE id = 'execution'`); err != nil {
				t.Fatal(err)
			}
			if err := store.BlockUnreadableExecution(ctx, "execution"); !errors.Is(err, transition.ErrIntegrity) {
				t.Fatalf("BlockUnreadableExecution error = %v, want integrity finding", err)
			}
			maintenance, err := mysql.OpenIntegrityMaintenance(config.FormatDSN(), func(context.Context, string, string, string, int64, int64) error {
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			findings, err := maintenance.Findings(ctx, "operator", "inspect restoration candidate")
			if err != nil || len(findings) != 1 || !findings[0].DependenciesKnown {
				t.Fatalf("restoration finding = %+v, %v", findings, err)
			}

			switch mutation {
			case "revision":
				_, err = db.ExecContext(ctx, `UPDATE rff_executions SET selection_status = 'Running' WHERE id = 'execution'`)
			case "fence":
				_, err = db.ExecContext(ctx, `UPDATE rff_transition_leases SET generation = generation + 1 WHERE execution_id = 'execution'`)
			case "operation_fence":
				var record []byte
				if err := db.QueryRowContext(ctx, `SELECT record FROM rff_operations WHERE execution_id = 'execution'`).Scan(&record); err != nil {
					t.Fatal(err)
				}
				var operation map[string]any
				if err := json.Unmarshal(record, &operation); err != nil {
					t.Fatal(err)
				}
				operation["Generation"] = operation["Generation"].(float64) + 1
				updated, err := json.Marshal(operation)
				if err != nil {
					t.Fatal(err)
				}
				_, err = db.ExecContext(ctx, `UPDATE rff_operations SET record = ? WHERE execution_id = 'execution'`, updated)
			case "unknown_dependency":
				_, err = db.ExecContext(ctx, `UPDATE rff_hook_work SET work = X'00' WHERE execution_id = 'execution'`)
			case "audit":
				_, err = db.ExecContext(ctx, `CREATE TRIGGER reject_integrity_audit BEFORE INSERT ON rff_integrity_audit
					FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'audit unavailable'`)
			}
			if err != nil {
				t.Fatal(err)
			}
			expected := transition.ErrConflict
			if mutation == "unknown_dependency" {
				expected = transition.ErrIntegrity
			}
			if mutation == "audit" {
				expected = nil
			}
			restoreErr := maintenance.RestoreExecution(ctx, "operator", "attempt trusted restoration", findings[0], trustedRecord)
			if expected == nil {
				if restoreErr == nil {
					t.Fatal("RestoreExecution succeeded with unavailable audit storage")
				}
			} else if !errors.Is(restoreErr, expected) {
				t.Fatalf("RestoreExecution error = %v, want %v", restoreErr, expected)
			}
			stillOpen, err := maintenance.Findings(ctx, "operator", "confirm unresolved finding")
			if err != nil || len(stillOpen) != 1 || stillOpen[0].ResolvedAt != nil {
				t.Fatalf("finding after rejected restoration = %+v, %v", stillOpen, err)
			}
			if _, err := maintenance.ArchivedExecutionEvidence(ctx, "operator", "confirm no partial archive", findings[0].ID); !errors.Is(err, transition.ErrConflict) {
				t.Fatalf("archive after rejected restoration = %v, want no archive", err)
			}
			audit, err := maintenance.Audit(ctx, "operator", "confirm no partial audit", findings[0].ID)
			if err != nil || len(audit) != 0 {
				t.Fatalf("audit after rejected restoration = %+v, %v", audit, err)
			}
			if err := maintenance.Close(); err != nil {
				t.Error(err)
			}
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestUnreadableExecutionQuarantinePreservesIndependentHook(t *testing.T) {
	ctx := context.Background()
	db, config := newMySQLDatabase(t)
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
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
	if _, err := db.ExecContext(ctx, `UPDATE rff_executions SET record = X'00' WHERE id = 'a'`); err != nil {
		t.Fatal(err)
	}
	if err := store.BlockUnreadableExecution(ctx, "a"); !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("BlockUnreadableExecution error = %v, want integrity finding", err)
	}
	lease, err := store.ClaimHook(ctx, "worker", func() time.Time { return now }, time.Minute)
	if err != nil || lease.ExecutionID != "b" {
		t.Fatalf("independent hook claim = %+v, %v; want b", lease, err)
	}
	if _, err := store.ClaimHook(ctx, "worker-2", func() time.Time { return now }, time.Minute); !errors.Is(err, transition.ErrNoHookActivity) {
		t.Fatalf("claim after independent work = %v, want quarantined execution a to remain unclaimable", err)
	}
}
