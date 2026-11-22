package taskmanager_test

import (
	"context"
	"database/sql"
	"errors"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/fsm"
	"github.com/reinaldoborges/rff-go/internal/taskmanager"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type notificationControlClassifier struct{}

func (notificationControlClassifier) ClassifyHookFailure(transition.Execution[string, string], error) (transition.HookResult, error) {
	return transition.HookResult{}, errors.New("unexpected hook failure")
}

type notificationControlCommit struct {
	db       *sql.DB
	deadline time.Time
}

func (h notificationControlCommit) Commit(ctx context.Context, _ transition.TransitionLease[string], execution transition.Execution[string, string]) (transition.CommitReceipt, error) {
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return transition.CommitReceipt{}, err
	}
	defer tx.Rollback()
	var operation string
	err = tx.QueryRowContext(ctx, `SELECT operation_id FROM test_notification_commits WHERE execution_id = ?`, execution.ID).Scan(&operation)
	if errors.Is(err, sql.ErrNoRows) {
		if _, err := tx.ExecContext(ctx, `INSERT INTO tm_memberships (id, group_id, user_id, status, deadline, reason)
			VALUES ('membership', 'group', 'invitee', 'Invited', ?, '')`, h.deadline.Format(time.RFC3339Nano)); err != nil {
			return transition.CommitReceipt{}, err
		}
		operation = execution.CommitOperationID
		if _, err := tx.ExecContext(ctx, `INSERT INTO test_notification_commits (execution_id, operation_id) VALUES (?, ?)`, execution.ID, operation); err != nil {
			return transition.CommitReceipt{}, err
		}
	} else if err != nil {
		return transition.CommitReceipt{}, err
	}
	if operation != execution.CommitOperationID {
		return transition.CommitReceipt{}, errors.New("commit operation changed")
	}
	if err := tx.Commit(); err != nil {
		return transition.CommitReceipt{}, err
	}
	return transition.CommitReceipt{ExecutionID: execution.ID, OperationID: operation, Outcome: transition.CommitSucceeded}, nil
}

type notificationControlFailure struct{}

func (notificationControlFailure) Handoff(context.Context, transition.TransitionLease[string], transition.NotificationIntent[string, string]) error {
	return &transition.NotificationHandoffFailure{Failure: fsm.Failure{Code: "injected"}}
}

func TestOperatorNotificationRepairControls(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "taskmanager")
	build := exec.Command("go", "build", "-o", binary, "../../cmd/taskmanager")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v\n%s", err, output)
	}
	for _, action := range []transition.ControlAction{transition.ControlNotificationRetry, transition.ControlNotificationWaive} {
		t.Run(string(action), func(t *testing.T) {
			ctx := context.Background()
			now := time.Now().UTC()
			clock := func() time.Time { return now }
			path := filepath.Join(t.TempDir(), "repair.db")
			fixture, err := taskmanager.Open(path, clock)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if fixture != nil {
					if err := fixture.Close(); err != nil {
						t.Error(err)
					}
				}
			})
			if err := fixture.SeedUsers(ctx, "owner", "invitee"); err != nil {
				t.Fatal(err)
			}
			if err := fixture.SeedOperator(ctx, "operator"); err != nil {
				t.Fatal(err)
			}
			if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
				t.Fatal(err)
			}
			id, err := fixture.Invite(ctx, "owner", "group", "invitee", "membership", "invite", now.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := store.Close(); err != nil {
					t.Error(err)
				}
			})
			config := transition.CoordinatorConfig{ID: "taskmanager", Clock: clock, LeaseTTL: time.Minute}
			coordinator, err := transition.NewCoordinator[string, string](store, notificationControlClassifier{}, config)
			if err != nil {
				t.Fatal(err)
			}
			if err := coordinator.Reconcile(ctx); err != nil {
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
			if err := coordinator.Reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("sqlite3", path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			})
			if _, err := db.ExecContext(ctx, `CREATE TABLE test_notification_commits (execution_id TEXT PRIMARY KEY, operation_id TEXT NOT NULL)`); err != nil {
				t.Fatal(err)
			}
			commits, err := transition.NewCommitCoordinator[string, string](store, notificationControlCommit{db: db, deadline: now.Add(time.Hour)}, config)
			if err != nil {
				t.Fatal(err)
			}
			if err := commits.Reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			notifications, err := transition.NewNotificationCoordinator[string, string](store, notificationControlFailure{}, config)
			if err != nil {
				t.Fatal(err)
			}
			if err := notifications.Reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			record, err := fixture.InspectExecution(ctx, "operator", id)
			if err != nil || record.Status != transition.NotificationRepair {
				t.Fatalf("failed handoff did not enter repair: %+v, %v", record, err)
			}
			if err := fixture.Close(); err != nil {
				t.Fatal(err)
			}
			fixture, err = taskmanager.Open(path, clock)
			if err != nil {
				t.Fatal(err)
			}
			deniedCommand := exec.Command(binary, "-db", path, "control", "owner", id, "repair", string(action))
			if output, err := deniedCommand.CombinedOutput(); err == nil || !strings.Contains(string(output), taskmanager.ErrDenied.Error()) {
				t.Fatalf("owner was allowed notification control: output=%q error=%v", output, err)
			}
			record, err = fixture.InspectExecution(ctx, "operator", id)
			if err != nil || record.Status != transition.NotificationRepair || len(record.NotificationControls) != 0 {
				t.Fatalf("denied control changed repair: %+v, %v", record, err)
			}
			allowedCommand := exec.Command(binary, "-db", path, "control", "operator", id, "repair", string(action))
			if output, err := allowedCommand.CombinedOutput(); err != nil || strings.TrimSpace(string(output)) != `"ok"` {
				t.Fatalf("operator notification control failed: output=%q error=%v", output, err)
			}
			var delivered []taskmanager.InvitationNotification
			if err := fixture.SetNotificationSink(func(_ context.Context, notification taskmanager.InvitationNotification) error {
				delivered = append(delivered, notification)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := fixture.Drive(ctx); err != nil {
				t.Fatal(err)
			}
			record, err = fixture.InspectExecution(ctx, "operator", id)
			if err != nil || record.Status != transition.Complete {
				t.Fatalf("authorized notification control did not settle: %+v, %v", record, err)
			}
			membership, err := fixture.Membership(ctx, "owner", "membership")
			if err != nil || membership.Status != taskmanager.Invited || !membership.Deadline.Equal(now.Add(time.Hour)) {
				t.Fatalf("notification repair changed committed membership: %+v, %v", membership, err)
			}
			if action == transition.ControlNotificationRetry {
				if len(delivered) != 1 || delivered[0].MembershipID != membership.ID || delivered[0].ID == "" {
					t.Fatalf("retry did not deliver notification: %+v", delivered)
				}
			} else if len(delivered) != 0 {
				t.Fatalf("waiver delivered notification: %+v", delivered)
			}
			if err := fixture.ControlExecution(ctx, "operator", id, "repair", action); err != nil {
				t.Fatalf("notification control did not replay: %v", err)
			}
			audit, err := fixture.OperatorAudit(ctx, "operator")
			if err != nil {
				t.Fatal(err)
			}
			allowed, denied := false, false
			for _, entry := range audit {
				if entry.ExecutionID == id && entry.Action == string(action) {
					allowed = allowed || entry.Actor == "operator" && entry.Allowed
					denied = denied || entry.Actor == "owner" && !entry.Allowed
				}
			}
			if !allowed || !denied {
				t.Fatalf("notification controls not audited: %+v", audit)
			}
		})
	}
}
