package taskmanager_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/internal/taskmanager"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestOverlappingIdentifiersPreserveLegacyActiveWork(t *testing.T) {
	for _, taskFirst := range []bool{false, true} {
		name := "legacy-membership"
		if taskFirst {
			name = "legacy-task"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			path := filepath.Join(t.TempDir(), "legacy.db")
			fixture, err := taskmanager.Open(path, func() time.Time { return now })
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
			if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
				t.Fatal(err)
			}
			var original string
			if taskFirst {
				original, err = fixture.CreateTask(ctx, "owner", "group", "task", "Task", "task")
			} else {
				original, err = fixture.Invite(ctx, "owner", "group", "invitee", "taskmanager-task:task", "invite", now.Add(time.Hour))
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := fixture.Close(); err != nil {
				t.Fatal(err)
			}
			fixture = nil
			db, err := sql.Open("sqlite3", path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`DROP TABLE tm_runtime_entities`); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			fixture, err = taskmanager.Open(path, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			if taskFirst {
				_, err = fixture.Invite(ctx, "owner", "group", "invitee", "taskmanager-task:task", "invite", now.Add(time.Hour))
			} else {
				_, err = fixture.CreateTask(ctx, "owner", "group", "task", "Task", "task")
			}
			if err != nil {
				t.Fatalf("new independent request conflicts with legacy work: %v", err)
			}
			if err := fixture.Drive(ctx); err != nil {
				t.Fatal(err)
			}
			var replay string
			if taskFirst {
				replay, err = fixture.CreateTask(ctx, "owner", "group", "task", "Task", "task")
			} else {
				replay, err = fixture.Invite(ctx, "owner", "group", "invitee", "taskmanager-task:task", "invite", now.Add(time.Hour))
			}
			if err != nil || replay != original {
				t.Fatalf("legacy replay: %q, %v", replay, err)
			}
			if _, err := fixture.Task(ctx, "owner", "task"); err != nil {
				t.Fatal(err)
			}
			if _, err := fixture.Membership(ctx, "owner", "taskmanager-task:task"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTaskLifecycleProgressesAndRejectsInvalidEvents(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	fixture, err := taskmanager.Open(filepath.Join(t.TempDir(), "tasks.db"), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := fixture.SeedUsers(ctx, "owner", "outsider"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.CreateTask(ctx, "outsider", "group", "task", "Task", "denied"); !errors.Is(err, taskmanager.ErrDenied) {
		t.Fatalf("outsider created task: %v", err)
	}
	if _, err := fixture.CreateTask(ctx, "owner", "group", "task", "Task", "create"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	task, err := fixture.Task(ctx, "owner", "task")
	if err != nil || task.Status != taskmanager.TaskOpen {
		t.Fatalf("task was not initialized open: %+v, %v", task, err)
	}
	if _, err := fixture.TransitionTask(ctx, "owner", "task", "invalid", taskmanager.CompleteTask); !errors.Is(err, taskmanager.ErrConflict) {
		t.Fatalf("open task completed without starting: %v", err)
	}
	for _, step := range []struct {
		event  taskmanager.TaskEvent
		status taskmanager.TaskStatus
		key    string
	}{
		{taskmanager.StartTask, taskmanager.TaskInProgress, "start"},
		{taskmanager.CompleteTask, taskmanager.TaskDone, "complete"},
	} {
		if _, err := fixture.TransitionTask(ctx, "owner", "task", step.key, step.event); err != nil {
			t.Fatal(err)
		}
		if err := fixture.Drive(ctx); err != nil {
			t.Fatal(err)
		}
		task, err = fixture.Task(ctx, "owner", "task")
		if err != nil || task.Status != step.status {
			t.Fatalf("task did not progress to %s: %+v, %v", step.status, task, err)
		}
	}
	if _, err := fixture.TransitionTask(ctx, "owner", "task", "cancel", taskmanager.CancelTask); !errors.Is(err, taskmanager.ErrConflict) {
		t.Fatalf("completed task was cancelled: %v", err)
	}
}

func TestTaskAssignmentRequiresActiveSameGroupMembershipAndRetainsHistory(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	fixture, err := taskmanager.Open(filepath.Join(t.TempDir(), "assignment.db"), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := fixture.SeedUsers(ctx, "owner", "member", "invitee", "outsider"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "outsider", "other-group", "other-membership"); err != nil {
		t.Fatal(err)
	}
	for _, user := range []string{"member", "invitee"} {
		if _, err := fixture.Invite(ctx, "owner", "group", user, user+"-membership", "invite-"+user, now.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if err := fixture.Drive(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := fixture.Decide(ctx, "member", "member-membership", "accept", taskmanager.AcceptInvitation); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.CreateTask(ctx, "member", "group", "task", "Original", "create"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.CreateTask(ctx, "invitee", "group", "unauthorized", "Task", "denied"); !errors.Is(err, taskmanager.ErrDenied) {
		t.Fatalf("invited user created task: %v", err)
	}
	if err := fixture.UpdateTask(ctx, "invitee", "task", "Unauthorized"); !errors.Is(err, taskmanager.ErrDenied) {
		t.Fatalf("invited user updated task: %v", err)
	}
	for _, invalid := range []string{"invitee-membership", "other-membership", "missing"} {
		if err := fixture.AssignTask(ctx, "member", "task", invalid); !errors.Is(err, taskmanager.ErrConflict) {
			t.Fatalf("assignment to %q was accepted: %v", invalid, err)
		}
	}
	if err := fixture.AssignTask(ctx, "outsider", "task", "member-membership"); !errors.Is(err, taskmanager.ErrDenied) {
		t.Fatalf("cross-group user assigned task: %v", err)
	}
	if err := fixture.AssignTask(ctx, "member", "task", "member-membership"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.UpdateTask(ctx, "member", "task", "Updated"); err != nil {
		t.Fatal(err)
	}
	task, err := fixture.Task(ctx, "owner", "task")
	if err != nil || task.Assignee != "member-membership" || task.Title != "Updated" {
		t.Fatalf("task update was not committed: %+v, %v", task, err)
	}
	if err := fixture.AssignTask(ctx, "owner", "task", ""); err != nil {
		t.Fatal(err)
	}
	history, err := fixture.TaskAssignmentHistory(ctx, "owner", "task")
	if err != nil || len(history) != 1 || history[0].MembershipID != "member-membership" {
		t.Fatalf("unassignment lost historical membership reference: %+v, %v", history, err)
	}
	if _, err := fixture.TransitionTask(ctx, "member", "task", "start", taskmanager.StartTask); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.TransitionTask(ctx, "member", "task", "complete", taskmanager.CompleteTask); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	completed, err := fixture.Task(ctx, "member", "task")
	if err != nil || completed.Status != taskmanager.TaskDone {
		t.Fatalf("active member could not complete task: %+v, %v", completed, err)
	}
}

func TestTaskCancellationAndClosureRejectInvalidWork(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	fixture, err := taskmanager.Open(filepath.Join(t.TempDir(), "cancellation.db"), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := fixture.SeedUsers(ctx, "owner", "outsider"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.SeedOperator(ctx, "operator"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"open", "started", "pending"} {
		if _, err := fixture.CreateTask(ctx, "owner", "group", id, id, "create-"+id); err != nil {
			t.Fatal(err)
		}
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Task(ctx, "outsider", "open"); !errors.Is(err, taskmanager.ErrDenied) {
		t.Fatalf("outsider accessed task: %v", err)
	}
	if _, err := fixture.TransitionTask(ctx, "owner", "started", "start", taskmanager.StartTask); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"open", "started"} {
		if _, err := fixture.TransitionTask(ctx, "owner", id, "cancel-"+id, taskmanager.CancelTask); err != nil {
			t.Fatal(err)
		}
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"open", "started"} {
		task, err := fixture.Task(ctx, "owner", id)
		if err != nil || task.Status != taskmanager.TaskCancelled {
			t.Fatalf("cancellation failed: %+v, %v", task, err)
		}
	}
	execution, err := fixture.TransitionTask(ctx, "owner", "pending", "start-pending", taskmanager.StartTask)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.CloseGroup(ctx, "owner", "group"); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := fixture.Drive(ctx); err != nil {
			t.Fatal(err)
		}
	}
	record, err := fixture.InspectExecution(ctx, "operator", execution)
	if err != nil || record.CommitReceipt == nil || record.CommitReceipt.Outcome != transition.CommitDenied || record.Status != transition.Aborted {
		t.Fatalf("closed group did not reject accepted task work: %+v, %v", record, err)
	}
	if err := fixture.UpdateTask(ctx, "owner", "pending", "changed"); !errors.Is(err, taskmanager.ErrDenied) {
		t.Fatalf("closed group allowed task update: %v", err)
	}
}

func TestTaskRequestsReplayAcrossReopen(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "replay.db")
	fixture, err := taskmanager.Open(path, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := fixture.SeedUsers(ctx, "owner"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
		t.Fatal(err)
	}
	created, err := fixture.CreateTask(ctx, "owner", "group", "task", "Task", "create")
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.Close(); err != nil {
		t.Fatal(err)
	}
	fixture, err = taskmanager.Open(path, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if replay, err := fixture.CreateTask(ctx, "owner", "group", "task", "Task", "create"); err != nil || replay != created {
		t.Fatalf("create replay changed: %q, %v", replay, err)
	}
	if _, err := fixture.CreateTask(ctx, "owner", "group", "task", "Changed", "create"); !errors.Is(err, taskmanager.ErrConflict) {
		t.Fatalf("changed create replay did not conflict: %v", err)
	}
	started, err := fixture.TransitionTask(ctx, "owner", "task", "start", taskmanager.StartTask)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if replay, err := fixture.TransitionTask(ctx, "owner", "task", "start", taskmanager.StartTask); err != nil || replay != started {
		t.Fatalf("transition replay changed: %q, %v", replay, err)
	}
	if _, err := fixture.TransitionTask(ctx, "owner", "task", "start", taskmanager.CancelTask); !errors.Is(err, taskmanager.ErrConflict) {
		t.Fatalf("changed transition replay did not conflict: %v", err)
	}
}

func TestLegacyPendingInputReplaysAfterEntityRemapping(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "pending.db")
	fixture, err := taskmanager.Open(path, func() time.Time { return now })
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
	if err := fixture.SeedUsers(ctx, "owner"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
		t.Fatal(err)
	}
	original, err := fixture.CreateTask(ctx, "owner", "group", "task", "Task", "task")
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "owner", "other-group", "taskmanager-task:task"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Close(); err != nil {
		t.Fatal(err)
	}
	fixture = nil
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	result, err := db.Exec(`UPDATE rff_inputs SET record = CAST(json_set(CAST(record AS TEXT), '$.Outcome', NULL) AS BLOB)
		WHERE json_extract(CAST(record AS TEXT), '$.Channel') = 'taskmanager-tasks'`)
	if err != nil {
		t.Fatal(err)
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		t.Fatalf("prepare interrupted input: count=%d, error=%v", count, err)
	}
	if _, err := db.Exec(`DROP TABLE tm_runtime_entities`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	fixture, err = taskmanager.Open(path, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	replay, err := fixture.CreateTask(ctx, "owner", "group", "task", "Task", "task")
	if err != nil || replay != original {
		t.Fatalf("interrupted legacy input replay: %q, %v", replay, err)
	}
	if _, err := fixture.TransitionTask(ctx, "owner", "task", "start", taskmanager.StartTask); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	task, err := fixture.Task(ctx, "owner", "task")
	if err != nil || task.Status != taskmanager.TaskInProgress {
		t.Fatalf("task after legacy replay: %+v, %v", task, err)
	}
}

func TestTaskAndMembershipWithOverlappingIdentifiersAreIndependent(t *testing.T) {
	for _, taskFirst := range []bool{false, true} {
		name := "membership-first"
		if taskFirst {
			name = "task-first"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			path := filepath.Join(t.TempDir(), "overlap.db")
			fixture, err := taskmanager.Open(path, func() time.Time { return now })
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
			if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
				t.Fatal(err)
			}
			var invitation, task string
			invite := func() {
				invitation, err = fixture.Invite(ctx, "owner", "group", "invitee", "taskmanager-task:task", "invite", now.Add(time.Hour))
				if err != nil {
					t.Fatalf("invite: %v", err)
				}
			}
			create := func() {
				task, err = fixture.CreateTask(ctx, "owner", "group", "task", "Task", "task")
				if err != nil {
					t.Fatalf("create task: %v", err)
				}
			}
			if taskFirst {
				create()
				invite()
			} else {
				invite()
				create()
			}
			if err := fixture.Close(); err != nil {
				t.Fatal(err)
			}
			fixture = nil
			fixture, err = taskmanager.Open(path, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			var notifications []taskmanager.InvitationNotification
			if err := fixture.SetNotificationSink(func(_ context.Context, notification taskmanager.InvitationNotification) error {
				notifications = append(notifications, notification)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := fixture.Drive(ctx); err != nil {
				t.Fatal(err)
			}
			if len(notifications) != 1 || notifications[0].MembershipID != "taskmanager-task:task" ||
				notifications[0].GroupID != "group" || notifications[0].UserID != "invitee" {
				t.Fatalf("invitation notification lost the membership identity: %+v", notifications)
			}
			if replay, err := fixture.Invite(ctx, "owner", "group", "invitee", "taskmanager-task:task", "invite", now.Add(time.Hour)); err != nil || replay != invitation {
				t.Fatalf("invitation replay: %q, %v", replay, err)
			}
			if replay, err := fixture.CreateTask(ctx, "owner", "group", "task", "Task", "task"); err != nil || replay != task {
				t.Fatalf("task replay: %q, %v", replay, err)
			}
			if _, err := fixture.Decide(ctx, "invitee", "taskmanager-task:task", "accept", taskmanager.AcceptInvitation); err != nil {
				t.Fatal(err)
			}
			if _, err := fixture.TransitionTask(ctx, "owner", "task", "start", taskmanager.StartTask); err != nil {
				t.Fatal(err)
			}
			if err := fixture.Drive(ctx); err != nil {
				t.Fatal(err)
			}
			gotTask, err := fixture.Task(ctx, "owner", "task")
			if err != nil || gotTask.Status != taskmanager.TaskInProgress {
				t.Fatalf("task status: %+v, %v", gotTask, err)
			}
			membership, err := fixture.Membership(ctx, "owner", "taskmanager-task:task")
			if err != nil || membership.Status != taskmanager.Active {
				t.Fatalf("membership status: %+v, %v", membership, err)
			}
			if err := fixture.AssignTask(ctx, "owner", "task", membership.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := fixture.RemoveMember(ctx, "owner", membership.ID, "remove-assigned"); !errors.Is(err, taskmanager.ErrConflict) {
				t.Fatalf("assigned membership removal did not conflict: %v", err)
			}
			if err := fixture.AssignTask(ctx, "owner", "task", ""); err != nil {
				t.Fatal(err)
			}
			if _, err := fixture.RemoveMember(ctx, "owner", membership.ID, "remove"); err != nil {
				t.Fatal(err)
			}
			if err := fixture.Drive(ctx); err != nil {
				t.Fatal(err)
			}
			membership, err = fixture.Membership(ctx, "owner", membership.ID)
			if err != nil || membership.Status != taskmanager.Removed {
				t.Fatalf("unassigned membership was not removed: %+v, %v", membership, err)
			}
		})
	}
}

func TestConcurrentOverlappingTaskAndMembershipAcceptance(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	fixture, err := taskmanager.Open(filepath.Join(t.TempDir(), "concurrent.db"), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := fixture.SeedUsers(ctx, "owner", "invitee"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		<-start
		_, err := fixture.Invite(ctx, "owner", "group", "invitee", "taskmanager-task:task", "invite", now.Add(time.Hour))
		results <- err
	}()
	go func() {
		<-start
		_, err := fixture.CreateTask(ctx, "owner", "group", "task", "Task", "task")
		results <- err
	}()
	close(start)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("independent concurrent acceptance failed: %v", err)
		}
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Task(ctx, "owner", "task"); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Membership(ctx, "owner", "taskmanager-task:task"); err != nil {
		t.Fatal(err)
	}
}
