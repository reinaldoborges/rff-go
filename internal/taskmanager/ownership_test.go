package taskmanager_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/internal/taskmanager"
)

func TestOwnershipTransferIsAtomicAuditedAndReplayable(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "ownership.db")
	fixture, err := taskmanager.Open(path, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := fixture.SeedUsers(ctx, "owner", "member", "outsider"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "outsider", "other-group", "outsider-membership"); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.TransferOwnership(ctx, "owner", "group", "outsider", "cross-group"); !errors.Is(err, taskmanager.ErrConflict) {
		t.Fatalf("cross-group member became owner: %v", err)
	}
	if _, err := fixture.TransferOwnership(ctx, "owner", "group", "member", "too-early"); !errors.Is(err, taskmanager.ErrConflict) {
		t.Fatalf("non-member became owner: %v", err)
	}
	if _, err := fixture.Invite(ctx, "owner", "group", "member", "member-membership", "invite", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Decide(ctx, "member", "member-membership", "accept", taskmanager.AcceptInvitation); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.TransferOwnership(ctx, "outsider", "group", "member", "denied"); !errors.Is(err, taskmanager.ErrDenied) {
		t.Fatalf("non-owner transferred ownership: %v", err)
	}
	if _, err := fixture.TransferOwnership(ctx, "owner", "group", "owner", "self"); !errors.Is(err, taskmanager.ErrConflict) {
		t.Fatalf("self-transfer was accepted: %v", err)
	}
	receipt, err := fixture.TransferOwnership(ctx, "owner", "group", "member", "transfer")
	if err != nil || receipt.Actor != "owner" || receipt.FormerOwner != "owner" || receipt.NewOwner != "member" || !receipt.At.Equal(now) {
		t.Fatalf("invalid ownership receipt: receipt=%+v error=%v", receipt, err)
	}
	group, err := fixture.Group(ctx, "owner", "group")
	if err != nil || group.Owner != "member" {
		t.Fatalf("former owner lost membership or ownership did not change: group=%+v error=%v", group, err)
	}
	if err := fixture.Close(); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	fixture, err = taskmanager.Open(path, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := fixture.TransferOwnership(ctx, "owner", "group", "member", "transfer")
	if err != nil || replayed != receipt {
		t.Fatalf("original actor could not replay after ownership changed: receipt=%+v error=%v", replayed, err)
	}
	if _, err := fixture.TransferOwnership(ctx, "owner", "group", "outsider", "transfer"); !errors.Is(err, taskmanager.ErrConflict) {
		t.Fatalf("changed replay did not conflict: %v", err)
	}
	if _, err := fixture.TransferOwnership(ctx, "owner", "group", "member", "new"); !errors.Is(err, taskmanager.ErrDenied) {
		t.Fatalf("former owner retained transfer authority: %v", err)
	}
	if _, err := fixture.RemoveMember(ctx, "member", "owner-membership", "remove-former-owner"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Group(ctx, "owner", "group"); !errors.Is(err, taskmanager.ErrDenied) {
		t.Fatalf("removed former owner retained group access: %v", err)
	}
	replayed, err = fixture.TransferOwnership(ctx, "owner", "group", "member", "transfer")
	if err != nil || replayed != receipt {
		t.Fatalf("removed original actor could not replay: receipt=%+v error=%v", replayed, err)
	}
	if _, err := fixture.TransferOwnership(ctx, "owner", "group", "outsider", "transfer"); !errors.Is(err, taskmanager.ErrConflict) {
		t.Fatalf("removed actor changed transfer replay: %v", err)
	}
	if err := fixture.CloseGroup(ctx, "member", "group"); err != nil {
		t.Fatal(err)
	}
	replayed, err = fixture.TransferOwnership(ctx, "owner", "group", "member", "transfer")
	if err != nil || replayed != receipt {
		t.Fatalf("original actor could not replay after closure: receipt=%+v error=%v", replayed, err)
	}
}

func TestOwnershipTransferAuditFailureRollsBackOwnershipAndReplay(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "ownership-rollback.db")
	fixture, err := taskmanager.Open(path, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := fixture.SeedUsers(ctx, "owner", "member"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Invite(ctx, "owner", "group", "member", "membership", "invite", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Decide(ctx, "member", "membership", "accept", taskmanager.AcceptInvitation); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
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
	if _, err := db.ExecContext(ctx, `CREATE TRIGGER fail_transfer_audit BEFORE INSERT ON tm_ownership_transfers
		BEGIN SELECT RAISE(ABORT, 'injected audit failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.TransferOwnership(ctx, "owner", "group", "member", "transfer"); err == nil {
		t.Fatal("transfer succeeded after an audit persistence failure")
	}
	group, err := fixture.Group(ctx, "owner", "group")
	if err != nil || group.Owner != "owner" {
		t.Fatalf("failed audit left ownership changed: %+v, %v", group, err)
	}
	if _, err := db.ExecContext(ctx, `DROP TRIGGER fail_transfer_audit`); err != nil {
		t.Fatal(err)
	}
	receipt, err := fixture.TransferOwnership(ctx, "owner", "group", "member", "transfer")
	if err != nil || receipt.FormerOwner != "owner" || receipt.NewOwner != "member" {
		t.Fatalf("rolled-back request could not be retried: %+v, %v", receipt, err)
	}
	replayed, err := fixture.TransferOwnership(ctx, "owner", "group", "member", "transfer")
	if err != nil || replayed != receipt {
		t.Fatalf("committed transfer did not replay its audit: %+v, %v", replayed, err)
	}
}
