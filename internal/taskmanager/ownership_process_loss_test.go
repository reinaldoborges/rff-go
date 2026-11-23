package taskmanager

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
)

func TestOwnershipTransferSurvivesProcessLoss(t *testing.T) {
	const pathEnv = "RFF_OWNERSHIP_CRASH_PATH"
	const crashExit = 73
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	if path := os.Getenv(pathEnv); path != "" {
		sql.Register("sqlite3-transfer-crash", &sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				return conn.RegisterFunc("interrupt_transfer", func() int64 {
					os.Exit(crashExit)
					return 0
				}, false)
			},
		})
		fixture, err := openWithDriver(path, clock, "sqlite3-transfer-crash")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.TransferOwnership(ctx, "owner", "group", "member", "transfer"); err != nil {
			t.Fatal(err)
		}
		t.Fatal("transfer committed without reaching the crash trigger")
	}

	path := filepath.Join(t.TempDir(), "ownership-crash.db")
	fixture, err := Open(path, clock)
	if err != nil {
		t.Fatal(err)
	}
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
	if _, err := fixture.Decide(ctx, "member", "membership", "accept", AcceptInvitation); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TRIGGER interrupt_transfer AFTER UPDATE OF owner_id ON tm_groups
		BEGIN SELECT interrupt_transfer(); END`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	childCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	command := exec.CommandContext(childCtx, executable, "-test.run=^TestOwnershipTransferSurvivesProcessLoss$")
	command.Env = append(os.Environ(), pathEnv+"="+path)
	output, err := command.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != crashExit || childCtx.Err() != nil {
		t.Fatalf("transfer did not crash after ownership write: %v\n%s", err, output)
	}

	db, err = sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	var owner string
	var audits int
	if err := db.QueryRowContext(ctx, `SELECT owner_id FROM tm_groups WHERE id = 'group'`).Scan(&owner); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM tm_ownership_transfers`).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if owner != "owner" || audits != 0 {
		t.Fatalf("interrupted transfer persisted partial state: owner=%q audits=%d", owner, audits)
	}
	if _, err := db.ExecContext(ctx, `DROP TRIGGER interrupt_transfer`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	fixture, err = Open(path, clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	})
	receipt, err := fixture.TransferOwnership(ctx, "owner", "group", "member", "transfer")
	if err != nil || receipt.FormerOwner != "owner" || receipt.NewOwner != "member" {
		t.Fatalf("interrupted transfer did not retry: %+v, %v", receipt, err)
	}
	replayed, err := fixture.TransferOwnership(ctx, "owner", "group", "member", "transfer")
	if err != nil || replayed != receipt {
		t.Fatalf("retried transfer lost its audit or idempotency outcome: %+v, %v", replayed, err)
	}
}
