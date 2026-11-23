package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestLostProviderResponseReconcilesAfterRealProcessRestart(t *testing.T) {
	db := sqlitePath(t, "lost-response")
	plan := "lost-" + strconv.Itoa(os.Getpid())
	runCLI(t, "-action", "start", "-db", db, "-plan", plan)
	runCLI(t, "-action", "approve", "-db", db, "-plan", plan)
	first := runCLI(t, "-action", "charge-once", "-provider-mode", "lost-response", "-db", db, "-plan", plan)
	if !strings.Contains(first, "charges=1") || !strings.Contains(first, "state=Due") {
		t.Fatalf("lost response was not applied before the first process exited:\n%s", first)
	}
	before := inspectCharge(t, db, plan)
	if before.Result != nil {
		t.Fatalf("lost response unexpectedly recorded a tracked result before restart: %#v", before.Result)
	}

	final := runCLI(t, "-action", "drive", "-db", db, "-plan", plan)
	if !strings.Contains(final, "state=Renewed") || !strings.Contains(final, "charges=1") {
		t.Fatalf("restart did not reconcile the existing provider effect:\n%s", final)
	}
	after := inspectCharge(t, db, plan)
	if after.Binding.Identity != before.Binding.Identity || after.Result == nil || len(after.Result.Data) == 0 {
		t.Fatalf("operation identity/result did not survive process restart: before=%#v after=%#v", before, after)
	}
}

func TestLateProviderCallBlocksUntilEvidenceResolutionAndSeparateRetry(t *testing.T) {
	db := sqlitePath(t, "late-call")
	plan := "late-" + strconv.Itoa(os.Getpid())
	runCLI(t, "-action", "start", "-db", db, "-plan", plan)
	runCLI(t, "-action", "approve", "-db", db, "-plan", plan)
	runCLI(t, "-action", "charge-once", "-provider-mode", "late-call", "-db", db, "-plan", plan)
	blocked := runCLI(t, "-action", "drive", "-db", db, "-plan", plan)
	if !strings.Contains(blocked, "state=Due") || !strings.Contains(blocked, "phase=NeedsIntervention") ||
		!strings.Contains(blocked, "charges=0") {
		t.Fatalf("inconclusive late call did not block before state progression:\n%s", blocked)
	}
	before := inspectCharge(t, db, plan)
	if before.Result != nil {
		t.Fatalf("unknown provider outcome exposed a result: %#v", before.Result)
	}

	runCLI(t, "-action", "late-call", "-db", db, "-plan", plan)
	runCLI(t, "-action", "resolve", "-db", db, "-plan", plan)
	resolved := inspectCharge(t, db, plan)
	if resolved.Binding.Identity != before.Binding.Identity || resolved.Result == nil {
		t.Fatalf("operator resolution did not preserve the original operation: before=%#v resolved=%#v", before, resolved)
	}
	runCLI(t, "-action", "retry", "-db", db, "-plan", plan)
	final := runCLI(t, "-action", "drive", "-db", db, "-plan", plan)
	if !strings.Contains(final, "state=Renewed") || !strings.Contains(final, "charges=1") {
		t.Fatalf("separately authorized retry did not finish the renewal:\n%s", final)
	}
}

func TestAuthorizedAbortRefundsWithoutUndoingCommittedDueState(t *testing.T) {
	db := sqlitePath(t, "compensation")
	host, err := OpenSQLite(db)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	ctx := context.Background()
	plan := "compensation-" + strconv.Itoa(os.Getpid())
	const period = "2026-11"
	if _, err := host.Start(ctx, plan, period); err != nil {
		t.Fatal(err)
	}
	if err := host.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := host.Approve(ctx, plan, period, "operator"); err != nil {
		t.Fatal(err)
	}
	if err := host.SetProviderMode(ctx, plan, providerFailAfter); err != nil {
		t.Fatal(err)
	}
	if err := host.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	blocked, err := host.Inspect(ctx, plan, period, "operator")
	if err != nil || blocked.State != Due || blocked.ActiveStatus != transition.NeedsIntervention || blocked.Charges != 1 {
		t.Fatalf("failure did not leave Due committed with a confirmed charge: status=%#v err=%v", blocked, err)
	}
	if err := host.Abort(ctx, plan, "operator"); err != nil {
		t.Fatal(err)
	}
	if err := host.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	aborted, err := host.Inspect(ctx, plan, period, "operator")
	if err != nil || aborted.State != Due || aborted.Refunds != 1 {
		t.Fatalf("tracked refund did not preserve Due: status=%#v err=%v", aborted, err)
	}
	retryExecution, err := host.Reattempt(ctx, plan, period, "operator")
	if err != nil {
		t.Fatal(err)
	}
	replayedRetry, err := host.Reattempt(ctx, plan, period, "operator")
	if err != nil || replayedRetry != retryExecution {
		t.Fatalf("retry replay was not idempotent: first=%q replay=%q err=%v", retryExecution, replayedRetry, err)
	}
	if err := host.SetProviderMode(ctx, plan, providerNormal); err != nil {
		t.Fatal(err)
	}
	if err := host.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	renewed, err := host.Inspect(ctx, plan, period, "operator")
	if err != nil || renewed.State != Renewed || renewed.Charges != 2 || renewed.Refunds != 1 {
		t.Fatalf("host-authorized retry after confirmed refund failed: status=%#v err=%v", renewed, err)
	}
}

func TestAuthorizationAndSeparateStoreProtocols(t *testing.T) {
	runtimePath := sqlitePath(t, "separate-runtime")
	hostPath := sqlitePath(t, "separate-host")
	host, err := OpenSQLiteSeparate(runtimePath, hostPath)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	ctx := context.Background()
	plan := "separate-" + strconv.Itoa(os.Getpid())
	const period = "2026-11"
	if _, err := host.Start(ctx, plan, period); err != nil {
		t.Fatal(err)
	}
	if err := host.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := host.Approve(ctx, plan, period, "untrusted"); err == nil {
		t.Fatal("unauthorized manual approval succeeded")
	}
	if _, err := host.Inspect(ctx, plan, period, "untrusted"); err == nil {
		t.Fatal("unauthorized inspection succeeded")
	}
	if _, err := host.Approve(ctx, plan, period, "operator"); err != nil {
		t.Fatal(err)
	}
	if err := host.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	status, err := host.Inspect(ctx, plan, period, "operator")
	if err != nil || status.State != Renewed || status.Charges != 1 {
		t.Fatalf("separate-store marker and successor protocol failed: status=%#v err=%v", status, err)
	}
	if err := host.Retry(ctx, plan, "untrusted"); err == nil {
		t.Fatal("unauthorized operator control succeeded")
	}
}

func TestStaleABAOriginIsSuperseded(t *testing.T) {
	runtimePath := sqlitePath(t, "stale-runtime")
	hostPath := sqlitePath(t, "stale-host")
	host, err := OpenSQLiteSeparate(runtimePath, hostPath)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	ctx := context.Background()
	plan := "stale-" + strconv.Itoa(os.Getpid())
	const period = "2026-11"
	if _, err := host.Start(ctx, plan, period); err != nil {
		t.Fatal(err)
	}
	if err := host.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := host.Approve(ctx, plan, period, "operator"); err != nil {
		t.Fatal(err)
	}
	host.beforeSuccessorAccept = func(ctx context.Context, _ transition.SuccessorBinding[string, State]) error {
		tx, err := host.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		var revision int64
		if err := tx.QueryRowContext(ctx, `SELECT revision FROM renewal_plans WHERE plan_id = ?`, plan).Scan(&revision); err != nil {
			_ = tx.Rollback()
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE renewal_plans SET state = ?, revision = ? WHERE plan_id = ?`,
			AwaitingApproval, revision+1, plan); err != nil {
			_ = tx.Rollback()
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE renewal_plans SET state = ?, revision = ? WHERE plan_id = ?`,
			Due, revision+2, plan); err != nil {
			_ = tx.Rollback()
			return err
		}
		return tx.Commit()
	}
	err = host.Drive(ctx)
	if !errors.Is(err, transition.ErrSuccessorSuperseded) {
		t.Fatalf("A-to-B-to-A origin race was not fenced: %v", err)
	}
	status, err := host.Inspect(ctx, plan, period, "operator")
	if err != nil || status.State != Due || status.Charges != 0 ||
		len(status.Continuations) != 1 || status.Continuations[0] != transition.ContinuationSuperseded {
		t.Fatalf("stale continuation advanced the plan: status=%#v err=%v", status, err)
	}
}

func TestMySQLHostScenarioAndProcessRestart(t *testing.T) {
	dsn := os.Getenv("RFF_MYSQL_DSN")
	if dsn == "" {
		t.Skip("RFF_MYSQL_DSN is required for real MySQL host coverage")
	}
	plan := fmt.Sprintf("mysql-%d", os.Getpid())
	period := "2026-11"
	start := runMySQLCLI(t, dsn, "-action", "start", "-plan", plan, "-period", period)
	if !strings.Contains(start, "state=AwaitingApproval") {
		t.Fatalf("MySQL initialization did not commit: %s", start)
	}
	runMySQLCLI(t, dsn, "-action", "approve", "-plan", plan, "-period", period)
	runMySQLCLI(t, dsn, "-action", "charge-once", "-provider-mode", "lost-response", "-plan", plan, "-period", period)
	final := runMySQLCLI(t, dsn, "-action", "drive", "-plan", plan, "-period", period)
	if !strings.Contains(final, "states=[AwaitingApproval Due PaymentCaptured Renewed]") ||
		!strings.Contains(final, "charges=1") {
		t.Fatalf("MySQL host scenario did not reconcile after process restart: %s", final)
	}
}

func sqlitePath(t *testing.T, name string) string {
	t.Helper()
	path := filepath.Join(".", fmt.Sprintf(".renewal-%s-%d.db", name, os.Getpid()))
	t.Cleanup(func() {
		for _, suffix := range []string{"", "-shm", "-wal"} {
			_ = os.Remove(path + suffix)
		}
	})
	return path
}

func runCLI(t *testing.T, args ...string) string {
	t.Helper()
	command := exec.Command("go", append([]string{"run", "."}, args...)...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run renewal CLI %v: %v\n%s", args, err, output)
	}
	return string(output)
}

func runMySQLCLI(t *testing.T, dsn string, args ...string) string {
	t.Helper()
	commandArgs := append([]string{"run", ".", "-backend", "mysql", "-dsn", dsn}, args...)
	command := exec.Command("go", commandArgs...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run MySQL renewal CLI %v: %v\n%s", args, err, output)
	}
	return string(output)
}

func inspectCharge(t *testing.T, db, plan string) transition.OperationInspection {
	t.Helper()
	host, err := OpenSQLite(db)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	inspection, err := host.InspectCharge(context.Background(), plan, "2026-11", "operator")
	if err != nil {
		t.Fatal(err)
	}
	return inspection
}
