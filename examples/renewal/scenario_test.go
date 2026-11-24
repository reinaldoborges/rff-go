package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	driver "github.com/go-sql-driver/mysql"
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

func TestWorkloadAuthorizationIsDecidedByHost(t *testing.T) {
	host, err := OpenSQLite(sqlitePath(t, "workload-authorization"))
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	runWorkloadAuthorizationScenario(t, host)
}

func runWorkloadAuthorizationScenario(t *testing.T, host *Host) {
	t.Helper()
	ctx := context.Background()
	allowedPlan := fmt.Sprintf("workload-allowed-%d", os.Getpid())
	const period = "2026-11"
	if _, err := host.Start(ctx, allowedPlan, period); err != nil {
		t.Fatal(err)
	}
	if err := host.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := host.Approve(ctx, allowedPlan, period, "operator"); err != nil {
		t.Fatal(err)
	}
	t.Logf("driving explicitly authorized workload on %s host", host.dialect)
	if err := host.Drive(ctx); err != nil {
		t.Fatalf("host-authorized workload did not complete renewal: %v", err)
	}
	allowed, err := host.Inspect(ctx, allowedPlan, period, "operator")
	if err != nil || allowed.State != Renewed || allowed.Charges != 1 {
		t.Fatalf("authorized workload did not complete renewal: status=%#v err=%v", allowed, err)
	}

	deniedPlan := fmt.Sprintf("workload-denied-%d", os.Getpid())
	if _, err := host.Start(ctx, deniedPlan, period); err != nil {
		t.Fatal(err)
	}
	if err := host.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := host.Approve(ctx, deniedPlan, period, "operator"); err != nil {
		t.Fatal(err)
	}
	if err := host.SetWorkloadAuthorization(ctx, deniedPlan, "renewal-worker", "untrusted", false); err == nil {
		t.Fatal("unauthorized principal changed workload authorization")
	}
	if err := host.SetWorkloadAuthorization(ctx, deniedPlan, "renewal-worker", "operator", false); err != nil {
		t.Fatal(err)
	}
	t.Logf("driving explicitly denied workload on %s host", host.dialect)
	if err := host.Drive(ctx); !errors.Is(err, transition.ErrSuccessorDenied) {
		status, inspectErr := host.Inspect(ctx, deniedPlan, period, "operator")
		t.Logf("denied workload status after drive failure: status=%#v inspectErr=%v", status, inspectErr)
		t.Fatalf("host workload denial did not block successor acceptance: %v", err)
	}
	denied, err := host.Inspect(ctx, deniedPlan, period, "operator")
	if err != nil || denied.State != Due || denied.Charges != 0 {
		t.Fatalf("denied workload advanced or charged the plan: status=%#v err=%v", denied, err)
	}
}

func runOperatorAuthorizationScenario(t *testing.T, host *Host, suffix string) {
	t.Helper()
	ctx := context.Background()
	plan := "operator-denial-" + suffix
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
		t.Fatal("unauthorized inspection disclosed host state")
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
	status, err := host.Inspect(ctx, plan, period, "operator")
	if err != nil || status.State != Due || status.ActiveStatus != transition.NeedsIntervention {
		t.Fatalf("authorization scenario did not reach intervention: status=%#v err=%v", status, err)
	}
	if err := host.Retry(ctx, plan, "untrusted"); err == nil {
		t.Fatal("unauthorized hook control succeeded")
	}
	if _, err := host.Inspect(ctx, plan, period, "untrusted"); err == nil {
		t.Fatal("unauthorized intervention inspection disclosed host state")
	}
}

func runRenewalRecoveryScenario(t *testing.T, host *Host, suffix string) {
	t.Helper()
	ctx := context.Background()
	const period = "2026-11"

	latePlan := "late-recovery-" + suffix
	if _, err := host.Start(ctx, latePlan, period); err != nil {
		t.Fatal(err)
	}
	if err := host.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := host.Approve(ctx, latePlan, period, "operator"); err != nil {
		t.Fatal(err)
	}
	if err := host.SetProviderMode(ctx, latePlan, providerLateCall); err != nil {
		t.Fatal(err)
	}
	if err := host.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	unknown, err := host.Inspect(ctx, latePlan, period, "operator")
	if err != nil || unknown.State != Due || unknown.ActiveStatus != transition.NeedsIntervention || unknown.Charges != 0 {
		t.Fatalf("late provider call did not block the committed origin: status=%#v err=%v", unknown, err)
	}
	if err := host.ApplyLateProviderCall(ctx, latePlan, period); err != nil {
		t.Fatal(err)
	}
	if err := host.ResolveCharge(ctx, latePlan, "operator"); err != nil {
		t.Fatal(err)
	}
	if err := host.Retry(ctx, latePlan, "operator"); err != nil {
		t.Fatal(err)
	}
	if err := host.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if err := host.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	renewed, err := host.Inspect(ctx, latePlan, period, "operator")
	if err != nil || renewed.State != Renewed || renewed.Charges != 1 {
		t.Fatalf("evidence-backed retry did not settle renewal: status=%#v err=%v", renewed, err)
	}

	refundPlan := "refund-recovery-" + suffix
	if _, err := host.Start(ctx, refundPlan, period); err != nil {
		t.Fatal(err)
	}
	if err := host.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := host.Approve(ctx, refundPlan, period, "operator"); err != nil {
		t.Fatal(err)
	}
	if err := host.SetProviderMode(ctx, refundPlan, providerFailAfter); err != nil {
		t.Fatal(err)
	}
	if err := host.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	blocked, err := host.Inspect(ctx, refundPlan, period, "operator")
	if err != nil || blocked.State != Due || blocked.ActiveStatus != transition.NeedsIntervention || blocked.Charges != 1 {
		t.Fatalf("pre-commit failure did not preserve Due with a tracked charge: status=%#v err=%v", blocked, err)
	}
	if err := host.Abort(ctx, refundPlan, "operator"); err != nil {
		t.Fatal(err)
	}
	if err := host.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	aborted, err := host.Inspect(ctx, refundPlan, period, "operator")
	if err != nil || aborted.State != Due || aborted.Refunds != 1 {
		t.Fatalf("tracked refund did not preserve the committed origin: status=%#v err=%v", aborted, err)
	}
	if _, err := host.Reattempt(ctx, refundPlan, period, "operator"); err != nil {
		t.Fatal(err)
	}
	if err := host.SetProviderMode(ctx, refundPlan, providerNormal); err != nil {
		t.Fatal(err)
	}
	if err := host.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	renewed, err = host.Inspect(ctx, refundPlan, period, "operator")
	if err != nil || renewed.State != Renewed || renewed.Charges != 2 || renewed.Refunds != 1 {
		t.Fatalf("retry after confirmed recovery did not finish renewal: status=%#v err=%v", renewed, err)
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
	runStaleABAOriginScenario(t, host)
}

func runStaleABAOriginScenario(t *testing.T, host *Host) {
	t.Helper()
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
	err := host.Drive(ctx)
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
	databases := newRenewalMySQLDatabases(t, dsn, 3)
	sharedDSN, separateRuntimeDSN, separateHostDSN := databases[0], databases[1], databases[2]
	period := "2026-11"
	sharedPlan := fmt.Sprintf("mysql-shared-%d", os.Getpid())
	start := runMySQLCLI(t, sharedDSN, "-action", "start", "-plan", sharedPlan, "-period", period)
	if !strings.Contains(start, "state=AwaitingApproval") {
		t.Fatalf("shared MySQL initialization did not commit: %s", start)
	}
	runMySQLCLI(t, sharedDSN, "-action", "approve", "-plan", sharedPlan, "-period", period)
	runMySQLCLI(t, sharedDSN, "-action", "charge-once", "-provider-mode", "lost-response", "-plan", sharedPlan, "-period", period)
	final := runMySQLCLI(t, sharedDSN, "-action", "drive", "-plan", sharedPlan, "-period", period)
	if !strings.Contains(final, "states=[AwaitingApproval Due PaymentCaptured Renewed]") ||
		!strings.Contains(final, "charges=1") {
		t.Fatalf("shared MySQL host scenario did not reconcile after process restart: %s", final)
	}

	separatePlan := fmt.Sprintf("mysql-separate-%d", os.Getpid())
	separateStart := runMySQLCLI(t, separateRuntimeDSN,
		"-host-dsn", separateHostDSN, "-action", "start", "-plan", separatePlan, "-period", period)
	if !strings.Contains(separateStart, "state=AwaitingApproval") {
		t.Fatalf("separate MySQL initialization did not commit: %s", separateStart)
	}
	runMySQLCLI(t, separateRuntimeDSN, "-host-dsn", separateHostDSN,
		"-action", "approve", "-plan", separatePlan, "-period", period)
	runMySQLCLI(t, separateRuntimeDSN, "-host-dsn", separateHostDSN,
		"-action", "charge-once", "-provider-mode", "lost-response", "-plan", separatePlan, "-period", period)
	separateFinal := runMySQLCLI(t, separateRuntimeDSN, "-host-dsn", separateHostDSN,
		"-action", "drive", "-plan", separatePlan, "-period", period)
	if !strings.Contains(separateFinal, "states=[AwaitingApproval Due PaymentCaptured Renewed]") ||
		!strings.Contains(separateFinal, "charges=1") {
		t.Fatalf("separate MySQL host scenario did not reconcile after process restart: %s", separateFinal)
	}

	sharedHost, err := OpenMySQL(sharedDSN)
	if err != nil {
		t.Fatal(err)
	}
	runWorkloadAuthorizationScenario(t, sharedHost)
	runOperatorAuthorizationScenario(t, sharedHost, "mysql-shared-"+strconv.Itoa(os.Getpid()))
	runRenewalRecoveryScenario(t, sharedHost, "mysql-shared-"+strconv.Itoa(os.Getpid()))
	if err := sharedHost.Close(); err != nil {
		t.Fatal(err)
	}

	separateHost, err := OpenMySQLSeparate(separateRuntimeDSN, separateHostDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer separateHost.Close()
	runStaleABAOriginScenario(t, separateHost)
	runOperatorAuthorizationScenario(t, separateHost, "mysql-separate-"+strconv.Itoa(os.Getpid()))
	runRenewalRecoveryScenario(t, separateHost, "mysql-separate-"+strconv.Itoa(os.Getpid()))
}

func newRenewalMySQLDatabases(t *testing.T, dsn string, count int) []string {
	t.Helper()
	config, err := driver.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	adminDB, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	var version string
	if err := adminDB.QueryRowContext(t.Context(), `SELECT VERSION()`).Scan(&version); err != nil {
		_ = adminDB.Close()
		t.Fatal(err)
	}
	if !strings.HasPrefix(version, "8.4.") {
		_ = adminDB.Close()
		t.Fatalf("host scenario requires MySQL 8.4, got %s", version)
	}
	t.Logf("running host scenario on MySQL %s", version)
	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		t.Fatal(err)
	}
	var databases []string
	t.Cleanup(func() {
		for index := range databases {
			name := fmt.Sprintf("rff_renewal_%s_%d", hex.EncodeToString(suffix[:]), index)
			if _, err := adminDB.ExecContext(context.Background(), "DROP DATABASE `"+name+"`"); err != nil {
				t.Error(err)
			}
		}
		if err := adminDB.Close(); err != nil {
			t.Error(err)
		}
	})
	for index := 0; index < count; index++ {
		name := fmt.Sprintf("rff_renewal_%s_%d", hex.EncodeToString(suffix[:]), index)
		if _, err := adminDB.ExecContext(t.Context(), "CREATE DATABASE `"+name+"` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"); err != nil {
			t.Fatal(err)
		}
		databaseConfig := *config
		databaseConfig.DBName = name
		databases = append(databases, databaseConfig.FormatDSN())
	}
	return databases
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
		t.Fatalf("run MySQL renewal CLI: %v\n%s", err, output)
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
