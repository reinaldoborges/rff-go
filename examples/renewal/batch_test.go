package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestSQLiteBatchProgressSurvivesProcessRestart(t *testing.T) {
	for _, separate := range []bool{false, true} {
		name := "shared"
		if separate {
			name = "separate"
		}
		t.Run(name, func(t *testing.T) {
			runtimePath := filepath.Join(t.TempDir(), "runtime.db")
			hostPath := runtimePath
			if separate {
				hostPath = filepath.Join(t.TempDir(), "host.db")
			}
			host, err := openSQLite(runtimePath, hostPath)
			if err != nil {
				t.Fatal(err)
			}
			restarted := runBatchProcessRestart(t, host, "batch-restart-"+strconv.Itoa(os.Getpid())+"-"+name,
				[]string{"RFF_BATCH_RUNTIME_PATH=" + runtimePath, "RFF_BATCH_HOST_PATH=" + hostPath},
				func(t *testing.T) *Host {
					reopened, err := openSQLite(runtimePath, hostPath)
					if err != nil {
						t.Fatal(err)
					}
					return reopened
				})
			defer restarted.Close()
		})
	}
}

func runBatchProcessRestart(t *testing.T, host *Host, batchID string, childEnv []string, reopen func(*testing.T) *Host) *Host {
	t.Helper()
	ctx := context.Background()
	_, err := host.StartBatch(ctx, batchID, "2026-11", []string{"record-a", "record-b"})
	if err != nil {
		t.Fatalf("start batch: %v", err)
	}
	if err := host.Drive(ctx); err != nil {
		t.Fatalf("drive batch initialization: %v", err)
	}
	if _, err := host.Approve(ctx, batchID, "2026-11", "operator"); err != nil {
		t.Fatalf("approve batch: %v", err)
	}
	if err := prepareCommittedDue(ctx, host); err != nil {
		t.Fatalf("commit batch approval: %v", err)
	}
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}

	command := exec.Command(os.Args[0], "-test.run=^TestBatchProcessRestartHelper$")
	command.Env = append(os.Environ(), childEnv...)
	command.Env = append(command.Env, "RFF_BATCH_CRASH_RECORD=record-a")
	output, err := command.CombinedOutput()
	var exitError *exec.ExitError
	if !errors.As(err, &exitError) || exitError.ExitCode() != 73 {
		t.Fatalf("batch process did not stop after the confirmed first-record effect: err=%v output=%s", err, output)
	}

	time.Sleep(6 * time.Second)
	restarted := reopen(t)
	beforeRetry, err := restarted.BatchProgress(ctx, batchID)
	if err != nil {
		t.Fatal(err)
	}
	if len(beforeRetry.Records) != 2 || beforeRetry.Records[0].Complete ||
		beforeRetry.Records[0].Effects != 1 || beforeRetry.Records[1].Effects != 0 {
		t.Fatalf("crashed batch lost its durable member boundary: %#v", beforeRetry)
	}
	if err := restarted.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Retry(ctx, batchID, "operator", batchID+"-retry"); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	progress, err := restarted.BatchProgress(ctx, batchID)
	if err != nil {
		t.Fatal(err)
	}
	status, statusErr := restarted.Inspect(ctx, batchID, "2026-11", "operator")
	if progress.Cursor != 2 || len(progress.Records) != 2 ||
		progress.Records[0].RecordID != "record-a" || !progress.Records[0].Complete || progress.Records[0].Effects != 1 ||
		progress.Records[1].RecordID != "record-b" || !progress.Records[1].Complete || progress.Records[1].Effects != 1 ||
		statusErr != nil || status.State != Renewed {
		t.Fatalf("batch did not resume confirmed per-record progress exactly through the durable fake: progress=%#v status=%#v statusErr=%v", progress, status, statusErr)
	}
	return restarted
}

func TestBatchProcessRestartHelper(t *testing.T) {
	runtimePath := os.Getenv("RFF_BATCH_RUNTIME_PATH")
	runtimeDSN := os.Getenv("RFF_BATCH_RUNTIME_DSN")
	if runtimePath == "" && runtimeDSN == "" {
		return
	}
	var host *Host
	var err error
	if runtimeDSN != "" {
		hostDSN := os.Getenv("RFF_BATCH_HOST_DSN")
		if hostDSN == "" || hostDSN == runtimeDSN {
			host, err = OpenMySQL(runtimeDSN)
		} else {
			host, err = OpenMySQLSeparate(runtimeDSN, hostDSN)
		}
	} else {
		hostPath := os.Getenv("RFF_BATCH_HOST_PATH")
		host, err = openSQLite(runtimePath, hostPath)
	}
	if err != nil {
		t.Fatal(err)
	}
	host.afterBatchEffect = func(record string) {
		if record == os.Getenv("RFF_BATCH_CRASH_RECORD") {
			os.Exit(73)
		}
	}
	if err := host.Drive(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteBatchUnknownRecordBlocksDependentProgress(t *testing.T) {
	for _, separate := range []bool{false, true} {
		name := "shared"
		if separate {
			name = "separate"
		}
		t.Run(name, func(t *testing.T) {
			runtimePath := filepath.Join(t.TempDir(), "runtime.db")
			hostPath := runtimePath
			if separate {
				hostPath = filepath.Join(t.TempDir(), "host.db")
			}
			host, err := openSQLite(runtimePath, hostPath)
			if err != nil {
				t.Fatal(err)
			}
			defer host.Close()
			runUnknownBatchScenario(t, host, name)
		})
	}
}

func TestSQLiteFanoutPersistsIndependentEntityProgress(t *testing.T) {
	for _, separate := range []bool{false, true} {
		name := "shared"
		if separate {
			name = "separate"
		}
		t.Run(name, func(t *testing.T) {
			runtimePath := filepath.Join(t.TempDir(), "runtime.db")
			hostPath := runtimePath
			if separate {
				hostPath = filepath.Join(t.TempDir(), "host.db")
			}
			host, err := openSQLite(runtimePath, hostPath)
			if err != nil {
				t.Fatal(err)
			}
			defer host.Close()
			runFanoutScenario(t, host, "fanout-"+strconv.Itoa(os.Getpid())+"-"+name)
		})
	}
}

func TestSQLiteFanoutPartialAcceptanceSurvivesProcessRestart(t *testing.T) {
	for _, separate := range []bool{false, true} {
		name := "shared"
		if separate {
			name = "separate"
		}
		t.Run(name, func(t *testing.T) {
			runtimePath := filepath.Join(t.TempDir(), "runtime.db")
			hostPath := runtimePath
			if separate {
				hostPath = filepath.Join(t.TempDir(), "host.db")
			}
			host, err := openSQLite(runtimePath, hostPath)
			if err != nil {
				t.Fatal(err)
			}
			runFanoutProcessRestart(t, host, "fanout-restart-"+name,
				[]string{"RFF_BATCH_RUNTIME_PATH=" + runtimePath, "RFF_BATCH_HOST_PATH=" + hostPath},
				func(t *testing.T) *Host {
					reopened, err := openSQLite(runtimePath, hostPath)
					if err != nil {
						t.Fatal(err)
					}
					return reopened
				})
		})
	}
}

func runFanoutProcessRestart(t *testing.T, host *Host, runID string, childEnv []string, reopen func(*testing.T) *Host) {
	t.Helper()
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestFanoutProcessRestartHelper$")
	command.Env = append(os.Environ(), childEnv...)
	command.Env = append(command.Env, "RFF_FANOUT_RUN="+runID)
	output, err := command.CombinedOutput()
	var exitError *exec.ExitError
	if !errors.As(err, &exitError) || exitError.ExitCode() != 74 {
		t.Fatalf("fanout process did not stop after acceptance before checkpoint: err=%v output=%s", err, output)
	}
	restarted := reopen(t)
	defer restarted.Close()
	ctx := context.Background()
	before, err := restarted.FanoutProgress(ctx, runID)
	if err != nil || len(before.Records) != 2 || before.Records[0].ExecutionID != "" || before.Records[1].ExecutionID != "" {
		t.Fatalf("partial acceptance boundary not retained: %#v err=%v", before, err)
	}
	active, err := restarted.store.ActiveExecutionIDs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var acceptedID string
	for _, id := range active {
		execution, err := restarted.store.ReadActiveExecution(ctx, id)
		if err == nil && execution.Entity == runID+"-a" {
			acceptedID = id
		}
	}
	if acceptedID == "" {
		t.Fatal("accepted fanout execution was not durable")
	}
	if err := restarted.DriveFanout(ctx, runID); err != nil {
		t.Fatal(err)
	}
	after, err := restarted.FanoutProgress(ctx, runID)
	if err != nil || after.Cursor != 2 || after.Records[0].ExecutionID != acceptedID || after.Records[1].ExecutionID == "" ||
		after.Records[0].ExecutionID == after.Records[1].ExecutionID || after.Records[0].Effects != 1 || after.Records[1].Effects != 1 {
		t.Fatalf("fanout did not resume and checkpoint both accepted executions: %#v err=%v", after, err)
	}
}

func TestFanoutProcessRestartHelper(t *testing.T) {
	runID := os.Getenv("RFF_FANOUT_RUN")
	if runID == "" {
		return
	}
	var host *Host
	var err error
	if dsn := os.Getenv("RFF_BATCH_RUNTIME_DSN"); dsn != "" {
		host, err = openBatchMySQL(dsn, os.Getenv("RFF_BATCH_HOST_DSN"))
	} else {
		host, err = openSQLite(os.Getenv("RFF_BATCH_RUNTIME_PATH"), os.Getenv("RFF_BATCH_HOST_PATH"))
	}
	if err != nil {
		t.Fatal(err)
	}
	host.afterFanoutAccept = func(string) { os.Exit(74) }
	if err := host.StartFanout(context.Background(), runID, "2026-11", []string{runID + "-a", runID + "-b"}); err != nil {
		t.Fatal(err)
	}
	t.Fatal("fanout acceptance fault did not fire")
}

func TestSQLiteBatchClaimsSerializeConcurrentRuns(t *testing.T) {
	for _, separate := range []bool{false, true} {
		name := "shared"
		if separate {
			name = "separate"
		}
		t.Run(name, func(t *testing.T) {
			runtimePath := filepath.Join(t.TempDir(), "runtime.db")
			hostPath := runtimePath
			if separate {
				hostPath = filepath.Join(t.TempDir(), "host.db")
			}
			host, err := openSQLite(runtimePath, hostPath)
			if err != nil {
				t.Fatal(err)
			}
			defer host.Close()
			runBatchClaimRace(t, host, "sqlite-claim-"+strconv.Itoa(os.Getpid())+"-"+name)
			runStandaloneClaimRace(t, host, "sqlite-standalone-"+strconv.Itoa(os.Getpid())+"-"+name)
		})
	}
}

func runStandaloneClaimRace(t *testing.T, host *Host, suffix string) {
	t.Helper()
	for _, owner := range []string{"batch", "fanout"} {
		t.Run(owner, func(t *testing.T) {
			record := suffix + "-" + owner
			start := make(chan struct{})
			results := make(chan error, 2)
			go func() {
				<-start
				_, err := host.Start(context.Background(), record, "2026-11")
				results <- err
			}()
			go func() {
				<-start
				var err error
				if owner == "batch" {
					_, err = host.StartBatch(context.Background(), "owner-"+record, "2026-11", []string{record})
				} else {
					err = host.StartFanout(context.Background(), "owner-"+record, "2026-11", []string{record})
				}
				results <- err
			}()
			close(start)
			first, second := <-results, <-results
			if (first == nil) == (second == nil) ||
				first != nil && !errors.Is(first, transition.ErrConflict) ||
				second != nil && !errors.Is(second, transition.ErrConflict) {
				t.Fatalf("standalone claim race was not serialized: first=%v second=%v", first, second)
			}
		})
	}
}

func runBatchClaimRace(t *testing.T, host *Host, suffix string) {
	t.Helper()
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, batchID := range []string{"claim-a-" + suffix, "claim-b-" + suffix} {
		batchID := batchID
		go func() {
			<-start
			_, err := host.StartBatch(context.Background(), batchID, "2026-11", []string{"contended-" + suffix})
			results <- err
		}()
	}
	close(start)
	first, second := <-results, <-results
	if (first == nil) == (second == nil) ||
		first != nil && !errors.Is(first, transition.ErrConflict) ||
		second != nil && !errors.Is(second, transition.ErrConflict) {
		t.Fatalf("host did not serialize record claims between concurrent batches: first=%v second=%v", first, second)
	}
}

func runUnknownBatchScenario(t *testing.T, host *Host, name string) {
	t.Helper()
	ctx := context.Background()
	batchID := "batch-unknown-" + strconv.Itoa(os.Getpid()) + "-" + name
	first, err := host.StartBatch(ctx, batchID, "2026-11", []string{"unknown-a-" + name, "blocked-b-" + name})
	if err != nil {
		t.Fatal(err)
	}
	members := []string{"unknown-a-" + name, "blocked-b-" + name}
	replay, err := host.StartBatch(ctx, batchID, "2026-11", members)
	if err != nil || replay != first {
		t.Fatalf("stable batch membership replay changed its execution: first=%q replay=%q err=%v", first, replay, err)
	}
	if _, err := host.StartBatch(ctx, batchID, "2026-11", []string{members[1], members[0]}); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("changed batch membership was not rejected: %v", err)
	}
	if err := host.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := host.Approve(ctx, batchID, "2026-11", "operator"); err != nil {
		t.Fatal(err)
	}
	if err := host.SetProviderMode(ctx, members[0], providerLateCall); err != nil {
		t.Fatal(err)
	}
	if err := host.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	progress, err := host.BatchProgress(ctx, batchID)
	if err != nil {
		t.Fatal(err)
	}
	if len(progress.Records) != 2 || progress.Cursor != 0 ||
		progress.Records[0].Complete || progress.Records[0].OperationStatus != "pending" ||
		progress.Records[1].Complete || progress.Records[1].OperationStatus != "not-started" {
		t.Fatalf("unknown first-record outcome did not stop dependent progression: %#v", progress)
	}
	if err := host.ApplyLateProviderCall(ctx, members[0], "2026-11"); err != nil {
		t.Fatal(err)
	}
	if _, err := host.InspectCharge(ctx, members[0], "2026-11", "operator"); err != nil {
		t.Fatalf("inspect batch member charge: %v", err)
	}
	if err := host.ResolveCharge(ctx, batchID, "operator"); err != nil {
		t.Fatalf("resolve batch member charge: %v", err)
	}
	if err := host.Retry(ctx, batchID, "operator", batchID+"-retry"); err != nil {
		t.Fatal(err)
	}
	if err := host.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	progress, err = host.BatchProgress(ctx, batchID)
	if err != nil || progress.Cursor != 2 || progress.Records[0].Effects != 1 || progress.Records[1].Effects != 1 {
		t.Fatalf("resolved batch did not finish without duplicate charges: %#v err=%v", progress, err)
	}
}

func TestStandaloneCannotOverlapBatchOrFanout(t *testing.T) {
	host, err := OpenSQLite(filepath.Join(t.TempDir(), "claims.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	ctx := context.Background()
	if _, err := host.StartBatch(ctx, "claims-batch", "2026-11", []string{"batch-member"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.Start(ctx, "batch-member", "2026-11"); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("standalone start bypassed batch claim: %v", err)
	}
	if _, err := host.Start(ctx, "standalone", "2026-11"); err != nil {
		t.Fatal(err)
	}
	if err := host.StartFanout(ctx, "claims-fanout", "2026-11", []string{"standalone"}); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("fanout bypassed standalone claim: %v", err)
	}
}

func TestBatchAbortRefundsEveryChargedRecord(t *testing.T) {
	host, err := OpenSQLite(filepath.Join(t.TempDir(), "refund.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	runBatchAbortRefundsEveryChargedRecord(t, host)
}

func runBatchAbortRefundsEveryChargedRecord(t *testing.T, host *Host) {
	t.Helper()
	ctx := context.Background()
	if _, err := host.StartBatch(ctx, "refund-batch", "2026-11", []string{"refund-a", "refund-b"}); err != nil {
		t.Fatal(err)
	}
	if err := host.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := host.Approve(ctx, "refund-batch", "2026-11", "operator"); err != nil {
		t.Fatal(err)
	}
	if err := host.SetProviderMode(ctx, "refund-batch", providerFailAfter); err != nil {
		t.Fatal(err)
	}
	if err := host.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if err := host.Abort(ctx, "refund-batch", "operator", "refund-batch-abort"); err != nil {
		t.Fatal(err)
	}
	if err := host.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	progress, err := host.BatchProgress(ctx, "refund-batch")
	status, statusErr := host.Inspect(ctx, "refund-batch", "2026-11", "operator")
	if err != nil || statusErr != nil || status.State != Due || progress.Records[0].Effects != 1 || progress.Records[1].Effects != 1 || progress.Records[0].Refunds != 1 || progress.Records[1].Refunds != 1 {
		t.Fatalf("batch abort did not refund each charged member: progress=%#v status=%#v err=%v statusErr=%v", progress, status, err, statusErr)
	}
}

func TestBatchAbortWaitsForUnknownBeforeRefund(t *testing.T) {
	host, err := OpenSQLite(filepath.Join(t.TempDir(), "partial-refund.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	runBatchAbortWaitsForUnknownBeforeRefund(t, host)
}

func runBatchAbortWaitsForUnknownBeforeRefund(t *testing.T, host *Host) {
	t.Helper()
	ctx := context.Background()
	const batch, period = "partial-refund-batch", "2026-11"
	if _, err := host.StartBatch(ctx, batch, period, []string{"paid", "unknown", "not-started"}); err != nil {
		t.Fatal(err)
	}
	if err := host.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := host.Approve(ctx, batch, period, "operator"); err != nil {
		t.Fatal(err)
	}
	if err := host.SetProviderMode(ctx, "unknown", providerLateCall); err != nil {
		t.Fatal(err)
	}
	if err := host.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if err := host.Abort(ctx, batch, "operator", batch+"-abort"); err != nil {
		t.Fatal(err)
	}
	if err := host.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	blocked, err := host.BatchProgress(ctx, batch)
	if err != nil || blocked.Records[0].Refunds != 1 || blocked.Records[1].Refunds != 0 || blocked.Records[2].Effects != 0 {
		t.Fatalf("abort did not block unknown record without dispatching dependent effects: %#v err=%v", blocked, err)
	}
	if err := host.ApplyLateProviderCall(ctx, "unknown", period); err != nil {
		t.Fatal(err)
	}
	if err := host.ResolveCharge(ctx, batch, "operator"); err != nil {
		t.Fatal(err)
	}
	if err := host.Retry(ctx, batch, "operator", batch+"-retry-recovery"); err != nil {
		t.Fatal(err)
	}
	if err := host.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := host.BatchProgress(ctx, batch)
	if err != nil || after.Records[0].Refunds != 1 || after.Records[1].Refunds != 1 || after.Records[2].Refunds != 0 {
		t.Fatalf("abort did not compensate exactly the charged records: %#v err=%v", after, err)
	}
}

func TestBatchResolvesUnknownMembersIndividually(t *testing.T) {
	host, err := OpenSQLite(filepath.Join(t.TempDir(), "multiple-unknown.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	ctx := context.Background()
	const batch, period = "multiple-unknown", "2026-11"
	if _, err := host.StartBatch(ctx, batch, period, []string{"unknown-first", "unknown-second"}); err != nil {
		t.Fatal(err)
	}
	if err := host.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := host.Approve(ctx, batch, period, "operator"); err != nil {
		t.Fatal(err)
	}
	for _, member := range []string{"unknown-first", "unknown-second"} {
		if err := host.SetProviderMode(ctx, member, providerLateCall); err != nil {
			t.Fatal(err)
		}
	}
	for _, member := range []string{"unknown-first", "unknown-second"} {
		if err := host.Drive(ctx); err != nil {
			t.Fatal(err)
		}
		if err := host.ApplyLateProviderCall(ctx, member, period); err != nil {
			t.Fatal(err)
		}
		if err := host.ResolveCharge(ctx, batch, "operator"); err != nil {
			t.Fatal(err)
		}
		if err := host.Retry(ctx, batch, "operator", batch+"-retry-"+member); err != nil {
			t.Fatal(err)
		}
	}
	if err := host.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	progress, err := host.BatchProgress(ctx, batch)
	if err != nil || progress.Cursor != 2 || progress.Records[0].Effects != 1 || progress.Records[1].Effects != 1 {
		t.Fatalf("repeated per-member intervention did not finish: %#v err=%v", progress, err)
	}
}

func TestFanoutCursorDoesNotSkipIncompleteRecord(t *testing.T) {
	host, err := OpenSQLite(filepath.Join(t.TempDir(), "fanout.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	runFanoutCursorDoesNotSkipIncompleteRecord(t, host)
}

func runFanoutCursorDoesNotSkipIncompleteRecord(t *testing.T, host *Host) {
	t.Helper()
	ctx := context.Background()
	if err := host.StartFanout(ctx, "partial-fanout", "2026-11", []string{"partial-a", "partial-b"}); err != nil {
		t.Fatal(err)
	}
	if err := host.SetProviderMode(ctx, "partial-a", providerLateCall); err != nil {
		t.Fatal(err)
	}
	if err := host.DriveFanout(ctx, "partial-fanout"); err != nil {
		t.Fatal(err)
	}
	progress, err := host.FanoutProgress(ctx, "partial-fanout")
	if err != nil || progress.Cursor != 0 || progress.Records[0].Complete || !progress.Records[1].Complete {
		t.Fatalf("fanout cursor skipped unresolved record: %#v err=%v", progress, err)
	}
	if err := host.ApplyLateProviderCall(ctx, "partial-a", "2026-11"); err != nil {
		t.Fatal(err)
	}
	if err := host.ResolveCharge(ctx, "partial-a", "operator"); err != nil {
		t.Fatal(err)
	}
	if err := host.Retry(ctx, "partial-a", "operator", "partial-a-retry"); err != nil {
		t.Fatal(err)
	}
	if err := host.DriveFanout(ctx, "partial-fanout"); err != nil {
		t.Fatal(err)
	}
	progress, err = host.FanoutProgress(ctx, "partial-fanout")
	if err != nil || progress.Cursor != 2 || !progress.Records[0].Complete || !progress.Records[1].Complete ||
		progress.Records[0].Effects != 1 || progress.Records[1].Effects != 1 {
		t.Fatalf("fanout cursor failed to catch up over completed suffix: %#v err=%v", progress, err)
	}
}

func runFanoutScenario(t *testing.T, host *Host, runID string) {
	t.Helper()
	ctx := context.Background()
	records := []string{"entity-a-" + runID, "entity-b-" + runID}
	if err := host.StartFanout(ctx, runID, "2026-11", records); err != nil {
		t.Fatal(err)
	}
	started, err := host.FanoutProgress(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(started.Records) != 2 || started.Records[0].EntityID == started.Records[1].EntityID ||
		started.Records[0].ExecutionID == "" || started.Records[1].ExecutionID == "" ||
		started.Records[0].ExecutionID == started.Records[1].ExecutionID {
		t.Fatalf("fan-out did not create independently reserved entities: %#v", started)
	}
	for _, record := range started.Records {
		status, err := host.Inspect(ctx, record.EntityID, "2026-11", "operator")
		if err != nil || status.ActiveStatus != transition.Waiting {
			t.Fatalf("fan-out did not retain an independent reservation for %q: %#v err=%v", record.EntityID, status, err)
		}
	}
	if err := host.StartFanout(ctx, runID, "2026-11", []string{records[1], records[0]}); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("changed fan-out membership was not rejected: %v", err)
	}
	if err := host.DriveFanout(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if err := host.DriveFanout(ctx, runID); err != nil {
		t.Fatal(err)
	}
	progress, err := host.FanoutProgress(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if progress.Cursor != 2 || len(progress.Records) != 2 {
		t.Fatalf("durable fan-out cursor did not finish: %#v", progress)
	}
	for index, record := range progress.Records {
		if record.EntityID != records[index] || !record.Complete || record.Effects != 1 {
			t.Fatalf("record-level entity progress or effect replay was incorrect: %#v", progress)
		}
		status, err := host.Inspect(ctx, record.EntityID, "2026-11", "operator")
		if err != nil || status.State != Renewed {
			t.Fatalf("fan-out entity did not reach its own terminal state: %#v err=%v", status, err)
		}
	}
	if _, err := host.StartBatch(ctx, "overlap-"+runID, "2026-11", []string{records[0]}); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("host record claim allowed a second batch to overlap fan-out: %v", err)
	}
}

func prepareCommittedDue(ctx context.Context, host *Host) error {
	if err := host.coordinator.Reconcile(ctx); err != nil {
		return err
	}
	if err := host.worker.RunOne(ctx); err != nil {
		return err
	}
	if err := host.commits.Reconcile(ctx); err != nil {
		return err
	}
	return host.discovery.Reconcile(ctx)
}

func TestBatchMySQLScenarioAndProcessRestart(t *testing.T) {
	dsn := os.Getenv("RFF_MYSQL_DSN")
	if dsn == "" {
		t.Skip("RFF_MYSQL_DSN is required for real MySQL batch coverage")
	}
	databases := newRenewalMySQLDatabases(t, dsn, 3)
	sharedDSN, separateRuntimeDSN, separateHostDSN := databases[0], databases[1], databases[2]
	for _, store := range []struct {
		name       string
		runtimeDSN string
		hostDSN    string
	}{
		{name: "shared", runtimeDSN: sharedDSN, hostDSN: sharedDSN},
		{name: "separate", runtimeDSN: separateRuntimeDSN, hostDSN: separateHostDSN},
	} {
		t.Run(store.name, func(t *testing.T) {
			host, err := openBatchMySQL(store.runtimeDSN, store.hostDSN)
			if err != nil {
				t.Fatal(err)
			}
			runBatchClaimRace(t, host, "mysql-claim-"+strconv.Itoa(os.Getpid())+"-"+store.name)
			runStandaloneClaimRace(t, host, "mysql-claim-"+strconv.Itoa(os.Getpid())+"-"+store.name)
			childEnv := []string{
				"RFF_BATCH_RUNTIME_DSN=" + store.runtimeDSN,
				"RFF_BATCH_HOST_DSN=" + store.hostDSN,
			}
			restarted := runBatchProcessRestart(t, host, "mysql-batch-"+strconv.Itoa(os.Getpid())+"-"+store.name,
				childEnv, func(t *testing.T) *Host {
					reopened, err := openBatchMySQL(store.runtimeDSN, store.hostDSN)
					if err != nil {
						t.Fatal(err)
					}
					return reopened
				})
			t.Run("unknown-resolution", func(t *testing.T) {
				runUnknownBatchScenario(t, restarted, "mysql-"+store.name)
			})
			t.Run("batch-compensation", func(t *testing.T) {
				runBatchAbortRefundsEveryChargedRecord(t, restarted)
				runBatchAbortWaitsForUnknownBeforeRefund(t, restarted)
			})
			t.Run("fanout", func(t *testing.T) {
				runFanoutScenario(t, restarted, "mysql-fanout-"+store.name+"-"+strconv.Itoa(os.Getpid()))
			})
			t.Run("fanout-incomplete-cursor", func(t *testing.T) {
				runFanoutCursorDoesNotSkipIncompleteRecord(t, restarted)
			})
			t.Run("fanout-process-restart", func(t *testing.T) {
				runFanoutProcessRestart(t, restarted, "mysql-fanout-restart-"+store.name+"-"+strconv.Itoa(os.Getpid()),
					childEnv, func(t *testing.T) *Host {
						reopened, err := openBatchMySQL(store.runtimeDSN, store.hostDSN)
						if err != nil {
							t.Fatal(err)
						}
						return reopened
					})
			})
		})
	}
}

func openBatchMySQL(runtimeDSN, hostDSN string) (*Host, error) {
	if runtimeDSN == hostDSN {
		return OpenMySQL(runtimeDSN)
	}
	return OpenMySQLSeparate(runtimeDSN, hostDSN)
}
