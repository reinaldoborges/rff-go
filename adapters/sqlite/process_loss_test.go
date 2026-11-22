package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestDomainCommitSurvivesActualProcessLoss(t *testing.T) {
	const phaseEnv = "RFF_COMMIT_LOSS_PHASE"
	const pathEnv = "RFF_COMMIT_LOSS_PATH"
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := fixedClock(now)
	phase := os.Getenv(phaseEnv)
	if phase != "" {
		store, err := sqlite.Open[string, string](os.Getenv(pathEnv), sqlite.StringCodec{}, sqlite.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		lease, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		host := &localCommitHost{store: store, clock: clock}
		if phase == "before" {
			host.afterWrite = func() { os.Exit(0) }
		}
		if _, err := host.Commit(ctx, lease, transition.Execution[string, string]{}); err != nil {
			t.Fatal(err)
		}
		if phase == "before" {
			t.Fatal("commit did not exit after the domain write")
		}
		os.Exit(0)
	}

	path := filepath.Join(t.TempDir(), "commit-loss.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE domain_state (entity TEXT PRIMARY KEY, state TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "key",
			Operation: transition.Initialize, Target: "open", Status: transition.Waiting,
			AcceptedAt: now, Deadline: now.Add(time.Hour),
			Policy: fsm.Policy{MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
		}, nil
	})
	if err != nil {
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
	if _, err := store.MarkWorkSuccess(ctx, lease, clock); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runChild := func(phase string) {
		t.Helper()
		command := exec.Command(executable, "-test.run=^TestDomainCommitSurvivesActualProcessLoss$")
		command.Env = append(os.Environ(), phaseEnv+"="+phase, pathEnv+"="+path)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("%s subprocess failed: %v\n%s", phase, err, output)
		}
	}
	runChild("before")
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	pending, err := store.ReadActiveExecution(ctx, "execution")
	if err != nil || pending.CommitReceipt != nil || pending.CommitOperationID == "" {
		t.Fatalf("interrupted commit did not remain pending: %+v, %v", pending, err)
	}
	db, err = sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM domain_state`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("interrupted commit persisted domain state: count=%d error=%v", count, err)
	}
	if err := errors.Join(db.Close(), store.Close()); err != nil {
		t.Fatal(err)
	}
	runChild("after")
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	host := &localCommitHost{store: store, clock: clock}
	receipt, err := host.Commit(ctx, lease, transition.Execution[string, string]{})
	if err != nil || receipt.Outcome != transition.CommitSucceeded || host.calls != 0 {
		t.Fatalf("committed operation did not replay without another write: %+v calls=%d error=%v", receipt, host.calls, err)
	}
	progress, err := store.ReadHookProgress(ctx, "execution")
	if err != nil || len(progress.Attempts) != 1 || !progress.Completed {
		t.Fatalf("commit recovery reran successful work: %+v, %v", progress, err)
	}
	db, err = sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	var state string
	if err := db.QueryRowContext(ctx, `SELECT state FROM domain_state WHERE entity = 'entity'`).Scan(&state); err != nil || state != "open" {
		t.Fatalf("committed domain state was lost: state=%q error=%v", state, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAcceptedAndStartedWorkSurvivesActualProcessLoss(t *testing.T) {
	const phaseEnv = "RFF_WORKER_LOSS_PHASE"
	const pathEnv = "RFF_WORKER_LOSS_PATH"
	phase := os.Getenv(phaseEnv)
	ctx := context.Background()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if phase == "" {
		path := filepath.Join(t.TempDir(), "process-loss.db")
		executable, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		runChild := func(childPhase string) {
			t.Helper()
			command := exec.Command(executable, "-test.run=^TestAcceptedAndStartedWorkSurvivesActualProcessLoss$")
			command.Env = append(os.Environ(), phaseEnv+"="+childPhase, pathEnv+"="+path)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("%s subprocess failed: %v\n%s", childPhase, err, output)
			}
		}
		runChild("accept")
		store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		ids, err := store.ActiveExecutionIDs(ctx)
		if err != nil || len(ids) != 1 || ids[0] != "execution" {
			t.Fatalf("accepted work was not discoverable after process exit: %v, %v", ids, err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		runChild("start")
		store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		})
		now := start.Add(2 * time.Second)
		clock := func() time.Time { return now }
		coordinator, err := transition.NewCoordinator[string, string](store, unknownClassifier{}, transition.CoordinatorConfig{
			ID: "restarted-coordinator", LeaseTTL: time.Minute, Clock: clock,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		progress, err := store.ReadHookProgress(ctx, "execution")
		if err != nil || !progress.Completed || len(progress.Attempts) != 1 || !progress.Attempts[0].Unknown || progress.BudgetUsed != 1 {
			t.Fatalf("lost started attempt was not recovered: %+v, %v", progress, err)
		}
		record, err := store.ReadActiveExecution(ctx, "execution")
		if err != nil || record.Target != "open" || record.MachineVersion != "1" || record.Policy.MaxAttempts != 4 ||
			!record.Deadline.Equal(start.Add(time.Hour)) {
			t.Fatalf("recovery changed pinned execution: %+v, %v", record, err)
		}
		if err := coordinator.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Second)
		retry, err := store.ClaimHook(ctx, "replacement-worker", clock, time.Minute)
		if err != nil || retry.ExecutionID != "execution" || retry.ActivityID == progress.ActivityID {
			t.Fatalf("unknown attempt did not schedule retry: %+v, %v", retry, err)
		}
		return
	}
	store, err := sqlite.Open[string, string](os.Getenv(pathEnv), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	clock := func() time.Time { return start }
	if phase == "accept" {
		_, _, err := store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			return &transition.Execution[string, string]{
				SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "key",
				MachineID: "task", MachineVersion: "1", Operation: transition.Initialize, Target: "open",
				Input:  transition.EncodedValue{Codec: transition.CodecVersion{Name: "input", Version: "1"}, Data: []byte("payload")},
				Status: transition.Waiting, AcceptedAt: start, Deadline: start.Add(time.Hour),
				Policy: fsm.Policy{MaxAttempts: 4, AttemptTimeout: time.Minute, OverallTimeout: time.Hour, RetryDelays: []time.Duration{time.Second}},
			}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		os.Exit(0)
	}
	coordinator, err := transition.NewCoordinator[string, string](store, unknownClassifier{}, transition.CoordinatorConfig{
		ID: "lost-coordinator", LeaseTTL: time.Second, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimHook(ctx, "lost-worker", clock, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, claim, clock); err != nil {
		t.Fatal(err)
	}
	os.Exit(0)
}
