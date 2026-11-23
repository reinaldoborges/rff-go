package runtimecontract

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

const (
	OperationRecoveryRestartAddress  = "RFF_OPERATION_RECOVERY_RESTART_ADDRESS"
	OperationRecoveryRestartStart    = "RFF_OPERATION_RECOVERY_RESTART_START"
	OperationRecoveryRestartProvider = "RFF_OPERATION_RECOVERY_RESTART_PROVIDER"
)

type RecoveryOperationRestartStore interface {
	OperationPrerequisiteStore
	transition.InterventionStore[string]
	transition.CheckedHookControlStore[string, string]
	transition.AbortStore[string]
	transition.InspectionStore
	Close() error
}

func compensationRestartBinding() transition.OperationBinding {
	return transition.OperationBinding{
		Identity: transition.OperationIdentity{ExecutionID: "recovery-execution", Key: "refund"},
		Kind:     "refund", BehaviorVersion: "recovery-v1", ComparatorVersion: "bytes-v1",
		Compensation: true, Prerequisites: []string{"charge"},
		Input: []byte("amount=100"), ResultCodec: transition.CodecVersion{Name: "receipt", Version: "v1"},
	}
}

// RunOperationRecoveryRestart kills a real process after a compensation effect
// but before its result is persisted, then verifies recovery resumes under the
// existing budget without repeating the provider call.
func RunOperationRecoveryRestart(
	t *testing.T,
	address string,
	open func(string) (RecoveryOperationRestartStore, error),
) {
	t.Helper()
	ctx := context.Background()
	start := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	now := start
	clock := func() time.Time { return now }
	store, err := open(address)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = store.Accept(ctx, "entity", "request", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "recovery-execution", Entity: "entity", Key: "request",
			Operation: transition.Initialize, Status: transition.Waiting, AcceptedAt: start, Deadline: start.Add(3 * time.Hour),
			Policy: fsm.Policy{MaxAttempts: 4, AttemptTimeout: time.Minute, OverallTimeout: 2 * time.Hour, RetryDelays: []time.Duration{0}},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	fence, err := store.AcquireTransitionLease(ctx, "entity", "recovery-execution", "coordinator", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, fence, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: start}); err != nil {
		t.Fatal(err)
	}
	original, err := store.ClaimHook(ctx, "initial-worker", clock, time.Minute)
	if err != nil {
		t.Fatalf("claim initial hook: %v", err)
	}
	if _, err := store.StartHook(ctx, original, clock); err != nil {
		t.Fatal(err)
	}
	charge := prerequisiteBinding("charge", false, nil)
	charge.Identity.ExecutionID = "recovery-execution"
	if _, started, err := store.BeginTrackedOperation(ctx, original, clock, charge, operationInputEqual); err != nil || !started {
		t.Fatalf("start original charge: started=%v err=%v", started, err)
	}
	if err := store.CompleteTrackedOperation(ctx, original, clock, charge.Identity, prerequisiteResult()); err != nil {
		t.Fatal(err)
	}
	if err := store.ReportHook(ctx, original, transition.HookResult{
		Failure: fsm.Failure{Code: "work-failed"}, Disposition: fsm.DispositionIntervention,
	}, clock); err != nil {
		t.Fatal(err)
	}
	if err := store.EnterHookIntervention(ctx, fence, clock); err != nil {
		t.Fatal(err)
	}
	control, err := store.ApplyCheckedHookControl(ctx, fence, clock,
		transition.HookControl{ID: "abort", Action: transition.AbortIntervention},
		func(transition.Execution[string, string], transition.HookCall) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if control.Deadline.IsZero() {
		t.Fatal("authorized abort did not start a recovery budget")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	var providerCalls atomic.Int32
	var providerEffects atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		providerCalls.Add(1)
		providerEffects.Add(1)
		_, _ = w.Write([]byte("refund-receipt"))
	}))
	defer provider.Close()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable, "-test.run=^TestOperationRecoveryProcessRestart$")
	command.Env = append(os.Environ(),
		OperationRecoveryRestartAddress+"="+address,
		OperationRecoveryRestartStart+"="+start.Format(time.RFC3339Nano),
		OperationRecoveryRestartProvider+"="+provider.URL,
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("compensation child process: %v output=%s", err, output)
	}
	if providerCalls.Load() != 1 || providerEffects.Load() != 1 {
		t.Fatalf("compensation effect did not occur exactly once before the crash: calls=%d effects=%d", providerCalls.Load(), providerEffects.Load())
	}

	store, err = open(address)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	now = start.Add(2 * time.Minute)
	inspection, err := store.InspectTrackedOperation(ctx, compensationRestartBinding().Identity)
	if err != nil || inspection.Result != nil || !inspection.MayHaveDispatched ||
		inspection.Binding.Identity.Key == charge.Identity.Key || !inspection.Binding.Compensation ||
		!equalStrings(inspection.Binding.Prerequisites, []string{"charge"}) {
		t.Fatalf("compensation loss did not retain its distinct unresolved identity: %+v err=%v", inspection, err)
	}
	coordinator, err := transition.NewCoordinator[string, string](store, unknownClassifier{}, transition.CoordinatorConfig{
		ID: "coordinator", LeaseTTL: time.Hour, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimHook(ctx, "replacement-worker", clock, time.Minute)
	if err != nil {
		t.Fatalf("claim retry after recovery restart: %v", err)
	}
	_, call, err := store.InspectHook(ctx, claim, clock)
	if err != nil || call.Kind != transition.HookRecover {
		t.Fatalf("recovery retry changed hook kind: call=%+v err=%v", call, err)
	}
	attempt, err := store.StartHook(ctx, claim, clock)
	if err != nil || attempt.Attempt.Number != 3 {
		t.Fatalf("recovery did not resume with retained attempt history: attempt=%+v err=%v", attempt, err)
	}
	if _, started, err := store.BeginTrackedOperation(ctx, claim, clock, compensationRestartBinding(), operationInputEqual); !errors.Is(err, transition.ErrOperationUnresolved) || started {
		t.Fatalf("restarted recovery redispatched an unknown compensation: started=%v err=%v", started, err)
	}
	if providerCalls.Load() != 1 || providerEffects.Load() != 1 {
		t.Fatalf("restarted recovery repeated the compensation effect: calls=%d effects=%d", providerCalls.Load(), providerEffects.Load())
	}
	if err := store.ReportHook(ctx, claim, transition.HookResult{
		Failure: fsm.Failure{Code: "refund-outcome-unknown"}, Disposition: fsm.DispositionIntervention,
	}, clock); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	progress, err := store.ReadHookProgress(ctx, "recovery-execution")
	if err != nil || progress.BudgetUsed != 2 || len(progress.Attempts) != 3 ||
		!progress.Attempts[1].Unknown || progress.Attempts[1].Kind != transition.HookRecover ||
		progress.Attempts[2].Kind != transition.HookRecover {
		t.Fatalf("recovery budget/history reset across process restart: progress=%+v err=%v", progress, err)
	}
	execution, err := store.InspectExecution(ctx, "recovery-execution")
	if err != nil || execution.Status != transition.NeedsIntervention || !execution.CycleDeadline.Equal(control.Deadline) ||
		len(execution.Controls) != 1 || execution.Controls[0].Control.ID != "abort" {
		t.Fatalf("recovery phase or deadline changed after restart: execution=%+v err=%v", execution, err)
	}
}

// OperationRecoveryRestartChild terminates after the provider effect and before
// persisting a tracked compensation result.
func OperationRecoveryRestartChild(t *testing.T, open func(string) (RecoveryOperationRestartStore, error)) {
	t.Helper()
	address := os.Getenv(OperationRecoveryRestartAddress)
	start, err := time.Parse(time.RFC3339Nano, os.Getenv(OperationRecoveryRestartStart))
	if err != nil {
		t.Fatal(err)
	}
	now := start
	clock := func() time.Time { return now }
	store, err := open(address)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	lease, err := store.ClaimHook(ctx, "recovery-worker", clock, time.Minute)
	if err != nil {
		t.Fatalf("child claim recovery hook: %v", err)
	}
	_, call, err := store.InspectHook(ctx, lease, clock)
	if err != nil || call.Kind != transition.HookRecover {
		t.Fatalf("child did not claim recovery: call=%+v err=%v", call, err)
	}
	if _, err := store.StartHook(ctx, lease, clock); err != nil {
		t.Fatal(err)
	}
	binding := compensationRestartBinding()
	if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, binding, operationInputEqual); err != nil || !started {
		t.Fatalf("start tracked compensation: started=%v err=%v", started, err)
	}
	response, err := http.Post(os.Getenv(OperationRecoveryRestartProvider), "application/octet-stream", bytes.NewReader(binding.Input))
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil || response.StatusCode != http.StatusOK || string(data) != "refund-receipt" {
		t.Fatalf("provider response: status=%d data=%q read=%v close=%v", response.StatusCode, data, readErr, closeErr)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	os.Exit(0)
}
