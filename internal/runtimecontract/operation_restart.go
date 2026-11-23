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
	OperationRestartAddress = "RFF_OPERATION_RESTART_ADDRESS"
	OperationRestartPhase   = "RFF_OPERATION_RESTART_PHASE"
	operationProviderURL    = "RFF_OPERATION_RESTART_PROVIDER"
)

type RestartOperationStore interface {
	OperationStore
	transition.OperationReconciliationStore
	Close() error
}

func restartBinding() transition.OperationBinding {
	return transition.OperationBinding{
		Identity: transition.OperationIdentity{ExecutionID: "execution", Key: "charge"},
		Kind:     "charge", BehaviorVersion: "1", ComparatorVersion: "1",
		Input: []byte("amount=100"), ResultCodec: transition.CodecVersion{Name: "result", Version: "1"},
	}
}

// RunOperationLateCallRestart kills a real worker process while its provider
// request is held open, then proves an inconclusive not-found lookup cannot
// authorize a second dispatch or accept the stale worker's report.
func RunOperationLateCallRestart(t *testing.T, address string, open func(string) (RestartOperationStore, error)) {
	t.Helper()
	ctx := context.Background()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := start
	clock := func() time.Time { return now }
	store, err := open(address)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = store.Accept(ctx, "entity", "request", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "request",
			Operation: transition.Initialize, Status: transition.Waiting, Deadline: start.Add(time.Hour),
			Policy: fsm.Policy{MaxAttempts: 3, AttemptTimeout: time.Minute, OverallTimeout: time.Hour, RetryDelays: []time.Duration{0}},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	fence, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	activityID, err := store.DispatchHook(ctx, fence, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: start})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	providerStarted := make(chan struct{}, 1)
	releaseProvider := make(chan struct{})
	providerFinished := make(chan struct{})
	var providerCalls atomic.Int32
	var providerEffects atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		providerCalls.Add(1)
		providerStarted <- struct{}{}
		<-releaseProvider
		providerEffects.Add(1)
		_, _ = w.Write([]byte("late-receipt"))
		close(providerFinished)
	}))
	defer provider.Close()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable, "-test.run=^TestOperationProcessRestart$")
	command.Env = append(os.Environ(),
		OperationRestartAddress+"="+address,
		OperationRestartPhase+"=late",
		operationProviderURL+"="+provider.URL,
	)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-providerStarted:
	case <-time.After(10 * time.Second):
		_ = command.Process.Kill()
		_ = command.Wait()
		close(releaseProvider)
		t.Fatal("late provider call did not start")
	}
	if err := command.Process.Kill(); err != nil {
		close(releaseProvider)
		t.Fatal(err)
	}
	_ = command.Wait()

	store, err = open(address)
	if err != nil {
		close(releaseProvider)
		t.Fatal(err)
	}
	now = start.Add(2 * time.Second)
	defer func() { _ = store.Close() }()
	inspection, err := store.InspectTrackedOperation(ctx, restartBinding().Identity)
	if err != nil || inspection.Result != nil || inspection.ActivityID == "" {
		close(releaseProvider)
		t.Fatalf("process loss did not retain an unknown dispatch: %+v %v", inspection, err)
	}
	if err := store.ResolveUnknownHook(ctx, fence, clock, activityID, 1, transition.HookResult{
		Failure: fsm.Failure{Code: "worker-lost"}, Disposition: fsm.DispositionRetry,
	}); err != nil {
		close(releaseProvider)
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, fence, clock, transition.HookDispatch{
		Kind: transition.HookRetry, RetryCycle: 1, DueAt: now,
		Failure: &fsm.Failure{Code: "worker-lost"},
	}); err != nil {
		close(releaseProvider)
		t.Fatal(err)
	}
	replacement, err := store.ClaimHook(ctx, "replacement-worker", clock, time.Minute)
	if err != nil {
		close(releaseProvider)
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, replacement, clock); err != nil {
		close(releaseProvider)
		t.Fatal(err)
	}
	binding := restartBinding()
	evidence := transition.OperationEvidence{
		Identity: binding.Identity, Outcome: transition.OperationOutcomeAbsent,
		Provider: "fake-provider", Scope: "merchant-1", Reference: "lookup-1",
		InputFingerprint: "operation-input-fingerprint-v1",
		Version:          "lookup-v1", Provenance: "not-found",
		BehaviorVersion: binding.BehaviorVersion, ComparatorVersion: binding.ComparatorVersion,
		ObservedAt: now, ValidUntil: now.Add(time.Minute), Quiesced: false,
	}
	reconciliation, err := store.ReconcileTrackedOperation(ctx, replacement, clock, binding, transition.OperationAssessment{
		Outcome: transition.OperationOutcomeAbsent, Evidence: evidence,
	}, func(_ context.Context, left, right []byte) (bool, error) { return bytes.Equal(left, right), nil })
	if err != nil || reconciliation.Dispatch || reconciliation.Resolved {
		close(releaseProvider)
		t.Fatalf("inconclusive absence authorized repeat: %+v %v", reconciliation, err)
	}
	if _, started, err := store.BeginTrackedOperation(ctx, replacement, clock, binding, func(_ context.Context, left, right []byte) (bool, error) {
		return bytes.Equal(left, right), nil
	}); !errors.Is(err, transition.ErrOperationUnresolved) || started {
		close(releaseProvider)
		t.Fatalf("unknown late call was redispatched: started=%v err=%v", started, err)
	}
	oldLease := transition.HookLease{
		ExecutionID: "execution", ActivityID: inspection.ActivityID, Generation: inspection.Generation,
	}
	if err := store.CompleteTrackedOperation(ctx, oldLease, clock, binding.Identity, transition.OperationResult{
		Codec: binding.ResultCodec, Data: []byte("stale-receipt"),
	}); !errors.Is(err, transition.ErrStaleActivityLease) {
		close(releaseProvider)
		t.Fatalf("stale process report was not fenced: %v", err)
	}
	close(releaseProvider)
	select {
	case <-providerFinished:
	case <-time.After(10 * time.Second):
		t.Fatal("delayed provider call did not finish")
	}
	if providerCalls.Load() != 1 || providerEffects.Load() != 1 {
		t.Fatalf("late provider call was repeated or failed to take effect: calls=%d effects=%d", providerCalls.Load(), providerEffects.Load())
	}
}

// OperationRestartChild terminates after a durable dispatch or result boundary.
func OperationRestartChild(t *testing.T, open func(string) (RestartOperationStore, error)) {
	t.Helper()
	store, err := open(os.Getenv(OperationRestartAddress))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return start }
	lease, err := store.ClaimHook(ctx, "lost-worker", clock, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, lease, clock); err != nil {
		t.Fatal(err)
	}
	equal := func(_ context.Context, a, b []byte) (bool, error) { return bytes.Equal(a, b), nil }
	binding := restartBinding()
	if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, binding, equal); err != nil || !started {
		t.Fatalf("start operation: started=%v err=%v", started, err)
	}
	phase := os.Getenv(OperationRestartPhase)
	if phase != "intent" {
		response, err := http.Post(os.Getenv(operationProviderURL), "application/octet-stream", bytes.NewReader([]byte("charge")))
		if err != nil {
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		if readErr != nil || closeErr != nil || response.StatusCode != http.StatusOK || string(data) != "receipt" {
			t.Fatalf("provider result: status=%d data=%q read=%v close=%v", response.StatusCode, data, readErr, closeErr)
		}
	}
	if phase == "result" {
		result := transition.OperationResult{Codec: binding.ResultCodec, Data: []byte("receipt")}
		if err := store.CompleteTrackedOperation(ctx, lease, clock, binding.Identity, result); err != nil {
			t.Fatal(err)
		}
	}
	os.Exit(0)
}

// RunOperationRestart exercises a real process exit and a later hook attempt.
func RunOperationRestart(t *testing.T, address, phase string, open func(string) (RestartOperationStore, error)) {
	t.Helper()
	ctx := context.Background()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := start
	clock := func() time.Time { return now }
	store, err := open(address)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = store.Accept(ctx, "entity", "request", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "request",
			Operation: transition.Initialize, Status: transition.Waiting, Deadline: start.Add(time.Hour),
			Policy: fsm.Policy{MaxAttempts: 3, AttemptTimeout: time.Minute, OverallTimeout: time.Hour, RetryDelays: []time.Duration{0}},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	fence, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	activityID, err := store.DispatchHook(ctx, fence, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: start})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	var providerCalls atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		providerCalls.Add(1)
		_, _ = w.Write([]byte("receipt"))
	}))
	defer provider.Close()
	command := exec.Command(executable, "-test.run=^TestOperationProcessRestart$")
	command.Env = append(os.Environ(), OperationRestartAddress+"="+address, OperationRestartPhase+"="+phase, operationProviderURL+"="+provider.URL)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("operation child: %v output=%s", err, output)
	}
	expectedCalls := int32(1)
	if phase == "intent" {
		expectedCalls = 0
	}
	if providerCalls.Load() != expectedCalls {
		t.Fatalf("provider calls after process loss = %d, want %d", providerCalls.Load(), expectedCalls)
	}
	store, err = open(address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now = start.Add(2 * time.Second)
	failure := fsm.Failure{Code: "worker-lost"}
	if err := store.ResolveUnknownHook(ctx, fence, clock, activityID, 1, transition.HookResult{Failure: failure, Disposition: fsm.DispositionRetry}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, fence, clock, transition.HookDispatch{Kind: transition.HookRetry, RetryCycle: 1, DueAt: now, Failure: &failure}); err != nil {
		t.Fatal(err)
	}
	lease, err := store.ClaimHook(ctx, "replacement", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, lease, clock); err != nil {
		t.Fatal(err)
	}
	binding := restartBinding()
	equal := func(_ context.Context, a, b []byte) (bool, error) { return bytes.Equal(a, b), nil }
	result, started, err := store.BeginTrackedOperation(ctx, lease, clock, binding, equal)
	if phase == "result" {
		if err != nil || started || result == nil || string(result.Data) != "receipt" {
			t.Fatalf("durable result did not replay: result=%+v started=%v error=%v", result, started, err)
		}
	} else if !errors.Is(err, transition.ErrOperationUnresolved) || started {
		t.Fatalf("uncertain dispatch did not block: result=%+v started=%v error=%v", result, started, err)
	}
	if providerCalls.Load() != expectedCalls {
		t.Fatal("restart repeated the provider call")
	}
}
