package runtimecontract

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type classifiedOperationCall struct {
	result transition.OperationResult
	err    error
}

type classifiedDispatchHandler struct {
	fsm.NoopHandler[string, string, string, string]
	outcome  transition.OperationOutcome
	now      time.Time
	result   chan<- classifiedOperationCall
	identity chan<- transition.OperationIdentity
}

func (h classifiedDispatchHandler) Enter(ctx context.Context, _ fsm.ExecutionContext[string, string, string, string]) error {
	codec := transition.CodecVersion{Name: "receipt", Version: "v1"}
	req := transition.OperationRequest{
		Key: "payment", Kind: "charge", BehaviorVersion: "v1", ComparatorVersion: "bytes-v1",
		Input: []byte("amount=100"), ResultCodec: codec,
		Preflight:     func(context.Context) error { return nil },
		ValidateInput: func(context.Context, []byte) error { return nil },
		EqualInput: func(_ context.Context, left, right []byte) (bool, error) {
			return bytes.Equal(left, right), nil
		},
		ValidateResult: func(context.Context, transition.OperationResult) error { return nil },
		ClassifyDispatch: func(_ context.Context, identity transition.OperationIdentity, _ error) (transition.OperationAssessment, error) {
			h.identity <- identity
			assessment := transition.OperationAssessment{
				Outcome: h.outcome,
				Evidence: transition.OperationEvidence{
					Identity: identity, Outcome: h.outcome,
					Provider: "payment-sandbox", Scope: "merchant-7", Reference: "charge-1",
					InputFingerprint: "charge-input-v1", Version: "lookup-v1", Provenance: "authoritative-query",
					BehaviorVersion: "v1", ComparatorVersion: "bytes-v1",
					ObservedAt: h.now, ValidUntil: h.now.Add(time.Minute),
				},
			}
			result := transition.OperationResult{Codec: codec}
			if h.outcome == transition.OperationOutcomeSucceeded {
				result.Data = []byte("receipt-100")
			} else {
				result.FailureCode = "declined"
			}
			assessment.Result = &result
			return assessment, nil
		},
		VerifyEvidence: func(context.Context, transition.OperationBinding, transition.OperationEvidence) error { return nil },
	}
	result, err := transition.TrackOperation(ctx, req, func(context.Context, transition.OperationIdentity) (transition.OperationResult, error) {
		return transition.OperationResult{}, errors.New("provider response lost")
	})
	h.result <- classifiedOperationCall{result: result, err: err}
	return nil
}

// RunOperationDispatchClassification verifies same-attempt provider failures
// classified as success or definitive failure become inspectable terminal results.
func RunOperationDispatchClassification(t *testing.T, newStore func(*testing.T) OperationStore) {
	t.Helper()
	for _, outcome := range []transition.OperationOutcome{
		transition.OperationOutcomeSucceeded,
		transition.OperationOutcomeFailed,
	} {
		t.Run(string(outcome), func(t *testing.T) {
			ctx := context.Background()
			clock := &timerClock{now: time.Now().UTC()}
			now := clock.Now()
			calls := make(chan classifiedOperationCall, 1)
			identity := make(chan transition.OperationIdentity, 1)
			store := newStore(t)
			machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
				ID: "classified-operation", Version: "1", InitialStates: []string{"open"},
				Handlers: map[string]fsm.Handler[string, string, string, string]{
					"open": classifiedDispatchHandler{outcome: outcome, now: now, result: calls, identity: identity},
				},
				SelectInitial: func(string) (fsm.Selection[string], error) {
					return fsm.Selection[string]{Target: "open", Policy: fsm.Policy{
						MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour,
					}}, nil
				},
				SelectTransition: func(string, string, string) (fsm.Selection[string], error) {
					return fsm.Selection[string]{}, errors.New("unused")
				},
				ClassifyFailure: func(error) (fsm.Failure, fsm.Disposition) {
					return fsm.Failure{Code: "unexpected-handler-failure"}, fsm.DispositionIntervention
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			var registry fsm.Registry[string, string, string, string]
			if err := registry.Register(machine); err != nil {
				t.Fatal(err)
			}
			resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return stringCodec{}, nil }
			acceptor, err := transition.NewAcceptor[string, string, string, string](
				store, &registry, resolve, resolve, func() (string, error) { return "classified-" + string(outcome), nil }, clock.Now,
			)
			if err != nil {
				t.Fatal(err)
			}
			execution, _, err := acceptor.Accept(ctx, transition.Request[string, string, string, string]{
				Entity: "entity", Key: "request", Operation: transition.Initialize,
				MachineID: "classified-operation", MachineVersion: "1",
				Input: "payload", InputCodec: transition.CodecVersion{Name: "string", Version: "1"},
				Notification: transition.NotifyInherit,
			})
			if err != nil {
				t.Fatal(err)
			}
			runner, err := transition.NewHookRunner[string, string, string, string](&registry, resolve, resolve)
			if err != nil {
				t.Fatal(err)
			}
			coordinator, err := transition.NewCoordinator[string, string](store, runner, transition.CoordinatorConfig{
				ID: "coordinator", LeaseTTL: time.Hour, Clock: clock.Now,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := coordinator.Reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			worker, err := transition.NewHookWorker[string, string, string, string](store, runner, transition.WorkerConfig{
				ID: "worker", Capacity: 1, LeaseTTL: time.Minute, RenewalInterval: time.Second, Clock: clock,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := worker.RunOne(ctx); err != nil {
				t.Fatalf("classified dispatch did not finish its attempt: %v", err)
			}
			var call classifiedOperationCall
			select {
			case call = <-calls:
			case <-time.After(5 * time.Second):
				t.Fatal("classified dispatch handler did not return")
			}
			switch outcome {
			case transition.OperationOutcomeSucceeded:
				if call.err != nil || string(call.result.Data) != "receipt-100" || call.result.FailureCode != "" {
					t.Fatalf("classified success was not returned: result=%+v err=%v", call.result, call.err)
				}
			case transition.OperationOutcomeFailed:
				if !errors.Is(call.err, transition.ErrOperationFailed) || call.result.FailureCode != "declined" {
					t.Fatalf("classified failure was not returned: result=%+v err=%v", call.result, call.err)
				}
			}
			var trackedIdentity transition.OperationIdentity
			select {
			case trackedIdentity = <-identity:
			case <-time.After(5 * time.Second):
				t.Fatal("dispatch classifier did not run")
			}
			if trackedIdentity.ExecutionID != execution.ID {
				t.Fatalf("tracked operation execution mismatch: %s != %s", trackedIdentity.ExecutionID, execution.ID)
			}
			inspection, err := store.InspectTrackedOperation(ctx, trackedIdentity)
			if err != nil || inspection.Result == nil || inspection.DispatchPending || !inspection.MayHaveDispatched {
				t.Fatalf("classified result was not durably inspectable: inspection=%+v err=%v", inspection, err)
			}
			if outcome == transition.OperationOutcomeSucceeded &&
				(string(inspection.Result.Data) != "receipt-100" || inspection.Result.FailureCode != "") {
				t.Fatalf("classified success result changed: %+v", inspection.Result)
			}
			if outcome == transition.OperationOutcomeFailed &&
				(inspection.Result.FailureCode != "declined" || inspection.Result.Data != nil) {
				t.Fatalf("classified failure result changed: %+v", inspection.Result)
			}
		})
	}
}
