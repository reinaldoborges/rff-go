package successorcontract

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	"github.com/reinaldoborges/rff-go/internal/continuationcontract"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type Store interface {
	continuationcontract.Store
	transition.SuccessorPreparationStore[string, string]
	transition.SuccessorRecoveryStore[string, string]
}

type AtomicDecisionStore interface {
	Store
	AcceptSuccessorAtomicDecision(context.Context, transition.ContinuationMarker[string, string],
		func(*Binding) (*Binding, error),
		func(context.Context, *sql.Tx, Binding) (Result, error),
	) (transition.Execution[string, string], bool, error)
}

func RunAtomicDecisions(t *testing.T, store AtomicDecisionStore, host *Host) {
	t.Helper()
	ctx := context.Background()
	for _, tt := range []struct {
		entity  string
		outcome transition.SuccessorHostOutcome
		phase   transition.SuccessorPhase
		want    error
	}{
		{"shared-stale", transition.SuccessorHostSuperseded, transition.SuccessorSuperseded, transition.ErrSuccessorSuperseded},
		{"shared-denied", transition.SuccessorHostDenied, transition.SuccessorDenied, transition.ErrSuccessorDenied},
	} {
		binding := Setup(t, store, host, tt.entity)
		decide := func(existing *Binding) (*Binding, error) {
			if existing != nil {
				return nil, nil
			}
			return &binding, nil
		}
		accept := func(_ context.Context, _ *sql.Tx, b Binding) (Result, error) {
			if tt.outcome == transition.SuccessorHostDenied {
				return Result{Outcome: tt.outcome, Principal: "worker"}, nil
			}
			return Result{Outcome: tt.outcome, Cancellation: &transition.SuccessorCancellation[string, string]{
				Binding: b, Evidence: []byte("fenced-stale"), Superseded: true,
			}}, nil
		}
		if _, _, err := store.AcceptSuccessorAtomicDecision(ctx, binding.Origin, decide, accept); !errors.Is(err, tt.want) {
			t.Fatalf("%s decision: %v", tt.entity, err)
		}
		inspection, err := store.InspectSuccessor(ctx, binding.Origin)
		if err != nil || inspection.Phase != tt.phase {
			t.Fatalf("%s decision must be durable: %+v error=%v", tt.entity, inspection, err)
		}
		if _, _, err := store.AcceptSuccessorAtomicDecision(ctx, binding.Origin, decide, func(context.Context, *sql.Tx, Binding) (Result, error) {
			t.Fatal("definitive decision retried without intervention")
			return Result{}, nil
		}); !errors.Is(err, tt.want) {
			t.Fatalf("%s replay: %v", tt.entity, err)
		}
	}
}

type stringCodec struct{}

func (stringCodec) Encode(_ context.Context, value string) ([]byte, error) { return []byte(value), nil }
func (stringCodec) Decode(_ context.Context, value []byte) (string, error) { return string(value), nil }
func (stringCodec) Equal(_ context.Context, stored []byte, incoming string) (bool, error) {
	return string(stored) == incoming, nil
}

func Setup(t *testing.T, store Store, host *Host, entity string) Binding {
	t.Helper()
	ctx := context.Background()
	policy := fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour}
	marker := transition.ContinuationMarker[string, string]{
		SchemaVersion: 1, ExecutionID: "origin-" + entity, Entity: entity, Target: "due",
		MachineID: "plan", MachineVersion: "1", Policy: policy,
	}
	marker = continuationcontract.PrepareOrigin(t, store, marker)
	if err := store.RecordContinuationMarker(ctx, marker); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	lease, err := store.AcquireTransitionLease(ctx, entity, marker.ExecutionID, "owner", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCommitReceipt(ctx, lease, clock, transition.CommitReceipt{
		ExecutionID: marker.ExecutionID, OperationID: marker.OperationID, Outcome: transition.CommitSucceeded,
	}); err != nil {
		t.Fatal(err)
	}
	if err := host.Seed(entity); err != nil {
		t.Fatal(err)
	}
	return BindingFor(marker)
}

func BindingFor(marker transition.ContinuationMarker[string, string]) Binding {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return Binding{
		Origin: marker, OriginRevision: []byte("revision-1"),
		Execution: transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "successor-" + marker.Entity, Entity: marker.Entity,
			Key: "successor-" + marker.Entity, Operation: transition.Transition, Previous: marker.Target, HasPrevious: true,
			Target: "paid", MachineID: "plan", MachineVersion: "1", Status: transition.Waiting,
			Policy: marker.Policy, AcceptedAt: now, Deadline: now.Add(time.Hour),
			Event:                transition.EncodedValue{Codec: transition.CodecVersion{Name: "event", Version: "1"}, Data: []byte("pay")},
			Input:                transition.EncodedValue{Codec: transition.CodecVersion{Name: "input", Version: "1"}, Data: []byte("payload")},
			NotificationOverride: transition.NotifyInherit,
		},
	}
}

func PrepareBinding(t *testing.T, store Store, binding Binding) Binding {
	t.Helper()
	prepared, created, err := store.PrepareSuccessor(context.Background(), binding.Origin, func(*Binding) (*Binding, error) { return &binding, nil })
	if err != nil || !created {
		t.Fatalf("prepare successor: created=%v error=%v", created, err)
	}
	return prepared
}

func Prepare(t *testing.T, store Store, host *Host, entity string) Binding {
	t.Helper()
	return PrepareBinding(t, store, Setup(t, store, host, entity))
}

// Run exercises a real, separate host database and adapter store.
func Run(t *testing.T, store Store, host *Host) {
	t.Helper()
	ctx := context.Background()
	concurrent := Setup(t, store, host, "concurrent")
	aborted := errors.New("competitor aborted")
	_, _, err := store.Accept(ctx, concurrent.Origin.Entity, "aborted", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return nil, aborted
	})
	if !errors.Is(err, aborted) {
		t.Fatalf("aborted competitor: %v", err)
	}
	type outcome struct {
		binding Binding
		created bool
		err     error
	}
	results := make(chan outcome, 8)
	for range 8 {
		go func() {
			binding, created, err := store.PrepareSuccessor(ctx, concurrent.Origin, func(existing *Binding) (*Binding, error) {
				if existing != nil {
					return nil, nil
				}
				return &concurrent, nil
			})
			results <- outcome{binding, created, err}
		}()
	}
	createdCount := 0
	for range 8 {
		result := <-results
		if result.err != nil || result.binding.Execution.ID != concurrent.Execution.ID {
			t.Fatalf("concurrent duplicate prepare: %+v", result)
		}
		if result.created {
			createdCount++
		}
	}
	if createdCount != 1 {
		t.Fatalf("concurrent duplicate created %d successors", createdCount)
	}
	binding := Prepare(t, store, host, "accepted")
	recovery, err := transition.NewSuccessorRecovery[string, string](store, host)
	if err != nil {
		t.Fatal(err)
	}
	host.Authorize = "temporary"
	if err := recovery.Reconcile(ctx, binding.Origin, true); err == nil {
		t.Fatal("temporary authorization failure must remain unresolved")
	}
	host.Authorize, host.LoseReply = "allow", true
	if err := recovery.Reconcile(ctx, binding.Origin, true); err == nil {
		t.Fatal("injected lost host response must be reported")
	}
	if err := host.Write("accepted", "outside", "revision-2"); err == nil {
		t.Fatal("direct state writer bypassed the durable host fence")
	}
	if err := host.Seed("unrelated"); err != nil {
		t.Fatal(err)
	}
	if err := host.Write("unrelated", "outside", "revision-2"); err != nil {
		t.Fatalf("unrelated entity could not progress: %v", err)
	}
	host.Authorize = "deny"
	if err := recovery.Reconcile(ctx, binding.Origin, false); err != nil {
		t.Fatalf("recovery must finalize the accepted receipt without reauthorization: %v", err)
	}
	inspection, err := store.InspectSuccessor(ctx, binding.Origin)
	if err != nil || inspection.Phase != transition.SuccessorAccepted || inspection.Principal != "test-worker" {
		t.Fatalf("accepted receipt or audit missing: %+v error=%v", inspection, err)
	}
	clock := func() time.Time { return time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC) }
	lease, err := store.AcquireTransitionLease(ctx, binding.Origin.Entity, binding.Execution.ID, "successor-owner", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, lease, clock, transition.HookDispatch{Kind: transition.HookTransition, DueAt: clock()}); err != nil {
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
	operation, err := store.MarkWorkSuccess(ctx, lease, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Complete("accepted", binding.Execution.ID, "revision-2", operation); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCommitReceipt(ctx, lease, clock, transition.CommitReceipt{
		ExecutionID: binding.Execution.ID, OperationID: operation, Outcome: transition.CommitSucceeded,
	}); err != nil {
		t.Fatal(err)
	}
	if err := host.Write("accepted", "due", "revision-3"); err != nil {
		t.Fatal(err)
	}
	prior, err := host.LookupSuccessor(ctx, binding)
	if err != nil || prior.Outcome != transition.SuccessorHostAccepted || prior.Receipt.Binding.Execution.ID != binding.Execution.ID {
		t.Fatalf("accepted receipt must win after later state changes: %+v error=%v", prior, err)
	}
	var registry fsm.Registry[string, string, string, string]
	resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return stringCodec{}, nil }
	acceptor, err := transition.NewAcceptor(store, &registry, resolve, resolve, func() (string, error) { return "unused", nil }, clock)
	if err != nil {
		t.Fatal(err)
	}
	private, err := acceptor.InspectSuccessor(ctx, binding.Origin, func(context.Context, transition.ContinuationMarker[string, string]) error {
		return errors.New("private authorization details")
	})
	if !errors.Is(err, transition.ErrSuccessorReplayDenied) || private.Binding.Execution.ID != "" {
		t.Fatalf("unauthorized inspection disclosed data: %+v error=%v", private, err)
	}
	private, err = acceptor.InspectSuccessor(ctx, binding.Origin, func(context.Context, transition.ContinuationMarker[string, string]) error { return nil })
	if err != nil || private.Phase != transition.SuccessorAccepted || private.Binding.Execution.ID != binding.Execution.ID {
		t.Fatalf("authorized inspection: %+v error=%v", private, err)
	}
	request := transition.SuccessorRequest[string, string, string, string]{
		Origin: binding.Origin, OriginRevision: binding.OriginRevision,
		Request: transition.Request[string, string, string, string]{
			Entity: binding.Execution.Entity, Key: binding.Execution.Key, Operation: transition.Transition,
			Previous: binding.Execution.Previous, HasPrevious: true, MachineID: binding.Execution.MachineID,
			MachineVersion: binding.Execution.MachineVersion, Event: "pay", HasEvent: true,
			EventCodec: binding.Execution.Event.Codec, Input: "payload", InputCodec: binding.Execution.Input.Codec,
			Notification: transition.NotifyInherit,
		},
	}
	allow := func(context.Context, transition.SuccessorRequest[string, string, string, string]) error { return nil }
	disclosed, err := acceptor.ReplaySuccessor(ctx, request, allow)
	if err != nil || disclosed.ID != binding.Execution.ID || disclosed.Status != transition.Complete {
		t.Fatalf("authorized replay after state change: %+v error=%v", disclosed, err)
	}
	request.Request.Input = "changed"
	if _, err := acceptor.ReplaySuccessor(ctx, request, allow); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("changed replay semantics must conflict: %v", err)
	}
	changed := binding
	changed.Execution.Input.Data = []byte("changed")
	if _, err := host.LookupSuccessor(ctx, changed); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("changed semantics must conflict: %v", err)
	}

	stale := Prepare(t, store, host, "stale")
	if err := host.Write("stale", "other", "revision-2"); err != nil {
		t.Fatal(err)
	}
	if err := host.Write("stale", "due", "revision-3"); err != nil {
		t.Fatal(err)
	}
	host.Authorize = "allow"
	if err := recovery.Reconcile(ctx, stale.Origin, true); !errors.Is(err, transition.ErrSuccessorSuperseded) {
		t.Fatalf("ABA must supersede unaccepted work: %v", err)
	}
	inspection, err = store.InspectSuccessor(ctx, stale.Origin)
	if err != nil || inspection.Phase != transition.SuccessorSuperseded {
		t.Fatalf("supersession not durable: %+v error=%v", inspection, err)
	}

	denied := Prepare(t, store, host, "denied")
	host.Authorize = "deny"
	if err := recovery.Reconcile(ctx, denied.Origin, true); !errors.Is(err, transition.ErrSuccessorDenied) {
		t.Fatalf("authorization denial must require intervention: %v", err)
	}
	host.Authorize = "allow"
	if err := recovery.Reconcile(ctx, denied.Origin, true); !errors.Is(err, transition.ErrSuccessorDenied) {
		t.Fatalf("denied work resumed without intervention: %v", err)
	}
}
