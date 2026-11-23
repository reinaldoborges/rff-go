package continuationcontract

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type DriveStore interface {
	Store
	transition.SuccessorRecoveryStore[string, string]
	AcquireContinuation(context.Context, transition.ContinuationMarker[string, string], string, func() time.Time, time.Duration) (transition.ContinuationLease, error)
	StartContinuation(context.Context, transition.ContinuationLease, func() time.Time) error
	SetContinuationCompatibility(context.Context, transition.ContinuationLease, func() time.Time, bool) error
	ParkContinuation(context.Context, transition.ContinuationLease, func() time.Time) error
	ResumeContinuation(context.Context, transition.ContinuationLease, func() time.Time) error
	FailContinuationAttempt(context.Context, transition.ContinuationLease, func() time.Time, fsm.Failure, fsm.Disposition) error
	RecoverContinuationAttempt(context.Context, transition.ContinuationLease, func() time.Time) error
	ApplyContinuationControl(context.Context, transition.ContinuationMarker[string, string], func() time.Time, transition.ContinuationControl) error
	PrepareContinuationSuccessor(context.Context, transition.ContinuationLease, func() time.Time, func(*transition.SuccessorBinding[string, string]) (*transition.SuccessorBinding[string, string], error)) (transition.SuccessorBinding[string, string], bool, error)
	ReconcileContinuation(context.Context, transition.ContinuationMarker[string, string], func() time.Time) error
	RecordContinuationDenial(context.Context, transition.SuccessorBinding[string, string], string, transition.ContinuationDeliveryPermit, func() time.Time) error
	ResolveContinuationAbsent(context.Context, transition.ContinuationMarker[string, string], transition.ContinuationDeliveryPermit, func() time.Time) error
	RecordContinuationDecision(context.Context, transition.ContinuationLease, func() time.Time, transition.ContinuationDecision) error
	ReadContinuation(context.Context, string) (transition.ContinuationProgress, error)
}

func RunDrivePause(t *testing.T, store DriveStore) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	marker := transition.ContinuationMarker[string, string]{
		SchemaVersion: 1, ExecutionID: "origin", Entity: "entity", Target: "due",
		MachineID: "plan", MachineVersion: "1",
		Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: 10 * time.Second, OverallTimeout: 20 * time.Second},
	}
	marker = PrepareOrigin(t, store, marker)
	if err := store.RecordContinuationMarker(ctx, marker); err != nil {
		t.Fatal(err)
	}
	originLease, err := store.AcquireTransitionLease(ctx, marker.Entity, marker.ExecutionID, "owner", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCommitReceipt(ctx, originLease, clock, transition.CommitReceipt{
		ExecutionID: marker.ExecutionID, OperationID: marker.OperationID, Outcome: transition.CommitSucceeded,
	}); err != nil {
		t.Fatal(err)
	}
	drive, err := store.AcquireContinuation(ctx, marker, "worker-a", clock, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StartContinuation(ctx, drive, clock); err != nil {
		t.Fatal(err)
	}
	if err := store.ParkContinuation(ctx, drive, clock); !errors.Is(err, transition.ErrInvalidDecision) {
		t.Fatalf("a non-existent reservation must not pause time: %v", err)
	}
	now = now.Add(3 * time.Second)
	competing := marker
	competing.ExecutionID = "competing"
	competing = PrepareOrigin(t, store, competing)
	if err := store.ParkContinuation(ctx, drive, clock); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("RFF_CONTINUATION_DRIVE_CRASH") == "after-park" {
		os.Exit(87)
	}
	now = now.Add(100 * time.Second)
	resumed, err := store.AcquireContinuation(ctx, marker, "worker-b", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ResumeContinuation(ctx, resumed, clock); !errors.Is(err, transition.ErrBusy) {
		t.Fatalf("reservation still held must remain paused: %v", err)
	}
	if err := store.RecordContinuationMarker(ctx, competing); err != nil {
		t.Fatal(err)
	}
	competingLease, err := store.AcquireTransitionLease(ctx, competing.Entity, competing.ExecutionID, "owner", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCommitReceipt(ctx, competingLease, clock, transition.CommitReceipt{
		ExecutionID: competing.ExecutionID, OperationID: competing.OperationID, Outcome: transition.CommitSucceeded,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.ResumeContinuation(ctx, resumed, clock); err != nil {
		t.Fatal(err)
	}
	now = now.Add(6 * time.Second)
	if err := store.RecordContinuationDecision(ctx, resumed, clock, transition.ContinuationDecision{}); err != nil {
		t.Fatalf("remaining attempt time should survive pause: %v", err)
	}
	progress, err := store.ReadContinuation(ctx, marker.ExecutionID)
	if err != nil || progress.Attempts != 1 || progress.Phase != transition.ContinuationStopped ||
		!progress.StartedAt.Equal(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("resumed drive must retain attempt: %+v, %v", progress, err)
	}
}

func RunDriveFencing(t *testing.T, store DriveStore) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	marker := transition.ContinuationMarker[string, string]{
		SchemaVersion: 1, ExecutionID: "origin", Entity: "entity", Target: "due",
		MachineID: "plan", MachineVersion: "1",
		Policy: fsm.Policy{MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
	}

	marker = PrepareOrigin(t, store, marker)
	if err := store.RecordContinuationMarker(ctx, marker); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcquireContinuation(ctx, marker, "worker-a", clock, 10*time.Second); !errors.Is(err, transition.ErrContinuationPending) {
		t.Fatalf("unreconciled origin must not start a drive: %v", err)
	}
	lease, err := store.AcquireTransitionLease(ctx, marker.Entity, marker.ExecutionID, "owner", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCommitReceipt(ctx, lease, clock, transition.CommitReceipt{
		ExecutionID: marker.ExecutionID, OperationID: marker.OperationID, Outcome: transition.CommitSucceeded,
	}); err != nil {
		t.Fatal(err)
	}
	first, err := store.AcquireContinuation(ctx, marker, "worker-a", clock, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	progress, err := store.ReadContinuation(ctx, marker.ExecutionID)
	if err != nil || progress.Attempts != 0 {
		t.Fatalf("claim alone must not charge an attempt: %+v, %v", progress, err)
	}
	now = now.Add(11 * time.Second)
	second, err := store.AcquireContinuation(ctx, marker, "worker-b", clock, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StartContinuation(ctx, first, clock); !errors.Is(err, transition.ErrStaleLease) {
		t.Fatalf("stale claim must not start: %v", err)
	}
	if err := store.StartContinuation(ctx, second, clock); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordContinuationDecision(ctx, first, clock, transition.ContinuationDecision{}); !errors.Is(err, transition.ErrStaleLease) {
		t.Fatalf("stale evaluator must not record a decision: %v", err)
	}
	if err := store.RecordContinuationDecision(ctx, second, clock, transition.ContinuationDecision{}); err != nil {
		t.Fatal(err)
	}
	progress, err = store.ReadContinuation(ctx, marker.ExecutionID)
	if err != nil || progress.Attempts != 1 || progress.Phase != transition.ContinuationStopped || progress.Decision == nil {
		t.Fatalf("no-continuation decision not retained: %+v, %v", progress, err)
	}
}

func RunDriveStartedLeaseLoss(t *testing.T, store DriveStore) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	marker := transition.ContinuationMarker[string, string]{
		SchemaVersion: 1, ExecutionID: "origin", Entity: "entity", Target: "due",
		MachineID: "plan", MachineVersion: "1",
		Policy: fsm.Policy{MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
	}
	marker = PrepareOrigin(t, store, marker)
	if err := store.RecordContinuationMarker(ctx, marker); err != nil {
		t.Fatal(err)
	}
	origin, err := store.AcquireTransitionLease(ctx, marker.Entity, marker.ExecutionID, "owner", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCommitReceipt(ctx, origin, clock, transition.CommitReceipt{
		ExecutionID: marker.ExecutionID, OperationID: marker.OperationID, Outcome: transition.CommitSucceeded,
	}); err != nil {
		t.Fatal(err)
	}
	first, err := store.AcquireContinuation(ctx, marker, "first", clock, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StartContinuation(ctx, first, clock); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("RFF_CONTINUATION_DRIVE_CRASH") == "after-start" {
		os.Exit(87)
	}
	now = now.Add(2 * time.Second)
	second, err := store.AcquireContinuation(ctx, marker, "second", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordContinuationDecision(ctx, second, clock, transition.ContinuationDecision{}); !errors.Is(err, transition.ErrInvalidDecision) {
		t.Fatalf("new claimant cannot report an earlier claimant's attempt: %v", err)
	}
	if err := store.RecordContinuationDecision(ctx, first, clock, transition.ContinuationDecision{}); !errors.Is(err, transition.ErrStaleLease) {
		t.Fatalf("expired evaluator cannot record a decision: %v", err)
	}
	progress, err := store.ReadContinuation(ctx, marker.ExecutionID)
	if err != nil || progress.Attempts != 1 || progress.Decision != nil {
		t.Fatalf("unknown started attempt must remain counted: %+v, %v", progress, err)
	}
	if err := store.RecoverContinuationAttempt(ctx, second, clock); err != nil {
		t.Fatal(err)
	}
	progress, err = store.ReadContinuation(ctx, marker.ExecutionID)
	if err != nil || progress.Attempts != 1 || progress.Phase != transition.ContinuationIntervention || len(progress.History) != 1 {
		t.Fatalf("unknown attempt with no retry schedule must enter intervention: %+v, %v", progress, err)
	}
}

func RunDriveBudget(t *testing.T, store DriveStore) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	marker := transition.ContinuationMarker[string, string]{
		SchemaVersion: 1, ExecutionID: "origin", Entity: "entity", Target: "due",
		MachineID: "plan", MachineVersion: "1",
		Policy: fsm.Policy{MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour, RetryDelays: []time.Duration{5 * time.Second}},
	}
	marker = PrepareOrigin(t, store, marker)
	if err := store.RecordContinuationMarker(ctx, marker); err != nil {
		t.Fatal(err)
	}
	origin, err := store.AcquireTransitionLease(ctx, marker.Entity, marker.ExecutionID, "owner", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCommitReceipt(ctx, origin, clock, transition.CommitReceipt{
		ExecutionID: marker.ExecutionID, OperationID: marker.OperationID, Outcome: transition.CommitSucceeded,
	}); err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireContinuation(ctx, marker, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StartContinuation(ctx, lease, clock); err != nil {
		t.Fatal(err)
	}
	decision := transition.ContinuationDecision{
		Continue: true,
		Event:    transition.EncodedValue{Codec: transition.CodecVersion{Name: string([]byte{0xff, 'e'}), Version: "1"}, Data: []byte("charge")},
		Input:    transition.EncodedValue{Codec: transition.CodecVersion{Name: "input", Version: "1"}, Data: []byte("invoice")},
	}
	if err := store.RecordContinuationDecision(ctx, lease, clock, decision); err != nil {
		t.Fatal(err)
	}
	if err := store.FailContinuationAttempt(ctx, lease, clock, fsm.Failure{Code: "transient"}, fsm.DispositionRetry); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("RFF_CONTINUATION_DRIVE_CRASH") == "after-failure" {
		os.Exit(87)
	}
	lease, err = store.AcquireContinuation(ctx, marker, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.StartContinuation(ctx, lease, clock); !errors.Is(err, transition.ErrContinuationNotDue) {
		t.Fatalf("retry delay must be enforced: %v", err)
	}
	now = now.Add(5 * time.Second)
	if err := store.StartContinuation(ctx, lease, clock); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordContinuationDecision(ctx, lease, clock, transition.ContinuationDecision{}); !errors.Is(err, transition.ErrInvalidDecision) {
		t.Fatalf("recorded decision must never be replaced on retry: %v", err)
	}
	if err := store.FailContinuationAttempt(ctx, lease, clock, fsm.Failure{Code: "transient"}, fsm.DispositionRetry); err != nil {
		t.Fatal(err)
	}
	progress, err := store.ReadContinuation(ctx, marker.ExecutionID)
	if err != nil || progress.Attempts != 2 || progress.Phase != transition.ContinuationIntervention || len(progress.History) != 2 ||
		progress.Decision == nil || !progress.Decision.Continue || string(progress.Decision.Event.Data) != "charge" ||
		progress.Decision.Event.Codec.Name != string([]byte{0xff, 'e'}) {
		t.Fatalf("exhausted cycle must require intervention with history: %+v, %v", progress, err)
	}
	allowed := false
	controls, err := transition.NewContinuationControls[string, string](store, func(_ context.Context, _ transition.ContinuationControlAccess) (string, error) {
		if !allowed {
			return "", errors.New("private authorization detail")
		}
		return "operator", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controls.Inspect(ctx, marker.ExecutionID); !errors.Is(err, transition.ErrContinuationControlDenied) {
		t.Fatalf("unauthorized inspection must not reveal state: %v", err)
	}
	if err := controls.Apply(ctx, marker, clock, "retry-1", transition.RetryContinuation, "fixed"); !errors.Is(err, transition.ErrContinuationControlDenied) {
		t.Fatalf("unauthorized retry must be denied: %v", err)
	}
	allowed = true
	if err := controls.Apply(ctx, marker, clock, "retry-1", transition.RetryContinuation, "fixed"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if err := controls.Apply(ctx, marker, clock, "retry-1", transition.RetryContinuation, "fixed"); err != nil {
		t.Fatalf("identical retry must replay: %v", err)
	}
	allowed = false
	if err := controls.Apply(ctx, marker, clock, "retry-1", transition.RetryContinuation, "fixed"); !errors.Is(err, transition.ErrContinuationControlDenied) {
		t.Fatalf("replay must be authorized again: %v", err)
	}
	allowed = true
	if err := controls.Apply(ctx, marker, clock, "retry-1", transition.RetryContinuation, "different"); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("changed control semantics must conflict: %v", err)
	}
	inspection, err := controls.Inspect(ctx, marker.ExecutionID)
	if err != nil || inspection.Policy.MaxAttempts != 2 || inspection.Policy.AttemptTimeout != time.Minute ||
		inspection.Attempts != 0 || len(inspection.History) != 2 || len(inspection.Controls) != 1 ||
		!inspection.HasDecision || !inspection.CycleStarted.Equal(now.Add(-time.Second)) {
		t.Fatalf("retry must retain decision, history, and original retry time: %+v, %v", inspection, err)
	}
}

type RestartDriveStore interface {
	DriveStore
	ContinuationMarkers(context.Context) ([]transition.ContinuationMarker[string, string], error)
}

func CheckDriveRestart(t *testing.T, store RestartDriveStore) {
	t.Helper()
	ctx := context.Background()
	markers, err := store.ContinuationMarkers(ctx)
	if err != nil || len(markers) != 1 {
		t.Fatalf("markers after restart: %+v, %v", markers, err)
	}
	marker := markers[0]
	progress, err := store.ReadContinuation(ctx, marker.ExecutionID)
	if err != nil || progress.Attempts != 1 || progress.Phase != transition.ContinuationWaiting ||
		progress.Decision == nil || string(progress.Decision.Event.Data) != "charge" || len(progress.History) != 1 {
		t.Fatalf("drive progress after restart: %+v, %v", progress, err)
	}
	now := time.Date(2026, 1, 1, 0, 1, 1, 0, time.UTC)
	clock := func() time.Time { return now }
	lease, err := store.AcquireContinuation(ctx, marker, "worker-after-restart", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StartContinuation(ctx, lease, clock); err != nil {
		t.Fatalf("resume retry after restart: %v", err)
	}
	if err := store.RecordContinuationDecision(ctx, lease, clock, transition.ContinuationDecision{}); !errors.Is(err, transition.ErrInvalidDecision) {
		t.Fatalf("restart cannot replace prior decision: %v", err)
	}
}

func CheckDrivePauseRestart(t *testing.T, store RestartDriveStore) {
	t.Helper()
	ctx := context.Background()
	markers, err := store.ContinuationMarkers(ctx)
	if err != nil || len(markers) != 1 {
		t.Fatalf("markers after restart: %+v, %v", markers, err)
	}
	marker := markers[0]
	progress, err := store.ReadContinuation(ctx, marker.ExecutionID)
	if err != nil || progress.Attempts != 1 || progress.PausedAt.IsZero() {
		t.Fatalf("pause was not retained: %+v, %v", progress, err)
	}
	now := time.Date(2026, 1, 1, 0, 1, 43, 0, time.UTC)
	clock := func() time.Time { return now }
	lease, err := store.AcquireContinuation(ctx, marker, "worker-b", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ResumeContinuation(ctx, lease, clock); !errors.Is(err, transition.ErrBusy) {
		t.Fatalf("reservation remains held after restart: %v", err)
	}
	competingExecution, err := store.ReadActiveExecution(ctx, "competing")
	if err != nil {
		t.Fatal(err)
	}
	competing := marker
	competing.ExecutionID = competingExecution.ID
	competing.OperationID = competingExecution.CommitOperationID
	if err := store.RecordContinuationMarker(ctx, competing); err != nil {
		t.Fatal(err)
	}
	competingLease, err := store.AcquireTransitionLease(ctx, competing.Entity, competing.ExecutionID, "owner", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCommitReceipt(ctx, competingLease, clock, transition.CommitReceipt{
		ExecutionID: competing.ExecutionID, OperationID: competing.OperationID, Outcome: transition.CommitSucceeded,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.ResumeContinuation(ctx, lease, clock); err != nil {
		t.Fatal(err)
	}
	now = now.Add(6 * time.Second)
	if err := store.RecordContinuationDecision(ctx, lease, clock, transition.ContinuationDecision{}); err != nil {
		t.Fatalf("remaining attempt time after restart: %v", err)
	}
	progress, err = store.ReadContinuation(ctx, marker.ExecutionID)
	if err != nil || progress.Attempts != 1 || progress.Phase != transition.ContinuationStopped {
		t.Fatalf("restart must preserve the drive attempt: %+v, %v", progress, err)
	}
}

func RunDriveWaiver(t *testing.T, store DriveStore) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	marker := transition.ContinuationMarker[string, string]{
		SchemaVersion: 1, ExecutionID: "origin", Entity: "entity", Target: "due",
		MachineID: "plan", MachineVersion: "1",
		Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
	}
	marker = PrepareOrigin(t, store, marker)
	if err := store.RecordContinuationMarker(ctx, marker); err != nil {
		t.Fatal(err)
	}
	origin, err := store.AcquireTransitionLease(ctx, marker.Entity, marker.ExecutionID, "owner", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	control := transition.ContinuationControl{ID: "waive-1", Action: transition.WaiveContinuation, Principal: "operator", Reason: "not needed"}
	if err := store.ApplyContinuationControl(ctx, marker, clock, control); err != nil {
		t.Fatal(err)
	}
	progress, err := store.ReadContinuation(ctx, marker.ExecutionID)
	if err != nil || progress.Phase != transition.ContinuationWaiverPending {
		t.Fatalf("waiver cannot bypass origin receipt and reservation: %+v, %v", progress, err)
	}
	if err := store.RecordCommitReceipt(ctx, origin, clock, transition.CommitReceipt{
		ExecutionID: marker.ExecutionID, OperationID: marker.OperationID, Outcome: transition.CommitSucceeded,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyContinuationControl(ctx, marker, clock, control); err != nil {
		t.Fatal(err)
	}
	progress, err = store.ReadContinuation(ctx, marker.ExecutionID)
	if err != nil || progress.Phase != transition.ContinuationWaived || len(progress.Controls) != 1 {
		t.Fatalf("waiver must finalize after origin release without duplicating audit: %+v, %v", progress, err)
	}
	if _, err := store.AcquireContinuation(ctx, marker, "worker", clock, time.Minute); !errors.Is(err, transition.ErrInvalidDecision) {
		t.Fatalf("waived continuation cannot start: %v", err)
	}
}

func RunDriveWaiverPendingAcceptance(t *testing.T, store DriveStore, outcome string) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	marker := transition.ContinuationMarker[string, string]{
		SchemaVersion: 1, ExecutionID: "origin", Entity: "entity", Target: "due",
		MachineID: "plan", MachineVersion: "1",
		Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
	}
	marker = PrepareOrigin(t, store, marker)
	if err := store.RecordContinuationMarker(ctx, marker); err != nil {
		t.Fatal(err)
	}
	origin, err := store.AcquireTransitionLease(ctx, marker.Entity, marker.ExecutionID, "owner", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCommitReceipt(ctx, origin, clock, transition.CommitReceipt{
		ExecutionID: marker.ExecutionID, OperationID: marker.OperationID, Outcome: transition.CommitSucceeded,
	}); err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireContinuation(ctx, marker, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StartContinuation(ctx, lease, clock); err != nil {
		t.Fatal(err)
	}
	decision := transition.ContinuationDecision{
		Continue: true,
		Event:    transition.EncodedValue{Codec: transition.CodecVersion{Name: "event", Version: "1"}, Data: []byte("pay")},
		Input:    transition.EncodedValue{Codec: transition.CodecVersion{Name: "input", Version: "1"}, Data: []byte("payload")},
	}
	if err := store.RecordContinuationDecision(ctx, lease, clock, decision); err != nil {
		t.Fatal(err)
	}
	binding := transition.SuccessorBinding[string, string]{
		Origin: marker, OriginRevision: []byte("revision-1"),
		Execution: transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "successor", Entity: marker.Entity,
			Key: "successor", Operation: transition.Transition, Previous: marker.Target, HasPrevious: true,
			Target: "paid", MachineID: "plan", MachineVersion: "1", Status: transition.Waiting,
			Policy: marker.Policy, AcceptedAt: now, Deadline: now.Add(time.Hour),
			Event: decision.Event, Input: decision.Input, NotificationOverride: transition.NotifyInherit,
		},
	}
	prepared, created, err := store.PrepareContinuationSuccessor(ctx, lease, clock, func(*transition.SuccessorBinding[string, string]) (*transition.SuccessorBinding[string, string], error) {
		return &binding, nil
	})
	if err != nil || !created {
		t.Fatalf("prepare: created=%v error=%v", created, err)
	}
	if outcome == "retry-denied" {
		if err := store.RecordSuccessorDenial(ctx, prepared, "worker"); err != nil {
			t.Fatal(err)
		}
		if err := store.ReconcileContinuation(ctx, marker, clock); err != nil {
			t.Fatal(err)
		}
		progress, err := store.ReadContinuation(ctx, marker.ExecutionID)
		if err != nil || progress.Phase != transition.ContinuationIntervention {
			t.Fatalf("definitive denial must require intervention: %+v, %v", progress, err)
		}
		retry := transition.ContinuationControl{ID: "retry-denial", Action: transition.RetryContinuation, Principal: "operator", Reason: "access restored"}
		if err := store.ApplyContinuationControl(ctx, marker, clock, retry); err != nil {
			t.Fatal(err)
		}
		inspection, err := store.InspectSuccessor(ctx, marker)
		if err != nil || inspection.Phase != transition.SuccessorPrepared || inspection.Binding.Execution.ID != prepared.Execution.ID || len(inspection.Denials) != 1 {
			t.Fatalf("retry must retain identity and denial history: %+v, %v", inspection, err)
		}
		if err := store.RecordSuccessorDenial(ctx, prepared, "worker"); err != nil {
			t.Fatal(err)
		}
		inspection, err = store.InspectSuccessor(ctx, marker)
		if err != nil || inspection.Phase != transition.SuccessorDenied {
			t.Fatalf("repeated denial must block the new cycle: %+v, %v", inspection, err)
		}
		return
	}
	control := transition.ContinuationControl{ID: "waive-1", Action: transition.WaiveContinuation, Principal: "operator", Reason: "not needed"}
	if err := store.ApplyContinuationControl(ctx, marker, clock, control); err != nil {
		t.Fatal(err)
	}
	progress, err := store.ReadContinuation(ctx, marker.ExecutionID)
	if err != nil || progress.Phase != transition.ContinuationWaiverPending {
		t.Fatalf("unknown acceptance must keep waiver pending: %+v, %v", progress, err)
	}
	if _, _, err := store.PrepareContinuationSuccessor(ctx, lease, clock, func(*transition.SuccessorBinding[string, string]) (*transition.SuccessorBinding[string, string], error) {
		t.Fatal("waiver must fence old evaluator")
		return nil, nil
	}); !errors.Is(err, transition.ErrStaleLease) {
		t.Fatalf("waiver did not fence evaluator: %v", err)
	}
	if outcome == "accepted" {
		if _, err := store.FinalizeSuccessor(ctx, transition.SuccessorReceipt[string, string]{
			Binding: prepared, Principal: "worker", Decision: "allowed", AcceptedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
		if err := store.ReconcileContinuation(ctx, marker, clock); err != nil {
			t.Fatal(err)
		}
		if err := store.ApplyContinuationControl(ctx, marker, clock, control); err != nil {
			t.Fatal(err)
		}
		progress, err = store.ReadContinuation(ctx, marker.ExecutionID)
		if err != nil || progress.Phase != transition.ContinuationAccepted {
			t.Fatalf("accepted successor must win over waiver: %+v, %v", progress, err)
		}
		return
	}
	if err := store.CancelSuccessor(ctx, transition.SuccessorCancellation[string, string]{
		Binding: prepared, Evidence: []byte("verified-host-tombstone"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyContinuationControl(ctx, marker, clock, control); err != nil {
		t.Fatal(err)
	}
	progress, err = store.ReadContinuation(ctx, marker.ExecutionID)
	if err != nil || progress.Phase != transition.ContinuationWaived || len(progress.Controls) != 1 {
		t.Fatalf("waiver requires authoritative cancellation: %+v, %v", progress, err)
	}
}

func RunDriveDeadline(t *testing.T, store DriveStore) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	marker := transition.ContinuationMarker[string, string]{
		SchemaVersion: 1, ExecutionID: "origin", Entity: "entity", Target: "due",
		MachineID: "plan", MachineVersion: "1",
		Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: 10 * time.Second},
	}

	marker = PrepareOrigin(t, store, marker)
	if err := store.RecordContinuationMarker(ctx, marker); err != nil {
		t.Fatal(err)
	}
	origin, err := store.AcquireTransitionLease(ctx, marker.Entity, marker.ExecutionID, "owner", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCommitReceipt(ctx, origin, clock, transition.CommitReceipt{
		ExecutionID: marker.ExecutionID, OperationID: marker.OperationID, Outcome: transition.CommitSucceeded,
	}); err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireContinuation(ctx, marker, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetContinuationCompatibility(ctx, lease, clock, true); err != nil {
		t.Fatal(err)
	}
	progress, err := store.ReadContinuation(ctx, marker.ExecutionID)
	if err != nil || !progress.CompatibilityBlocked || progress.Attempts != 0 {
		t.Fatalf("missing behavior must block without starting an attempt: %+v, %v", progress, err)
	}
	now = now.Add(10 * time.Second)
	if err := store.SetContinuationCompatibility(ctx, lease, clock, false); !errors.Is(err, transition.ErrContinuationIntervention) {
		t.Fatalf("restoring behavior after the active deadline must require intervention: %v", err)
	}
	progress, err = store.ReadContinuation(ctx, marker.ExecutionID)
	if err != nil || progress.Attempts != 0 || progress.Phase != transition.ContinuationIntervention {
		t.Fatalf("expired budget must be durable without charging an attempt: %+v, %v", progress, err)
	}
}

func RunDriveQueuedDeadline(t *testing.T, store DriveStore) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	marker := transition.ContinuationMarker[string, string]{
		SchemaVersion: 1, ExecutionID: "origin", Entity: "entity", Target: "due",
		MachineID: "plan", MachineVersion: "1",
		Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: 10 * time.Second},
	}
	marker = PrepareOrigin(t, store, marker)
	if err := store.RecordContinuationMarker(ctx, marker); err != nil {
		t.Fatal(err)
	}
	origin, err := store.AcquireTransitionLease(ctx, marker.Entity, marker.ExecutionID, "owner", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCommitReceipt(ctx, origin, clock, transition.CommitReceipt{
		ExecutionID: marker.ExecutionID, OperationID: marker.OperationID, Outcome: transition.CommitSucceeded,
	}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(10 * time.Second)
	lease, err := store.AcquireContinuation(ctx, marker, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StartContinuation(ctx, lease, clock); !errors.Is(err, transition.ErrContinuationIntervention) {
		t.Fatalf("time queued after origin release must consume the deadline: %v", err)
	}
}

func CheckDriveStartedRestart(t *testing.T, store RestartDriveStore) {
	t.Helper()
	ctx := context.Background()
	markers, err := store.ContinuationMarkers(ctx)
	if err != nil || len(markers) != 1 {
		t.Fatalf("markers after restart: %+v, %v", markers, err)
	}
	marker := markers[0]
	now := time.Date(2026, 1, 1, 0, 0, 2, 0, time.UTC)
	clock := func() time.Time { return now }
	lease, err := store.AcquireContinuation(ctx, marker, "worker-after-restart", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecoverContinuationAttempt(ctx, lease, clock); err != nil {
		t.Fatal(err)
	}
	progress, err := store.ReadContinuation(ctx, marker.ExecutionID)
	if err != nil || progress.Attempts != 1 || progress.Phase != transition.ContinuationIntervention || len(progress.History) != 1 {
		t.Fatalf("lost started attempt must remain charged after restart: %+v, %v", progress, err)
	}
}

type driveStringCodec struct{}

func (driveStringCodec) Encode(_ context.Context, value string) ([]byte, error) {
	return []byte(value), nil
}
func (driveStringCodec) Decode(_ context.Context, value []byte) (string, error) {
	return string(value), nil
}
func (driveStringCodec) Equal(_ context.Context, stored []byte, incoming string) (bool, error) {
	return string(stored) == incoming, nil
}

func RunDriverEvaluation(t *testing.T, store DriveStore) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	marker := transition.ContinuationMarker[string, string]{
		SchemaVersion: 1, ExecutionID: "origin", Entity: "entity", Target: "due",
		MachineID: "plan", MachineVersion: "1",
		Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
	}
	marker = PrepareOrigin(t, store, marker)
	if err := store.RecordContinuationMarker(ctx, marker); err != nil {
		t.Fatal(err)
	}
	origin, err := store.AcquireTransitionLease(ctx, marker.Entity, marker.ExecutionID, "owner", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCommitReceipt(ctx, origin, clock, transition.CommitReceipt{
		ExecutionID: marker.ExecutionID, OperationID: marker.OperationID, Outcome: transition.CommitSucceeded,
	}); err != nil {
		t.Fatal(err)
	}
	var registry fsm.Registry[string, string, string, string]
	codec := func(transition.CodecVersion) (transition.Codec[string], error) { return driveStringCodec{}, nil }
	driver, err := transition.NewContinuationDriver(store, &registry, transition.ContinuationDriverConfig[string, string, string, string]{
		Clock: clock, LeaseDuration: time.Minute,
		EventCodec: transition.CodecVersion{Name: "event", Version: "1"}, InputCodec: transition.CodecVersion{Name: "input", Version: "1"},
		ResolveEvent: codec, ResolveInput: codec,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := driver.Drive(ctx, marker, "worker"); !errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("unavailable pinned version must block: %v", err)
	}
	progress, err := store.ReadContinuation(ctx, marker.ExecutionID)
	if err != nil || !progress.CompatibilityBlocked || progress.Attempts != 0 {
		t.Fatalf("compatibility block must be durable and uncharged: %+v, %v", progress, err)
	}
	calls := 0
	machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
		ID: "plan", Version: "1", InitialStates: []string{"due"},
		Handlers:             map[string]fsm.Handler[string, string, string, string]{"due": fsm.NoopHandler[string, string, string, string]{}},
		ContinuationPolicies: map[string]fsm.Policy{"due": marker.Policy},
		SelectInitial:        func(string) (fsm.Selection[string], error) { return fsm.Selection[string]{Target: "due"}, nil },
		SelectTransition: func(string, string, string) (fsm.Selection[string], error) {
			return fsm.Selection[string]{Target: "due"}, nil
		},
		Continue: func(_ context.Context, entity, state string) (fsm.ContinuationDecision[string, string], error) {
			calls++
			if entity != "entity" || state != "due" {
				t.Fatalf("wrong continuation inputs: %q, %q", entity, state)
			}
			return fsm.ContinuationDecision[string, string]{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(machine); err != nil {
		t.Fatal(err)
	}
	if err := driver.Drive(ctx, marker, "worker"); err != nil {
		t.Fatal(err)
	}
	progress, err = store.ReadContinuation(ctx, marker.ExecutionID)
	if err != nil || calls != 1 || progress.Attempts != 1 || progress.Phase != transition.ContinuationStopped || progress.CompatibilityBlocked {
		t.Fatalf("pinned no-continuation behavior must be recorded once: calls=%d progress=%+v err=%v", calls, progress, err)
	}
}
