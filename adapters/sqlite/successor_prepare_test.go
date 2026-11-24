package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/fsm"
	"github.com/reinaldoborges/rff-go/internal/continuationcontract"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestSuccessorPrepareSurvivesReopenAndReservesEntity(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "successor.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("acceptor semantic replay", func(t *testing.T) {
		ctx := context.Background()
		store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "successor.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		policy := fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Second, OverallTimeout: time.Minute}
		marker := transition.ContinuationMarker[string, string]{
			SchemaVersion: 1, ExecutionID: "origin", Entity: "entity", Target: "due",
			MachineID: "plan", MachineVersion: "1", Policy: policy,
		}
		marker = continuationcontract.PrepareOrigin(t, store, marker)
		if err := store.RecordContinuationMarker(ctx, marker); err != nil {
			t.Fatal(err)
		}
		now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		clock := func() time.Time { return now }
		lease, err := store.AcquireTransitionLease(ctx, marker.Entity, marker.ExecutionID, "owner", clock, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.RecordCommitReceipt(ctx, lease, clock, transition.CommitReceipt{
			ExecutionID: marker.ExecutionID, OperationID: marker.OperationID, Outcome: transition.CommitSucceeded,
		}); err != nil {
			t.Fatal(err)
		}
		selections := 0
		machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
			ID: "plan", Version: "1", InitialStates: []string{"due"},
			Handlers: map[string]fsm.Handler[string, string, string, string]{
				"due":  fsm.NoopHandler[string, string, string, string]{},
				"paid": fsm.NoopHandler[string, string, string, string]{},
			},
			SelectInitial: func(string) (fsm.Selection[string], error) {
				return fsm.Selection[string]{Target: "due", Policy: policy}, nil
			},
			SelectTransition: func(string, string, string) (fsm.Selection[string], error) {
				selections++
				return fsm.Selection[string]{Target: "paid", Policy: policy}, nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		var registry fsm.Registry[string, string, string, string]
		if err := registry.Register(machine); err != nil {
			t.Fatal(err)
		}
		codec := func(transition.CodecVersion) (transition.Codec[string], error) { return stringValueCodec{}, nil }
		acceptor, err := transition.NewAcceptor(store, &registry, codec, codec, func() (string, error) { return "successor", nil }, clock)
		if err != nil {
			t.Fatal(err)
		}
		request := transition.SuccessorRequest[string, string, string, string]{
			Origin: marker, OriginRevision: []byte("revision-1"),
			Request: transition.Request[string, string, string, string]{
				Entity: marker.Entity, Key: "next", Operation: transition.Transition, Previous: marker.Target,
				HasPrevious: true, MachineID: "plan", MachineVersion: "1", Event: "pay", HasEvent: true,
				EventCodec: transition.CodecVersion{Name: "event", Version: "1"}, Input: "payload",
				InputCodec: transition.CodecVersion{Name: "input", Version: "1"}, Notification: transition.NotifyInherit,
			},
		}
		decision, err := acceptor.SuccessorDecision(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		first, created, err := store.PrepareSuccessor(ctx, request.Origin, decision)
		if err != nil || !created || first.Execution.ID != "successor" {
			t.Fatalf("prepare: %+v created=%v error=%v", first, created, err)
		}
		replayed, created, err := acceptor.PrepareSuccessor(ctx, request)
		if err != nil || created || replayed.Execution.ID != first.Execution.ID || selections != 1 {
			t.Fatalf("replay: %+v created=%v selections=%d error=%v", replayed, created, selections, err)
		}
		request.Request.Event = "different"
		if _, _, err := acceptor.PrepareSuccessor(ctx, request); !errors.Is(err, transition.ErrConflict) {
			t.Fatalf("changed event did not conflict: %v", err)
		}
		cancel := transition.SuccessorCancellation[string, string]{Binding: first, Evidence: []byte("durable-host-tombstone"), Superseded: true}
		if err := store.CancelSuccessor(ctx, cancel); err != nil {
			t.Fatalf("fenced cancellation: %v", err)
		}
		if err := store.CancelSuccessor(ctx, cancel); err != nil {
			t.Fatalf("cancellation replay: %v", err)
		}
		inspection, err := store.InspectSuccessor(ctx, marker)
		if err != nil || inspection.Phase != transition.SuccessorSuperseded {
			t.Fatalf("supersession must be retained: %+v error=%v", inspection, err)
		}
		_, created, err = store.Accept(ctx, marker.Entity, "competitor", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			return &transition.Execution[string, string]{
				SchemaVersion: transition.CurrentSchemaVersion, ID: "competitor", Entity: marker.Entity,
				Key: "competitor", Status: transition.Waiting,
			}, nil
		})
		if err != nil || !created {
			t.Fatalf("fenced cancellation did not release runtime reservation: %v", err)
		}
	})
	marker := transition.ContinuationMarker[string, string]{
		SchemaVersion: 1, ExecutionID: "origin", Entity: "entity", Target: "due",
		MachineID: "plan", MachineVersion: "1",
		Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Second, OverallTimeout: time.Minute},
	}
	marker = continuationcontract.PrepareOrigin(t, store, marker)
	if err := store.RecordContinuationMarker(ctx, marker); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	lease, err := store.AcquireTransitionLease(ctx, marker.Entity, marker.ExecutionID, "owner", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCommitReceipt(ctx, lease, clock, transition.CommitReceipt{
		ExecutionID: marker.ExecutionID, OperationID: marker.OperationID, Outcome: transition.CommitSucceeded,
	}); err != nil {
		t.Fatal(err)
	}
	binding := transition.SuccessorBinding[string, string]{
		Origin: marker, OriginRevision: []byte("revision-1"),
		Execution: transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "successor", Entity: marker.Entity,
			Key: "successor-key", Operation: transition.Transition, Previous: marker.Target, HasPrevious: true,
			Target: "paid", MachineID: marker.MachineID, MachineVersion: marker.MachineVersion,
			Status: transition.Waiting, Policy: marker.Policy, AcceptedAt: now, Deadline: now.Add(time.Minute),
			Event:                transition.EncodedValue{Codec: transition.CodecVersion{Name: "event", Version: "1"}, Data: []byte("pay")},
			Input:                transition.EncodedValue{Codec: transition.CodecVersion{Name: "input", Version: "1"}, Data: []byte("payload")},
			NotificationOverride: transition.NotifyInherit,
		},
	}
	invalid := binding
	invalid.Execution.Event.Codec = transition.CodecVersion{}
	if _, _, err := store.PrepareSuccessor(ctx, marker, func(*transition.SuccessorBinding[string, string]) (*transition.SuccessorBinding[string, string], error) {
		return &invalid, nil
	}); !errors.Is(err, transition.ErrInvalidDecision) {
		t.Fatalf("unverifiable prepared event must fail closed: %v", err)
	}
	decide := func(existing *transition.SuccessorBinding[string, string]) (*transition.SuccessorBinding[string, string], error) {
		if existing != nil {
			return nil, nil
		}
		return &binding, nil
	}
	prepared, created, err := store.PrepareSuccessor(ctx, marker, decide)
	if err != nil || !created || prepared.Execution.ID != "successor" {
		t.Fatalf("prepare: %+v created=%v error=%v", prepared, created, err)
	}
	_, _, err = store.Accept(ctx, marker.Entity, "competitor", func(view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		if !view.Active {
			t.Fatal("prepared successor did not reserve the entity")
		}
		return nil, transition.ErrBusy
	})
	if !errors.Is(err, transition.ErrBusy) {
		t.Fatalf("competitor must wait: %v", err)
	}
	_, _, err = store.Accept(ctx, "another", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "successor", Entity: "another",
			Key: "key", Status: transition.Waiting,
		}, nil
	})
	if !errors.Is(err, transition.ErrInvalidDecision) {
		t.Fatalf("prepared execution ID must remain reserved: %v", err)
	}
	if err := store.RecordSuccessorDenial(ctx, prepared, "worker"); err != nil {
		t.Fatalf("record denial: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	inspection, err := store.InspectSuccessor(ctx, marker)
	if err != nil || inspection.Phase != transition.SuccessorDenied || inspection.Binding.Execution.ID != prepared.Execution.ID {
		t.Fatalf("recovery could not inspect durable prepare: %+v error=%v", inspection, err)
	}
	replayed, created, err := store.PrepareSuccessor(ctx, marker, decide)
	if err != nil || created || replayed.Execution.ID != prepared.Execution.ID {
		t.Fatalf("reopened prepare: %+v created=%v error=%v", replayed, created, err)
	}
	receipt := transition.SuccessorReceipt[string, string]{
		Binding: replayed, Principal: "worker", Decision: "allowed", AcceptedAt: now.Add(5 * time.Minute),
	}
	accepted, err := store.FinalizeSuccessor(ctx, receipt)
	if err != nil || accepted.ID != prepared.Execution.ID || accepted.Status != transition.Waiting ||
		!accepted.AcceptedAt.Equal(receipt.AcceptedAt) || !accepted.Deadline.Equal(receipt.AcceptedAt.Add(time.Minute)) {
		t.Fatalf("finalize: %+v error=%v", accepted, err)
	}
	accepted, err = store.FinalizeSuccessor(ctx, receipt)
	if err != nil || accepted.ID != prepared.Execution.ID {
		t.Fatalf("finalize replay: %+v error=%v", accepted, err)
	}
	var registry fsm.Registry[string, string, string, string]
	codec := func(transition.CodecVersion) (transition.Codec[string], error) { return stringValueCodec{}, nil }
	acceptor, err := transition.NewAcceptor(store, &registry, codec, codec, func() (string, error) { return "unused", nil }, clock)
	if err != nil {
		t.Fatal(err)
	}
	request := transition.SuccessorRequest[string, string, string, string]{
		Origin: marker, OriginRevision: []byte("revision-1"),
		Request: transition.Request[string, string, string, string]{
			Entity: marker.Entity, Key: "successor-key", Operation: transition.Transition, Previous: marker.Target,
			HasPrevious: true, MachineID: marker.MachineID, MachineVersion: marker.MachineVersion,
			Event: "pay", HasEvent: true, EventCodec: transition.CodecVersion{Name: "event", Version: "1"},
			Input: "payload", InputCodec: transition.CodecVersion{Name: "input", Version: "1"}, Notification: transition.NotifyInherit,
		},
	}
	disclosed, err := acceptor.ReplaySuccessor(ctx, request, func(context.Context, transition.SuccessorRequest[string, string, string, string]) error {
		return errors.New("private authorization details")
	})
	if !errors.Is(err, transition.ErrSuccessorReplayDenied) || disclosed.ID != "" {
		t.Fatalf("unauthorized replay disclosed execution: %+v error=%v", disclosed, err)
	}
	disclosed, err = acceptor.ReplaySuccessor(ctx, request, func(context.Context, transition.SuccessorRequest[string, string, string, string]) error { return nil })
	if err != nil || disclosed.ID != accepted.ID || !disclosed.AcceptedAt.Equal(receipt.AcceptedAt) {
		t.Fatalf("authorized replay: %+v error=%v", disclosed, err)
	}
	private, err := acceptor.InspectSuccessor(ctx, marker, func(context.Context, transition.ContinuationMarker[string, string]) error {
		return errors.New("private authorization details")
	})
	if !errors.Is(err, transition.ErrSuccessorReplayDenied) || private.Binding.Execution.ID != "" {
		t.Fatalf("unauthorized inspection disclosed data: %+v error=%v", private, err)
	}
	private, err = acceptor.InspectSuccessor(ctx, marker, func(context.Context, transition.ContinuationMarker[string, string]) error { return nil })
	if err != nil || private.Phase != transition.SuccessorAccepted || private.Binding.Execution.ID != accepted.ID {
		t.Fatalf("authorized inspection: %+v error=%v", private, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE rff_successors SET phase = 'unknown'`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{}); !errors.Is(err, sqlite.ErrUnsupportedSchema) {
		t.Fatalf("unverifiable successor phase must fail closed: %v", err)
	}
}
