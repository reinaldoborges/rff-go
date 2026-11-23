package mysql_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/mysql"
	"github.com/reinaldoborges/rff-go/fsm"
	"github.com/reinaldoborges/rff-go/internal/continuationcontract"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestSuccessorPrepareSurvivesReopenAndReservesEntity(t *testing.T) {
	ctx := context.Background()
	_, config := newMySQLDatabase(t)
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}

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
	if err := store.RecordSuccessorDenial(ctx, prepared, "worker"); err != nil {
		t.Fatalf("record denial: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
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
	receipt := transition.SuccessorReceipt[string, string]{Binding: replayed, Principal: "worker", Decision: "allowed", AcceptedAt: now.Add(5 * time.Minute)}
	accepted, err := store.FinalizeSuccessor(ctx, receipt)
	if err != nil || accepted.ID != prepared.Execution.ID ||
		!accepted.AcceptedAt.Equal(receipt.AcceptedAt) || !accepted.Deadline.Equal(receipt.AcceptedAt.Add(time.Minute)) {
		t.Fatalf("finalize: %+v error=%v", accepted, err)
	}
	accepted, err = store.FinalizeSuccessor(ctx, receipt)
	if err != nil || accepted.ID != prepared.Execution.ID {
		t.Fatalf("finalize replay: %+v error=%v", accepted, err)
	}
}
