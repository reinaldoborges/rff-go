// Package durableidentitytest exercises the public adapter identity contract.
package durableidentitytest

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

type Store interface {
	Accept(context.Context, string, string, transition.AcceptanceDecisionFunc[string, string]) (transition.Execution[string, string], bool, error)
	AcceptInput(context.Context, string, func(*transition.InputRecord) (*transition.InputRecord, error)) (transition.InputRecord, bool, error)
	PendingInputIDs(context.Context, string) ([]string, error)
	ReadInput(context.Context, string) (transition.InputRecord, error)
	ScheduleTimer(context.Context, string, func(*transition.TimerRecord) (*transition.TimerRecord, error)) (transition.TimerRecord, bool, error)
	ClaimDueTimer(context.Context, string, func() time.Time, time.Duration) (transition.TimerRecord, transition.TimerLease, error)
	CompleteTimer(context.Context, transition.TimerLease, func() time.Time) error
}

func Check(t *testing.T, store Store) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	ids := []string{"a", "A", "a\x00", "a\x00x", "a ", "é", "e\u0301", strings.Repeat("x", 255) + "a", strings.Repeat("x", 255) + "b", strings.Repeat("é", 128)}
	for _, id := range ids {
		execution, created, err := store.Accept(ctx, id, "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			return &transition.Execution[string, string]{SchemaVersion: transition.CurrentSchemaVersion, ID: id, Entity: id, Key: "key", Status: transition.Waiting}, nil
		})
		if err != nil || !created || execution.ID != id {
			t.Fatalf("execution %q: %#v created=%v err=%v", id, execution.ID, created, err)
		}
		input, created, err := store.AcceptInput(ctx, id, func(*transition.InputRecord) (*transition.InputRecord, error) {
			return &transition.InputRecord{SchemaVersion: 1, ID: id, Channel: "channel", WorkflowID: "workflow", Kind: transition.Command,
				Value: transition.EncodedValue{Codec: transition.CodecVersion{Name: "input", Version: "1"}}}, nil
		})
		if err != nil || !created || input.ID != id {
			t.Fatalf("input %q: %#v created=%v err=%v", id, input.ID, created, err)
		}
		input, err = store.ReadInput(ctx, id)
		if err != nil || input.ID != id {
			t.Fatalf("read input %q: %#v err=%v", id, input.ID, err)
		}
		timer, created, err := store.ScheduleTimer(ctx, id, func(*transition.TimerRecord) (*transition.TimerRecord, error) {
			return &transition.TimerRecord{SchemaVersion: transition.CurrentTimerSchemaVersion,
				TimerReceipt: transition.TimerReceipt{ID: id, WorkflowID: "workflow", Deadline: now},
				Event:        transition.EncodedValue{Codec: transition.CodecVersion{Name: "event", Version: "1"}}}, nil
		})
		if err != nil || !created || timer.ID != id {
			t.Fatalf("timer %q: %#v created=%v err=%v", id, timer.ID, created, err)
		}
	}
	want := append([]string(nil), ids...)
	sort.Strings(want)
	got, err := store.PendingInputIDs(ctx, "channel")
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("pending input order: got=%q want=%q err=%v", got, want, err)
	}
	for _, id := range want {
		timer, lease, err := store.ClaimDueTimer(ctx, "worker", clock, time.Minute)
		if err != nil || timer.ID != id || lease.ID != id {
			t.Fatalf("timer order: got=%q want=%q err=%v", timer.ID, id, err)
		}
		if err := store.CompleteTimer(ctx, lease, clock); err != nil {
			t.Fatal(err)
		}
	}
}
