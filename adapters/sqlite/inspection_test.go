package sqlite_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestInspectionIncludesTerminalAndUnreadableExecutionsWithoutInputs(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "inspection.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	now := time.Now().UTC()
	for _, item := range []struct{ id, target string }{{"done", "compatible"}, {"waiting", "incompatible"}} {
		_, _, err := store.Accept(ctx, item.id, "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			return &transition.Execution[string, string]{
				SchemaVersion: transition.CurrentSchemaVersion, ID: item.id, Entity: item.id, Key: "key",
				Operation: transition.Initialize, MachineID: "task", MachineVersion: "1",
				Target: item.target, Input: transition.EncodedValue{Codec: transition.CodecVersion{Name: "string", Version: "1"}, Data: []byte("secret-input")},
				Status: transition.Waiting, AcceptedAt: now, Deadline: now.Add(time.Hour),
				Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
			}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	clock := fixedClock(now)
	fence, err := store.AcquireTransitionLease(ctx, "done", "done", "coordinator", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, fence, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
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
	operation, err := store.MarkWorkSuccess(ctx, fence, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCommitReceipt(ctx, fence, clock, transition.CommitReceipt{
		ExecutionID: "done", OperationID: operation, Outcome: transition.CommitSucceeded,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, selectiveStateCodec{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.BlockUnreadableExecution(ctx, "waiting"); err != nil {
		t.Fatal(err)
	}
	ids, err := store.ListExecutionIDs(ctx)
	if err != nil || len(ids) != 2 || ids[0] != "done" || ids[1] != "waiting" {
		t.Fatalf("inspection list omitted retained execution: ids=%v error=%v", ids, err)
	}
	for _, item := range []struct {
		id     string
		status transition.Status
	}{{"done", transition.Complete}, {"waiting", transition.Waiting}} {
		inspection, err := store.InspectExecution(ctx, item.id)
		if err != nil || inspection.ID != item.id || inspection.Status != item.status {
			t.Fatalf("inspection failed: inspection=%+v error=%v", inspection, err)
		}
		if item.id == "waiting" && (inspection.Compatibility == nil || inspection.Compatibility.Component != transition.CompatibilityState) {
			t.Fatalf("inspection omitted compatibility diagnostic: %+v", inspection)
		}
		if item.id == "done" && (inspection.Hook == nil || len(inspection.Hook.Attempts) != 1) {
			t.Fatalf("inspection omitted attempt history: %+v", inspection)
		}
		data, err := json.Marshal(inspection)
		if err != nil || strings.Contains(string(data), "secret-input") ||
			strings.Contains(string(data), base64.StdEncoding.EncodeToString([]byte("secret-input"))) {
			t.Fatalf("inspection exposed encoded input: inspection=%s error=%v", data, err)
		}
	}
	if _, err := store.InspectExecution(ctx, "missing"); !errors.Is(err, transition.ErrExecutionNotActive) {
		t.Fatalf("missing execution was not reported: %v", err)
	}
}
