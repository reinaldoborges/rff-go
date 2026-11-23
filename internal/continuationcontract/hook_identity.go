package continuationcontract

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func RunHookIdentityCompatibility(t *testing.T, store Store, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	marker := transition.ContinuationMarker[string, string]{
		SchemaVersion: 1, ExecutionID: "legacy", Entity: "entity", Target: "due",
		MachineID: "plan", MachineVersion: "1",
		Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
	}
	PrepareOrigin(t, store, marker)
	var data []byte
	if err := db.QueryRowContext(ctx, `SELECT work FROM rff_hook_work WHERE execution_id = ?`, marker.ExecutionID).Scan(&data); err != nil {
		t.Fatal(err)
	}
	var record map[string]json.RawMessage
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	delete(record, "AttemptExecutionIDs")
	save := func() {
		t.Helper()
		data, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `UPDATE rff_hook_work SET work = ? WHERE execution_id = ?`, data, marker.ExecutionID); err != nil {
			t.Fatal(err)
		}
	}
	save()
	progress, err := store.ReadHookProgress(ctx, marker.ExecutionID)
	if err != nil || len(progress.Attempts) != 1 || progress.Attempts[0].Metadata.ExecutionID != marker.ExecutionID {
		t.Fatalf("legacy attempt history became unreadable: %+v %v", progress, err)
	}
	record["AttemptExecutionIDs"] = json.RawMessage(`["dGFtcGVyZWQ="]`)
	save()
	if _, err := store.ReadHookProgress(ctx, marker.ExecutionID); err == nil {
		t.Fatal("inconsistent attempt identity was silently accepted")
	}
}
