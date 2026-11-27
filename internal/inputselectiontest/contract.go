package inputselectiontest

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

type Store interface {
	AcceptInput(context.Context, string, func(*transition.InputRecord) (*transition.InputRecord, error)) (transition.InputRecord, bool, error)
	PendingInputIDs(context.Context, string) ([]string, error)
	ReadInput(context.Context, string) (transition.InputRecord, error)
	CompleteInput(context.Context, string, transition.InputOutcome) (transition.InputRecord, error)
}

type Maintenance interface {
	Findings(context.Context, string, string) ([]transition.IntegrityFinding, error)
	Resolve(context.Context, string, string, transition.IntegrityFinding) error
	Audit(context.Context, string, string, int64) ([]transition.IntegrityAudit, error)
	Close() error
}

type OpenMaintenance func(transition.IntegrityAuthorizer) (Maintenance, error)

type stringCodec struct{}

func (stringCodec) Encode(_ context.Context, value string) ([]byte, error) { return []byte(value), nil }
func (stringCodec) Decode(_ context.Context, data []byte) (string, error)  { return string(data), nil }
func (stringCodec) Equal(_ context.Context, stored []byte, incoming string) (bool, error) {
	return string(stored) == incoming, nil
}

func seedInput(t *testing.T, store Store, id, channel string) {
	t.Helper()
	if _, created, err := store.AcceptInput(context.Background(), id, func(*transition.InputRecord) (*transition.InputRecord, error) {
		return &transition.InputRecord{
			SchemaVersion: 1,
			ID:            id,
			Channel:       channel,
			WorkflowID:    "workflow",
			Kind:          transition.Command,
			Value: transition.EncodedValue{
				Codec: transition.CodecVersion{Name: "input", Version: "1"},
				Data:  []byte("value"),
			},
		}, nil
	}); err != nil || !created {
		t.Fatalf("seed input %q: created=%v error=%v", id, created, err)
	}
}

func CheckPendingInputSelection(t *testing.T, store Store) {
	t.Helper()
	ctx := context.Background()
	ids := []string{"a", "A", "a\x00", "a ", "aa", strings.Repeat("z", 256)}
	for _, id := range ids {
		seedInput(t, store, id, "Tasks")
	}
	for i := 0; i < 300; i++ {
		id := "bulk-" + strings.Repeat("x", 3) + "-" + string(rune('a'+i/26)) + string(rune('a'+i%26))
		ids = append(ids, id)
		seedInput(t, store, id, "Tasks")
	}
	seedInput(t, store, "case-sensitive", "tasks")
	seedInput(t, store, "other-channel", "Tasks-suffix")
	seedInput(t, store, "completed", "Tasks")
	if _, err := store.CompleteInput(ctx, "completed", transition.InputOutcome{Receipt: "receipt"}); err != nil {
		t.Fatalf("complete input: %v", err)
	}
	got, err := store.PendingInputIDs(ctx, "Tasks")
	if err != nil {
		t.Fatalf("list exact channel: %v", err)
	}
	sort.Strings(ids)
	if !reflect.DeepEqual(got, ids) {
		t.Fatalf("pending input ordering/completeness: got %d IDs, want %d; first got=%q want=%q", len(got), len(ids), got[:min(3, len(got))], ids[:min(3, len(ids))])
	}
	got, err = store.PendingInputIDs(ctx, "tasks")
	if err != nil || !reflect.DeepEqual(got, []string{"case-sensitive"}) {
		t.Fatalf("case-sensitive channel: got=%q error=%v", got, err)
	}
	got, err = store.PendingInputIDs(ctx, "no-matches")
	if err != nil || len(got) != 0 {
		t.Fatalf("no-match channel: got=%q error=%v", got, err)
	}

	prefix := strings.Repeat("p", 256)
	seedInput(t, store, "prefix-a", prefix+"A")
	seedInput(t, store, "prefix-b", prefix+"B")
	got, err = store.PendingInputIDs(ctx, prefix+"A")
	if err != nil || !reflect.DeepEqual(got, []string{"prefix-a"}) {
		t.Fatalf("full channel prefix collision: got=%q error=%v", got, err)
	}

	transient := errors.New("host outcome unknown")
	ingress, err := transition.NewInputIngress[string](store, "submitted", func(transition.CodecVersion) (transition.Codec[string], error) {
		return stringCodec{}, nil
	}, func(context.Context, transition.InputDelivery[string]) (transition.InputOutcome, error) {
		return transition.InputOutcome{}, transient
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = ingress.Submit(ctx, transition.InputRequest[string]{
		ID: "submitted-id", WorkflowID: "workflow", Kind: transition.Command, Value: "value",
		Codec: transition.CodecVersion{Name: "input", Version: "1"},
	})
	if !errors.Is(err, transient) {
		t.Fatalf("submit pending input: %v", err)
	}
	got, err = store.PendingInputIDs(ctx, "submitted")
	if err != nil || !reflect.DeepEqual(got, []string{"submitted-id"}) {
		t.Fatalf("Submit did not project a pending input: got=%q error=%v", got, err)
	}
	completedIngress, err := transition.NewInputIngress[string](store, "submitted-complete", func(transition.CodecVersion) (transition.Codec[string], error) {
		return stringCodec{}, nil
	}, func(context.Context, transition.InputDelivery[string]) (transition.InputOutcome, error) {
		return transition.InputOutcome{Receipt: "receipt"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := completedIngress.Submit(ctx, transition.InputRequest[string]{
		ID: "submitted-complete-id", WorkflowID: "workflow", Kind: transition.Command, Value: "value",
		Codec: transition.CodecVersion{Name: "input", Version: "1"},
	}); err != nil {
		t.Fatalf("submit completed input: %v", err)
	}
	got, err = store.PendingInputIDs(ctx, "submitted-complete")
	if err != nil || len(got) != 0 {
		t.Fatalf("Submit outcome did not remove pending input: got=%q error=%v", got, err)
	}
}

func CheckKnownChannelQuarantine(t *testing.T, store Store, db *sql.DB, open OpenMaintenance) {
	t.Helper()
	ctx := context.Background()
	prefix := strings.Repeat("k", 256)
	actualChannel, candidateChannel := prefix+"actual", prefix+"candidate"
	seedInput(t, store, "corrupt-projection", actualChannel)
	seedInput(t, store, "valid-candidate", candidateChannel)
	if _, err := db.ExecContext(ctx, `UPDATE rff_inputs SET channel = ? WHERE id = ?`, []byte(candidateChannel), "corrupt-projection"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PendingInputIDs(ctx, candidateChannel); !errors.Is(err, transition.ErrIncompleteInputList) || !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("projection mismatch was not detected: %v", err)
	}
	got, err := store.PendingInputIDs(ctx, candidateChannel)
	if err != nil || !reflect.DeepEqual(got, []string{"valid-candidate"}) {
		t.Fatalf("known finding blocked an unrelated exact channel: got=%q error=%v", got, err)
	}
	if _, err := store.PendingInputIDs(ctx, actualChannel); !errors.Is(err, transition.ErrIncompleteInputList) || !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("known finding did not make its channel listing incomplete: %v", err)
	}
	if _, err := store.PendingInputIDs(ctx, "unrelated"); err != nil {
		t.Fatalf("known finding blocked independent channel: %v", err)
	}
	ingress, err := transition.NewInputIngress[string](store, actualChannel, func(transition.CodecVersion) (transition.Codec[string], error) {
		return stringCodec{}, nil
	}, func(context.Context, transition.InputDelivery[string]) (transition.InputOutcome, error) {
		t.Fatal("quarantined input reached its handler")
		return transition.InputOutcome{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = ingress.Submit(ctx, transition.InputRequest[string]{
		ID: "corrupt-projection", WorkflowID: "workflow", Kind: transition.Command, Value: "value",
		Codec: transition.CodecVersion{Name: "input", Version: "1"},
	})
	if !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("Submit bypassed input quarantine: %v", err)
	}
	maintenance, err := open(func(context.Context, string, string, string, int64, int64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer maintenance.Close()
	findings, err := maintenance.Findings(ctx, "operator", "inspect input integrity")
	if err != nil || len(findings) != 1 || findings[0].Kind != "input" || findings[0].Field != "projection" {
		t.Fatalf("projection finding: %+v error=%v", findings, err)
	}
	if err := maintenance.Resolve(ctx, "operator", "rebuild projection", findings[0]); err != nil {
		t.Fatalf("authorized projection rebuild: %v", err)
	}
	got, err = store.PendingInputIDs(ctx, actualChannel)
	if err != nil || !reflect.DeepEqual(got, []string{"corrupt-projection"}) {
		t.Fatalf("rebuilt input projection: got=%q error=%v", got, err)
	}
	audit, err := maintenance.Audit(ctx, "operator", "inspect audit", findings[0].ID)
	if err != nil || len(audit) != 1 || audit[0].Action != "resolve" {
		t.Fatalf("projection repair audit: %+v error=%v", audit, err)
	}
}

func CheckHiddenProjectionCorruptionMayOmitInput(t *testing.T, store Store, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	seedInput(t, store, "hidden-corrupt-projection", "authoritative-channel")
	if _, err := db.ExecContext(ctx, `UPDATE rff_inputs SET channel = ? WHERE id = ?`, []byte("incorrect-projection"), "hidden-corrupt-projection"); err != nil {
		t.Fatal(err)
	}
	got, err := store.PendingInputIDs(ctx, "authoritative-channel")
	if err != nil || len(got) != 0 {
		t.Fatalf("undetected hidden projection did not produce the accepted incomplete success: got=%q error=%v", got, err)
	}
}

// CheckFindingCoexistence verifies input repair alongside prior hook findings.
func CheckFindingCoexistence(t *testing.T, store Store, db *sql.DB, maintenance Maintenance) {
	t.Helper()
	ctx := context.Background()
	seedInput(t, store, "coexist-input", "actual")
	if _, err := db.ExecContext(ctx, `UPDATE rff_inputs SET channel = ? WHERE id = ?`, []byte("wrong"), "coexist-input"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PendingInputIDs(ctx, "wrong"); !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("input projection corruption: %v", err)
	}
	findings, err := maintenance.Findings(ctx, "operator", "inspect mixed findings")
	if err != nil || len(findings) != 2 || findings[0].Kind != "hook" || findings[1].Kind != "input" || findings[1].Revision <= 0 {
		t.Fatalf("mixed findings: %+v, %v", findings, err)
	}
	if err := maintenance.Resolve(ctx, "operator", "repair input", findings[1]); err != nil {
		t.Fatalf("repair input alongside hook finding: %v", err)
	}
	got, err := store.PendingInputIDs(ctx, "actual")
	if err != nil || !reflect.DeepEqual(got, []string{"coexist-input"}) {
		t.Fatalf("input after mixed repair: %q, %v", got, err)
	}
}

func CheckUnknownChannelQuarantine(t *testing.T, store Store, db *sql.DB, open OpenMaintenance) {
	t.Helper()
	ctx := context.Background()
	const id = "unreadable-input"
	if _, created, err := store.AcceptInput(ctx, id, func(*transition.InputRecord) (*transition.InputRecord, error) {
		return &transition.InputRecord{
			SchemaVersion: 1,
			ID:            id,
			Channel:       "tasks",
			WorkflowID:    "workflow",
			Kind:          transition.Command,
			Value:         transition.EncodedValue{Codec: transition.CodecVersion{Name: "input", Version: "1"}},
		}, nil
	}); err != nil || !created {
		t.Fatalf("seed input: created=%v error=%v", created, err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE rff_inputs SET record = ? WHERE id = ?`, []byte("unreadable"), id); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PendingInputIDs(ctx, "tasks"); !errors.Is(err, transition.ErrIncompleteInputList) || !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("unreadable input was not reported as an integrity finding: %v", err)
	}
	if _, err := store.PendingInputIDs(ctx, "unrelated"); !errors.Is(err, transition.ErrIncompleteInputList) || !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("unknown-channel quarantine did not block every listing: %v", err)
	}
	ingress, err := transition.NewInputIngress[string](store, "tasks", func(transition.CodecVersion) (transition.Codec[string], error) {
		return stringCodec{}, nil
	}, func(context.Context, transition.InputDelivery[string]) (transition.InputOutcome, error) {
		t.Fatal("quarantined input reached its handler")
		return transition.InputOutcome{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = ingress.Submit(ctx, transition.InputRequest[string]{
		ID: id, WorkflowID: "workflow", Kind: transition.Command, Value: "value",
		Codec: transition.CodecVersion{Name: "input", Version: "1"},
	})
	if !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("Submit bypassed unknown-channel quarantine: %v", err)
	}
	maintenance, err := open(func(context.Context, string, string, string, int64, int64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := maintenance.Close(); err != nil {
			t.Error(err)
		}
	}()
	findings, err := maintenance.Findings(ctx, "operator", "inspect input integrity")
	if err != nil || len(findings) != 1 || findings[0].Kind != "input" || findings[0].Revision <= 0 {
		t.Fatalf("safe input finding not recorded: findings=%+v error=%v", findings, err)
	}
}
