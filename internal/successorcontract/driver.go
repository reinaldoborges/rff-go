package successorcontract

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type DriverStore interface {
	Store
	transition.ContinuationDriveStore[string, string]
	transition.ContinuationControlStore[string, string]
}

type driverHost struct {
	*Host
	clock func() time.Time
	drop  bool
}

func (h *driverHost) LookupSuccessor(ctx context.Context, binding Binding) (Result, error) {
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return Result{}, err
	}
	defer tx.Rollback()
	result, err := lookup(ctx, tx.QueryRowContext, binding)
	if err != nil {
		return Result{}, err
	}
	if err := tx.Commit(); err != nil {
		return Result{}, err
	}
	return result, nil
}

func (h *driverHost) CancelContinuationSuccessor(ctx context.Context, binding Binding) (Result, error) {
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return Result{}, err
	}
	defer tx.Rollback()
	prior, err := lookup(ctx, tx.QueryRowContext, binding)
	if err != nil {
		return Result{}, err
	}
	if prior.Outcome != transition.SuccessorHostAbsent && prior.Outcome != transition.SuccessorHostPending {
		return prior, nil
	}
	evidence := []byte("cancelled:" + binding.Origin.ExecutionID)
	if prior.Outcome == transition.SuccessorHostPending {
		result, err := tx.ExecContext(ctx, `UPDATE domain SET fence = '' WHERE entity = ? AND fence = ?`, binding.Origin.Entity, binding.Execution.ID)
		if err != nil {
			return Result{}, err
		}
		if n, err := result.RowsAffected(); err != nil || n != 1 {
			return Result{}, transition.ErrConflict
		}
		if _, err := tx.ExecContext(ctx, `UPDATE successor_receipts SET outcome = 'cancelled', evidence = ? WHERE origin = ? AND outcome = 'pending'`, evidence, binding.Origin.ExecutionID); err != nil {
			return Result{}, err
		}
	} else {
		data, err := bindingBytes(binding)
		if err != nil {
			return Result{}, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO successor_receipts(origin, binding, outcome, principal, accepted_at, evidence) VALUES (?, ?, 'cancelled', '', '', ?)`,
			binding.Origin.ExecutionID, data, evidence); err != nil {
			return Result{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return Result{}, err
	}
	if os.Getenv("RFF_DRIVER_CRASH") == "after-tombstone" {
		os.Exit(87)
	}
	return h.LookupSuccessor(ctx, binding)
}

func (h *driverHost) AcceptContinuationSuccessor(ctx context.Context, binding Binding, permit transition.ContinuationDeliveryPermit) (Result, error) {
	if h.drop {
		h.drop = false
		return Result{}, errors.New("request lost before host acceptance")
	}
	if !h.clock().Before(permit.ExpiresAt) {
		return Result{}, errors.New("expired delivery permit")
	}
	return h.Host.acceptSuccessor(ctx, binding, func() error {
		if !h.clock().Before(permit.ExpiresAt) {
			return errors.New("expired delivery permit")
		}
		return nil
	})
}

func RunDriverOneAttempt(t *testing.T, store DriverStore, host *Host, outcome string) {
	t.Helper()
	ctx := context.Background()
	setup := Setup(t, store, host, "driver")
	marker := setup.Origin
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	calls := 0
	machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
		ID: "plan", Version: "1", InitialStates: []string{"due"},
		Handlers: map[string]fsm.Handler[string, string, string, string]{
			"due": fsm.NoopHandler[string, string, string, string]{}, "paid": fsm.NoopHandler[string, string, string, string]{},
		},
		ContinuationPolicies: map[string]fsm.Policy{"due": marker.Policy},
		SelectInitial: func(string) (fsm.Selection[string], error) {
			return fsm.Selection[string]{Target: "due", Policy: marker.Policy}, nil
		},
		SelectTransition: func(string, string, string) (fsm.Selection[string], error) {
			if outcome == "late-selection" {
				now = now.Add(time.Minute)
			}
			return fsm.Selection[string]{Target: "paid", Policy: marker.Policy}, nil
		},
		Continue: func(context.Context, string, string) (fsm.ContinuationDecision[string, string], error) {
			calls++
			if outcome == "late-evaluation" {
				now = now.Add(time.Minute)
			}
			return fsm.ContinuationDecision[string, string]{Continue: true, Event: "pay", Input: "payload"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	var registry fsm.Registry[string, string, string, string]
	if err := registry.Register(machine); err != nil {
		t.Fatal(err)
	}
	codec := func(transition.CodecVersion) (transition.Codec[string], error) { return stringCodec{}, nil }
	acceptor, err := transition.NewAcceptor(store, &registry, codec, codec, func() (string, error) { return "next-execution", nil }, clock)
	if err != nil {
		t.Fatal(err)
	}
	guardedHost := &driverHost{Host: host, clock: clock, drop: outcome == "absent" || outcome == "waive-absent" || outcome == "stale-absence"}
	driver, err := transition.NewContinuationDriver(store, &registry, transition.ContinuationDriverConfig[string, string, string, string]{
		Clock: clock, LeaseDuration: 2 * time.Minute,
		EventCodec: transition.CodecVersion{Name: "event", Version: "1"}, InputCodec: transition.CodecVersion{Name: "input", Version: "1"},
		ResolveEvent: codec, ResolveInput: codec, Acceptor: acceptor, Host: guardedHost,
		OriginRevision: func(context.Context, transition.ContinuationMarker[string, string]) ([]byte, error) {
			if outcome == "revision-error" {
				return nil, errors.New("private revision failure")
			}
			return []byte("revision-1"), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	host.LoseReply = outcome == "accepted-lost"
	if outcome == "denied-accepted" || outcome == "stale-denial" {
		host.Authorize = "deny"
	}
	err = driver.Drive(ctx, marker, "worker")
	if os.Getenv("RFF_DRIVER_CRASH") == "after-dispatch" {
		os.Exit(87)
	}
	if outcome == "accepted-lost" {
		if err == nil {
			t.Fatal("expected lost host response")
		}
		now = now.Add(2 * time.Hour)
		if err := driver.Drive(ctx, marker, "worker-after-restart"); err != nil {
			t.Fatalf("accepted work must reconcile even after exhaustion: %v", err)
		}
	} else if outcome == "denied-accepted" || outcome == "stale-denial" {
		if !errors.Is(err, transition.ErrSuccessorDenied) {
			t.Fatalf("expected definitive denial: %v", err)
		}
		inspection, err := store.InspectSuccessor(ctx, marker)
		if err != nil {
			t.Fatal(err)
		}
		if outcome == "stale-denial" {
			progress, err := store.ReadContinuation(ctx, marker.ExecutionID)
			if err != nil {
				t.Fatal(err)
			}
			oldPermit := transition.ContinuationDeliveryPermit{Generation: progress.StartedGeneration, ExpiresAt: progress.PermitExpires}
			if err := store.ApplyContinuationControl(ctx, marker, clock, transition.ContinuationControl{
				ID: "retry", Action: transition.RetryContinuation, Principal: "operator", Reason: "credentials rotated",
			}); err != nil {
				t.Fatal(err)
			}
			if err := store.RecordContinuationDenial(ctx, inspection.Binding, "test-worker", oldPermit, clock); !errors.Is(err, transition.ErrStaleLease) {
				t.Fatalf("late denial must not override authorized retry: %v", err)
			}
			progress, err = store.ReadContinuation(ctx, marker.ExecutionID)
			if err != nil || progress.Phase != transition.ContinuationWaiting || progress.Attempts != 0 {
				t.Fatalf("retry was overwritten by a stale denial: %+v, %v", progress, err)
			}
			host.Authorize = "allow"
			if err := driver.Drive(ctx, marker, "worker"); err != nil {
				t.Fatalf("authorized retry should deliver original request: %v", err)
			}
			return
		}
		host.Authorize = "allow"
		result, err := guardedHost.AcceptContinuationSuccessor(ctx, inspection.Binding, transition.ContinuationDeliveryPermit{
			Generation: 1, ExpiresAt: now.Add(time.Minute),
		})
		if err != nil || result.Outcome != transition.SuccessorHostAccepted {
			t.Fatalf("concurrent accepted request: %+v, %v", result, err)
		}
		if err := driver.Drive(ctx, marker, "worker"); err != nil {
			t.Fatalf("accepted work must reconcile after intervention: %v", err)
		}
	} else if outcome == "late-selection" {
		if err == nil {
			t.Fatal("late successor selection must not prepare or accept")
		}
		progress, err := store.ReadContinuation(ctx, marker.ExecutionID)
		if err != nil || progress.Phase != transition.ContinuationIntervention || progress.Decision == nil {
			t.Fatalf("late selection must consume its attempt and retain its decision: %+v, %v", progress, err)
		}
		if _, err := store.InspectSuccessor(ctx, marker); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("late selection must not persist a successor: %v", err)
		}
		return
	} else if outcome == "late-evaluation" {
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("late evaluator must time out: %v", err)
		}
		progress, err := store.ReadContinuation(ctx, marker.ExecutionID)
		if err != nil || progress.Phase != transition.ContinuationIntervention || progress.Decision != nil || len(progress.History) != 1 {
			t.Fatalf("late evaluation must consume the attempt without recording a decision: %+v, %v", progress, err)
		}
		return
	} else if outcome == "revision-error" {
		if err == nil {
			t.Fatal("expected revision failure")
		}
		progress, err := store.ReadContinuation(ctx, marker.ExecutionID)
		if err != nil || progress.Phase != transition.ContinuationIntervention || progress.Decision == nil || len(progress.History) != 1 {
			t.Fatalf("preparation failure must exhaust the attempt and retain the decision: %+v, %v", progress, err)
		}
		if err := driver.Drive(ctx, marker, "worker"); !errors.Is(err, transition.ErrContinuationIntervention) {
			t.Fatalf("exhausted drive must report intervention: %v", err)
		}
		return
	} else if outcome == "waive-absent" {
		if err == nil {
			t.Fatal("expected lost dispatch")
		}
		if err := store.ApplyContinuationControl(ctx, marker, clock, transition.ContinuationControl{
			ID: "waive", Action: transition.WaiveContinuation, Principal: "operator", Reason: "not needed",
		}); err != nil {
			t.Fatal(err)
		}
		if os.Getenv("RFF_DRIVER_CRASH") == "after-waiver" {
			os.Exit(87)
		}
		if err := driver.Drive(ctx, marker, "other-worker"); err != nil {
			t.Fatalf("driver must fence unaccepted delivery before waiving: %v", err)
		}
		progress, err := store.ReadContinuation(ctx, marker.ExecutionID)
		if err != nil || progress.Phase != transition.ContinuationWaived {
			t.Fatalf("waiver was not finalized: %+v, %v", progress, err)
		}
		inspection, err := store.InspectSuccessor(ctx, marker)
		if err != nil {
			t.Fatal(err)
		}
		late, err := guardedHost.AcceptContinuationSuccessor(ctx, inspection.Binding, transition.ContinuationDeliveryPermit{
			Generation: 1, ExpiresAt: now.Add(time.Minute),
		})
		if err != nil || late.Outcome != transition.SuccessorHostCanceled {
			t.Fatalf("host tombstone must defeat delayed delivery: %+v, %v", late, err)
		}
		return
	} else if outcome == "absent" || outcome == "stale-absence" {
		if err == nil {
			t.Fatal("expected lost dispatch")
		}
		oldProgress, err := store.ReadContinuation(ctx, marker.ExecutionID)
		if err != nil {
			t.Fatal(err)
		}
		if err := driver.Drive(ctx, marker, "other-worker"); !errors.Is(err, transition.ErrSuccessorPending) {
			t.Fatalf("unexpired unknown request must stay pending: %v", err)
		}
		now = now.Add(61 * time.Second)
		if err := driver.Drive(ctx, marker, "other-worker"); !errors.Is(err, transition.ErrContinuationIntervention) {
			t.Fatalf("proven absent request after exhausted attempt must require intervention: %v", err)
		}
		if err := store.ApplyContinuationControl(ctx, marker, clock, transition.ContinuationControl{
			ID: "retry", Action: transition.RetryContinuation, Principal: "operator", Reason: "retry absent request",
		}); err != nil {
			t.Fatal(err)
		}
		if outcome == "stale-absence" {
			guardedHost.drop = true
			if err := driver.Drive(ctx, marker, "other-worker"); err == nil {
				t.Fatal("expected second lost dispatch")
			}
			now = now.Add(61 * time.Second)
			err := store.ResolveContinuationAbsent(ctx, marker, transition.ContinuationDeliveryPermit{
				Generation: oldProgress.StartedGeneration, ExpiresAt: oldProgress.PermitExpires,
			}, clock)
			if !errors.Is(err, transition.ErrStaleLease) {
				t.Fatalf("old absence proof must not resolve a later attempt: %v", err)
			}
			progress, err := store.ReadContinuation(ctx, marker.ExecutionID)
			if err != nil || progress.Phase != transition.ContinuationAccepting {
				t.Fatalf("new acceptance must remain unresolved: %+v, %v", progress, err)
			}
			return
		}
		if err := driver.Drive(ctx, marker, "other-worker"); err != nil {
			t.Fatalf("authorized retry should deliver original request: %v", err)
		}
	} else if err != nil {
		t.Fatalf("one attempt must evaluate and accept: %v", err)
	}
	progress, err := store.ReadContinuation(ctx, marker.ExecutionID)
	if err != nil || progress.Attempts != 1 || progress.Phase != transition.ContinuationAccepted || calls != 1 {
		t.Fatalf("one drive must finish the continuation: %+v calls=%d err=%v", progress, calls, err)
	}
	inspection, err := store.InspectSuccessor(ctx, marker)
	if err != nil || inspection.Phase != transition.SuccessorAccepted || inspection.Binding.Execution.ID != "next-execution" {
		t.Fatalf("successor not durably accepted: %+v, %v", inspection, err)
	}
}

func CheckDriverRestart(t *testing.T, store interface {
	DriverStore
	ContinuationMarkers(context.Context) ([]transition.ContinuationMarker[string, string], error)
}, host *Host, outcome string) {
	t.Helper()
	ctx := context.Background()
	markers, err := store.ContinuationMarkers(ctx)
	if err != nil || len(markers) != 1 {
		t.Fatalf("restart markers: %+v, %v", markers, err)
	}
	now := time.Date(2026, 1, 1, 0, 1, 1, 0, time.UTC)
	clock := func() time.Time { return now }
	var registry fsm.Registry[string, string, string, string]
	codec := func(transition.CodecVersion) (transition.Codec[string], error) { return stringCodec{}, nil }
	acceptor, err := transition.NewAcceptor(store, &registry, codec, codec, func() (string, error) { return "unused", nil }, clock)
	if err != nil {
		t.Fatal(err)
	}
	driver, err := transition.NewContinuationDriver(store, &registry, transition.ContinuationDriverConfig[string, string, string, string]{
		Clock: clock, LeaseDuration: time.Minute,
		EventCodec: transition.CodecVersion{Name: "event", Version: "1"}, InputCodec: transition.CodecVersion{Name: "input", Version: "1"},
		ResolveEvent: codec, ResolveInput: codec, Acceptor: acceptor, Host: &driverHost{Host: host, clock: clock},
		OriginRevision: func(context.Context, transition.ContinuationMarker[string, string]) ([]byte, error) {
			return []byte("revision-1"), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = driver.Drive(ctx, markers[0], "restart")
	expected := transition.ContinuationAccepted
	if outcome == "absent" {
		expected = transition.ContinuationIntervention
		if !errors.Is(err, transition.ErrContinuationIntervention) {
			t.Fatalf("absent attempt must exhaust its budget: %v", err)
		}
	} else if err != nil {
		t.Fatal(err)
	} else if outcome == "waive-absent" {
		expected = transition.ContinuationWaived
	}
	progress, err := store.ReadContinuation(ctx, markers[0].ExecutionID)
	if err != nil || progress.Phase != expected || progress.Attempts != 1 || progress.Decision == nil {
		t.Fatalf("unexpected restart state: %+v, %v", progress, err)
	}
}
