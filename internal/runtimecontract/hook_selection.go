package runtimecontract

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type IndexedHookStore interface {
	HiddenHookStore
	StartHook(context.Context, transition.HookLease, func() time.Time) (transition.StartedHook, error)
	ReportHook(context.Context, transition.HookLease, transition.HookResult, func() time.Time) error
	RenewHook(context.Context, transition.HookLease, func() time.Time, time.Duration) (transition.HookLease, error)
	BlockHookCompatibility(context.Context, transition.HookLease, transition.CompatibilityDiagnostic, func() time.Time) error
}

func RunIndexedHookSelection(t *testing.T, newStore func(*testing.T) IndexedHookStore) {
	t.Helper()
	t.Run("eligibility ordering and fences", func(t *testing.T) {
		ctx := context.Background()
		store := newStore(t)
		now := time.Date(2026, 1, 1, 12, 0, 0, 123456789, time.UTC)
		clock := func() time.Time { return now }
		seed := func(id string, due time.Time) {
			t.Helper()
			_, _, err := store.Accept(ctx, id, "request", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
				return &transition.Execution[string, string]{
					SchemaVersion: transition.CurrentSchemaVersion, ID: id, Entity: id, Key: "request",
					Operation: transition.Initialize, Target: "ready", Status: transition.Waiting,
					MachineID: "machine", MachineVersion: "1", Deadline: now.Add(time.Hour),
					Policy: fsm.Policy{MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
				}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			fence, err := store.AcquireTransitionLease(ctx, id, id, "coordinator", clock, time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.DispatchHook(ctx, fence, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: due}); err != nil {
				t.Fatal(err)
			}
		}
		claim := func(id string) transition.HookLease {
			t.Helper()
			lease, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
			if err != nil || lease.ExecutionID != id {
				t.Fatalf("claim = %+v, %v; want %q", lease, err, id)
			}
			return lease
		}
		finish := func(lease transition.HookLease) {
			t.Helper()
			if _, err := store.StartHook(ctx, lease, clock); err != nil {
				t.Fatal(err)
			}
			if err := store.ReportHook(ctx, lease, transition.HookResult{Success: true}, clock); err != nil {
				t.Fatal(err)
			}
		}
		seed("started", now)
		started := claim("started")
		if _, err := store.StartHook(ctx, started, clock); err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Minute)
		seed("completed", now)
		finish(claim("completed"))
		seed("blocked", now)
		blocked := claim("blocked")
		if err := store.BlockHookCompatibility(ctx, blocked, transition.CompatibilityDiagnostic{
			Component: transition.CompatibilityMachine, Name: "machine", Version: "1",
		}, clock); err != nil {
			t.Fatal(err)
		}
		seed("held", now)
		held := claim("held")
		seed("future", now.Add(time.Minute+time.Nanosecond))
		ids := []string{"a", "A", "a\x00tail", "é", "e\u0301", strings.Repeat("x", 255) + "a", strings.Repeat("x", 255) + "b"}
		for _, id := range ids {
			seed(id, now)
		}
		sort.Strings(ids)
		for _, id := range ids {
			finish(claim(id))
		}
		if _, err := store.ClaimHook(ctx, "worker", clock, time.Minute); !errors.Is(err, transition.ErrNoHookActivity) {
			t.Fatalf("ineligible started/completed/blocked/leased/future work was claimed: %v", err)
		}
		now = now.Add(time.Minute)
		reclaimed := claim("held")
		if reclaimed.Generation != held.Generation+1 || reclaimed.ActivityID != held.ActivityID {
			t.Fatalf("reclaim did not advance fence: first=%+v second=%+v", held, reclaimed)
		}
		if _, err := store.RenewHook(ctx, held, clock, time.Minute); !errors.Is(err, transition.ErrStaleActivityLease) {
			t.Fatalf("stale claim renewed: %v", err)
		}
		if err := store.ReportHook(ctx, held, transition.HookResult{Success: true}, clock); !errors.Is(err, transition.ErrStaleActivityLease) {
			t.Fatalf("stale claim reported: %v", err)
		}
		finish(reclaimed)
		if _, err := store.ClaimHook(ctx, "worker", clock, time.Minute); !errors.Is(err, transition.ErrNoHookActivity) {
			t.Fatalf("future hook was claimed early: %v", err)
		}
		now = now.Add(time.Nanosecond)
		finish(claim("future"))
	})
	t.Run("competing claims", func(t *testing.T) {
		ctx := context.Background()
		store := newStore(t)
		now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		clock := func() time.Time { return now }
		_, _, err := store.Accept(ctx, "entity", "request", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			return &transition.Execution[string, string]{SchemaVersion: transition.CurrentSchemaVersion,
				ID: "execution", Entity: "entity", Key: "request", Operation: transition.Initialize,
				Target: "ready", Status: transition.Waiting}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		fence, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.DispatchHook(ctx, fence, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
			t.Fatal(err)
		}
		start, results := make(chan struct{}), make(chan error, 2)
		for _, owner := range []string{"one", "two"} {
			go func(owner string) {
				<-start
				lease, err := store.ClaimHook(ctx, owner, clock, time.Minute)
				if err == nil && (lease.ExecutionID != "execution" || lease.Owner != owner || lease.Generation != 1) {
					err = errors.New("invalid winning lease")
				}
				results <- err
			}(owner)
		}
		close(start)
		var successes, unavailable int
		for range 2 {
			switch err := <-results; {
			case err == nil:
				successes++
			case errors.Is(err, transition.ErrNoHookActivity):
				unavailable++
			default:
				t.Fatalf("competing claim: %v", err)
			}
		}
		if successes != 1 || unavailable != 1 {
			t.Fatalf("successes=%d unavailable=%d", successes, unavailable)
		}
	})
}
