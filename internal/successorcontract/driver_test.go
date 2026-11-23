package successorcontract

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestDeliveryPermitCheckedAtHostAcceptance(t *testing.T) {
	host, err := OpenHost(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = host.Close() })
	if err := host.Seed("entity"); err != nil {
		t.Fatal(err)
	}
	binding := BindingFor(transition.ContinuationMarker[string, string]{
		ExecutionID: "origin", OperationID: "commit", Entity: "entity", Target: "due",
	})
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var nanos atomic.Int64
	nanos.Store(base.UnixNano())
	checked := make(chan struct{})
	var once sync.Once
	guarded := &driverHost{Host: host, clock: func() time.Time {
		once.Do(func() { close(checked) })
		return time.Unix(0, nanos.Load())
	}}
	tx, err := host.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := guarded.AcceptContinuationSuccessor(context.Background(), binding, transition.ContinuationDeliveryPermit{
			Generation: 1, ExpiresAt: base.Add(time.Second),
		})
		result <- err
	}()
	<-checked
	nanos.Store(base.Add(2 * time.Second).UnixNano())
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err == nil {
		t.Fatal("late host acceptance must reject the expired permit")
	}
	outcome, err := host.LookupSuccessor(context.Background(), binding)
	if err != nil || outcome.Outcome != transition.SuccessorHostAbsent {
		t.Fatalf("expired request was accepted: %+v, %v", outcome, err)
	}
}

func TestWaiverTombstoneRacesAcceptance(t *testing.T) {
	for _, cancellationFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "cancellation-first", false: "acceptance-first"}[cancellationFirst], func(t *testing.T) {
			host, err := OpenHost(filepath.Join(t.TempDir(), "host.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = host.Close() })
			if err := host.Seed("entity"); err != nil {
				t.Fatal(err)
			}
			binding := BindingFor(transition.ContinuationMarker[string, string]{
				ExecutionID: "origin", OperationID: "commit", Entity: "entity", Target: "due",
			})
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			checked, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			guarded := &driverHost{Host: host, clock: func() time.Time {
				if cancellationFirst {
					once.Do(func() { close(checked); <-release })
				}
				return now
			}}
			results := make(chan Result, 1)
			errors := make(chan error, 1)
			go func() {
				result, err := guarded.AcceptContinuationSuccessor(context.Background(), binding, transition.ContinuationDeliveryPermit{
					Generation: 1, ExpiresAt: now.Add(time.Minute),
				})
				results <- result
				errors <- err
			}()
			if cancellationFirst {
				<-checked
			} else {
				result := <-results
				if err := <-errors; err != nil || result.Outcome != transition.SuccessorHostAccepted {
					t.Fatalf("acceptance failed: %+v, %v", result, err)
				}
			}
			result, err := guarded.CancelContinuationSuccessor(context.Background(), binding)
			if err != nil {
				t.Fatal(err)
			}
			expected := transition.SuccessorHostAccepted
			if cancellationFirst {
				expected = transition.SuccessorHostCanceled
				close(release)
				accepted := <-results
				if err := <-errors; err != nil || accepted.Outcome != expected {
					t.Fatalf("late acceptance crossed tombstone: %+v, %v", accepted, err)
				}
			}
			if result.Outcome != expected {
				t.Fatalf("cancellation lost authoritative outcome: %+v", result)
			}
		})
	}
}
