package runtime_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestLifecycleOptionalIntegritySweepDoesNotGateReadiness(t *testing.T) {
	runner, _, _ := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
	var batches atomic.Int64
	firstStarted := make(chan struct{})
	firstRelease := make(chan struct{})
	secondStarted := make(chan struct{})
	secondRelease := make(chan struct{})
	var releaseFirst, releaseSecond sync.Once
	l, err := transition.NewLifecycle[string, string, string, int](
		&lifecycleTestStore{}, runner, runner, lifecycleTestHost{},
		transition.LifecycleConfig{
			Coordinator: transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
			Worker: transition.WorkerConfig{ID: "worker", Capacity: 1, LeaseTTL: time.Second,
				RenewalInterval: time.Millisecond, Clock: transition.SystemWorkerClock{}},
			PollInterval: 10 * time.Millisecond,
			IntegritySweep: &transition.IntegritySweepConfig{
				Interval:     5 * time.Millisecond,
				OverdueAfter: 30 * time.Millisecond,
				RunBatch: func(context.Context) (transition.IntegritySweepProgress, error) {
					call := batches.Add(1)
					if call == 1 {
						close(firstStarted)
						<-firstRelease
					} else if call == 2 {
						close(secondStarted)
						<-secondRelease
					}
					return transition.IntegritySweepProgress{
						SweepID: 1, StartedAt: time.Now(), UpdatedAt: time.Now(),
						Complete: call > 1, Healthy: call > 1, Incomplete: call == 1,
					}, nil
				},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatalf("start with an incomplete optional sweep: %v", err)
	}
	defer func() {
		releaseFirst.Do(func() { close(firstRelease) })
		releaseSecond.Do(func() { close(secondRelease) })
		_ = l.Shutdown(context.Background())
	}()
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("optional sweep did not start")
	}
	deadline := time.Now().Add(time.Second)
	for !l.Status().Ready && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !l.Status().Ready {
		t.Fatalf("in-progress optional sweep blocked readiness: %+v", l.Status())
	}
	deadline = time.Now().Add(time.Second)
	for (l.Status().IntegritySweep == nil || !l.Status().IntegritySweep.Overdue) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if status := l.Status(); status.IntegritySweep == nil || !status.IntegritySweep.Overdue {
		t.Fatalf("stalled optional sweep was not reported overdue: %+v", status)
	}
	releaseFirst.Do(func() { close(firstRelease) })
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("incomplete sweep did not continue")
	}
	deadline = time.Now().Add(time.Second)
	for (l.Status().IntegritySweep == nil || !l.Status().IntegritySweep.Progress.Incomplete) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if status := l.Status(); status.IntegritySweep == nil || !status.IntegritySweep.Progress.Incomplete {
		t.Fatalf("incomplete sweep progress was not exposed: %+v", status)
	}
	releaseSecond.Do(func() { close(secondRelease) })
	deadline = time.Now().Add(time.Second)
	for (!l.Status().Ready || l.Status().IntegritySweep == nil ||
		!l.Status().IntegritySweep.Progress.Complete) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	status := l.Status()
	if !status.Ready || status.Capacity.Available != 1 {
		t.Fatalf("optional sweep blocked readiness or capacity: %+v", status)
	}
	if status.IntegritySweep == nil || !status.IntegritySweep.Progress.Complete || batches.Load() < 2 {
		t.Fatalf("optional sweep did not continue to completion: status=%+v batches=%d", status.IntegritySweep, batches.Load())
	}
}

func TestLifecycleRetriesRecoverableIntegritySweepFailures(t *testing.T) {
	runner, _, _ := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
	transient := errors.New("temporary maintenance failure")
	var batches atomic.Int64
	l, err := transition.NewLifecycle[string, string, string, int](
		&lifecycleTestStore{}, runner, runner, lifecycleTestHost{},
		transition.LifecycleConfig{
			Coordinator: transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
			Worker: transition.WorkerConfig{ID: "worker", Capacity: 1, LeaseTTL: time.Second,
				RenewalInterval: time.Millisecond, Clock: transition.SystemWorkerClock{}},
			PollInterval: 10 * time.Millisecond,
			ClassifyError: func(err error) transition.PathErrorClass {
				if errors.Is(err, transient) {
					return transition.PathErrorRecoverable
				}
				return transition.PathErrorUnclassified
			},
			IntegritySweep: &transition.IntegritySweepConfig{
				Interval: 5 * time.Millisecond, OverdueAfter: time.Second,
				RunBatch: func(context.Context) (transition.IntegritySweepProgress, error) {
					if batches.Add(1) == 1 {
						return transition.IntegritySweepProgress{}, transient
					}
					return transition.IntegritySweepProgress{
						SweepID: 1, StartedAt: time.Now(), UpdatedAt: time.Now(), Complete: true, Healthy: true,
					}, nil
				},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatalf("start with an optional sweep failure: %v", err)
	}
	defer l.Shutdown(context.Background())
	deadline := time.Now().Add(time.Second)
	for (batches.Load() < 2 || l.Status().IntegritySweep == nil ||
		!l.Status().IntegritySweep.Progress.Complete) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	status := l.Status()
	if !status.Ready || batches.Load() < 2 || status.IntegritySweep == nil ||
		status.IntegritySweep.Path.Classification != "" || !status.IntegritySweep.Progress.Complete {
		t.Fatalf("recoverable sweep failure did not retry and recover: status=%+v batches=%d", status, batches.Load())
	}
}
