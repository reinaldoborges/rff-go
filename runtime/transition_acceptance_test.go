package runtime_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestTransitionAcceptanceAndSemanticConflicts(t *testing.T) {
	f := newAcceptanceFixture(t)
	request := f.request
	request.Operation = transition.Transition
	request.Previous, request.HasPrevious = "open", true
	request.Event, request.HasEvent = "finish", true
	request.EventCodec = transition.CodecVersion{Name: "event", Version: "1"}
	request.Notification = transition.NotifyEnable
	record, created, err := f.acceptor.Accept(context.Background(), request)
	if err != nil || !created || !record.HasPrevious || record.Previous != "open" ||
		record.Target != "done" || !record.Notify || string(record.Event.Data) != "finish" {
		t.Fatalf("unexpected transition acceptance: record=%+v created=%v error=%v", record, created, err)
	}
	changed := request
	changed.Event = "other"
	if _, _, err := f.acceptor.Accept(context.Background(), changed); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("changed event did not conflict: %v", err)
	}
	changed = request
	changed.MachineVersion = "2"
	if _, _, err := f.acceptor.Accept(context.Background(), changed); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("changed version did not conflict: %v", err)
	}
	if f.selections.Load() != 1 {
		t.Fatal("a conflicting replay reran selection")
	}
}

func TestConcurrentIdenticalRequestsSelectOnce(t *testing.T) {
	f := newAcceptanceFixture(t)
	const count = 20
	start := make(chan struct{})
	var wg sync.WaitGroup
	var created atomic.Int64
	failures := make(chan error, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			record, isNew, err := f.acceptor.Accept(context.Background(), f.request)
			if err != nil {
				failures <- err
			} else if record.ID != "execution-1" {
				failures <- errors.New("concurrent replay returned a different execution")
			} else if isNew {
				created.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	if created.Load() != 1 || f.selections.Load() != 1 {
		t.Fatalf("created=%d selections=%d, want one of each", created.Load(), f.selections.Load())
	}
}

func TestNotificationOverrideIsPersisted(t *testing.T) {
	for _, tt := range []struct {
		override transition.NotificationOverride
		want     bool
	}{
		{transition.NotifyInherit, true},
		{transition.NotifyEnable, true},
		{transition.NotifyDisable, false},
	} {
		t.Run(string(tt.override), func(t *testing.T) {
			f := newAcceptanceFixture(t)
			request := f.request
			request.Notification = tt.override
			record, _, err := f.acceptor.Accept(context.Background(), request)
			if err != nil || record.NotificationOverride != tt.override || record.Notify != tt.want {
				t.Fatalf("unexpected notification choice: record=%+v error=%v", record, err)
			}
		})
	}
}
