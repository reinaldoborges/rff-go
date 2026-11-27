package runtime_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

type identityTimerStore struct {
	called bool
	id     string
}

func (s *identityTimerStore) ScheduleTimer(_ context.Context, id string, _ func(*transition.TimerRecord) (*transition.TimerRecord, error)) (transition.TimerRecord, bool, error) {
	s.called = true
	s.id = id
	return transition.TimerRecord{}, false, nil
}

func TestTimerSchedulerRejectsInvalidIdentitiesBeforePersistence(t *testing.T) {
	for _, id := range []string{"", string([]byte{0xff}), strings.Repeat("x", 257), strings.Repeat("é", 129)} {
		store := &identityTimerStore{}
		scheduler, err := transition.NewTimerScheduler[string](store, func(transition.CodecVersion) (transition.Codec[string], error) {
			return nil, errors.New("unexpected codec resolution")
		})
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = scheduler.Schedule(context.Background(), transition.TimerRequest[string]{
			ID: id, WorkflowID: "workflow", Deadline: time.Now(), EventCodec: transition.CodecVersion{Name: "event", Version: "1"},
		})
		if !errors.Is(err, transition.ErrInvalidDurableID) || store.called {
			t.Fatalf("invalid ID %q: err=%v store called=%v", id, err, store.called)
		}
	}
}

type identityInputStore struct {
	transition.InputStore
	called bool
	id     string
}

func (s *identityInputStore) AcceptInput(_ context.Context, id string, _ func(*transition.InputRecord) (*transition.InputRecord, error)) (transition.InputRecord, bool, error) {
	s.called = true
	s.id = id
	return transition.InputRecord{}, false, nil
}

func TestInputIngressRejectsInvalidIdentitiesBeforePersistence(t *testing.T) {
	for _, id := range []string{"", string([]byte{0xff}), strings.Repeat("x", 257), strings.Repeat("é", 129)} {
		store := &identityInputStore{}
		ingress, err := transition.NewInputIngress[string](store, "channel", func(transition.CodecVersion) (transition.Codec[string], error) {
			return nil, errors.New("unexpected codec resolution")
		}, func(context.Context, transition.InputDelivery[string]) (transition.InputOutcome, error) {
			return transition.InputOutcome{}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		_, err = ingress.Submit(context.Background(), transition.InputRequest[string]{
			ID: id, WorkflowID: "workflow", Kind: transition.Command, Codec: transition.CodecVersion{Name: "input", Version: "1"},
		})
		if !errors.Is(err, transition.ErrInvalidDurableID) || store.called {
			t.Fatalf("invalid ID %q: err=%v store called=%v", id, err, store.called)
		}
	}
}

func TestRuntimePassesValidDurableIDsUnchanged(t *testing.T) {
	for _, id := range []string{strings.Repeat("x", 256), strings.Repeat("é", 128), "a\x00tail", "é", "e\u0301"} {
		timerStore := &identityTimerStore{}
		scheduler, err := transition.NewTimerScheduler[string](timerStore, func(transition.CodecVersion) (transition.Codec[string], error) {
			return nil, errors.New("unexpected codec resolution")
		})
		if err != nil {
			t.Fatal(err)
		}
		_, _, _ = scheduler.Schedule(context.Background(), transition.TimerRequest[string]{
			ID: id, WorkflowID: "workflow", Deadline: time.Now(), EventCodec: transition.CodecVersion{Name: "event", Version: "1"},
		})
		if !timerStore.called || timerStore.id != id {
			t.Fatalf("timer ID changed: got %q want %q", timerStore.id, id)
		}
		inputStore := &identityInputStore{}
		ingress, err := transition.NewInputIngress[string](inputStore, "channel", func(transition.CodecVersion) (transition.Codec[string], error) {
			return nil, errors.New("unexpected codec resolution")
		}, func(context.Context, transition.InputDelivery[string]) (transition.InputOutcome, error) {
			return transition.InputOutcome{}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		_, _ = ingress.Submit(context.Background(), transition.InputRequest[string]{
			ID: id, WorkflowID: "workflow", Kind: transition.Command, Codec: transition.CodecVersion{Name: "input", Version: "1"},
		})
		if !inputStore.called || inputStore.id != id {
			t.Fatalf("input ID changed: got %q want %q", inputStore.id, id)
		}
	}
}
