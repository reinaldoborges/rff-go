package runtime_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type acceptanceFixture struct {
	store       *memoryStore
	acceptor    *transition.Acceptor[string, state, string, int]
	request     transition.Request[string, state, string, int]
	selections  atomic.Int64
	ids         atomic.Int64
	codecAbsent atomic.Bool
	codecNil    atomic.Bool
}

var errInputCodecUnavailable = errors.New("input codec unavailable")

func newAcceptanceFixture(t *testing.T) *acceptanceFixture {
	t.Helper()
	f := &acceptanceFixture{store: newMemoryStore()}
	machine, err := fsm.NewMachine(fsm.Config[string, state, string, int]{
		ID: "task", Version: "1", InitialStates: []state{"open"},
		Handlers: map[state]fsm.Handler[string, state, string, int]{
			"open": fsm.NoopHandler[string, state, string, int]{},
			"done": fsm.NoopHandler[string, state, string, int]{},
		},
		SelectInitial: func(int) (fsm.Selection[state], error) {
			f.selections.Add(1)
			return fsm.Selection[state]{Target: "open", Notify: true, Policy: fsm.Policy{
				MaxAttempts: 2, AttemptTimeout: time.Second, OverallTimeout: time.Minute,
				RetryDelays: []time.Duration{time.Second},
			}}, nil
		},
		SelectTransition: func(source state, event string, _ int) (fsm.Selection[state], error) {
			f.selections.Add(1)
			if source != "open" || event != "finish" {
				return fsm.Selection[state]{}, errors.New("event rejected")
			}
			return fsm.Selection[state]{Target: "done", Policy: fsm.Policy{
				MaxAttempts: 1, AttemptTimeout: time.Second, OverallTimeout: time.Minute,
			}}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var registry fsm.Registry[string, state, string, int]
	if err := registry.Register(machine); err != nil {
		t.Fatal(err)
	}
	f.acceptor, err = transition.NewAcceptor[string, state, string, int](
		f.store, &registry,
		func(ref transition.CodecVersion) (transition.Codec[int], error) {
			if f.codecNil.Load() {
				var codec *intCodec
				return codec, nil
			}
			if f.codecAbsent.Load() || ref != (transition.CodecVersion{Name: "input", Version: "1"}) {
				return nil, errInputCodecUnavailable
			}
			return intCodec{}, nil
		},
		func(ref transition.CodecVersion) (transition.Codec[string], error) {
			if ref != (transition.CodecVersion{Name: "event", Version: "1"}) {
				return nil, errors.New("event codec unavailable")
			}
			return stringCodec{}, nil
		},
		func() (string, error) { return fmt.Sprintf("execution-%d", f.ids.Add(1)), nil },
		func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) },
	)
	if err != nil {
		t.Fatal(err)
	}
	f.request = transition.Request[string, state, string, int]{
		Entity: "entity", Key: "key", Operation: transition.Initialize,
		MachineID: "task", MachineVersion: "1", Input: 7,
		InputCodec:   transition.CodecVersion{Name: "input", Version: "1"},
		Notification: transition.NotifyInherit,
	}
	return f
}

func TestAcceptanceReplayConflictBusyAndMissingCodec(t *testing.T) {
	f := newAcceptanceFixture(t)
	first, created, err := f.acceptor.Accept(context.Background(), f.request)
	if err != nil || !created {
		t.Fatalf("first acceptance: created=%v error=%v", created, err)
	}
	replayed, created, err := f.acceptor.Accept(context.Background(), f.request)
	if err != nil || created || replayed.ID != first.ID || f.selections.Load() != 1 {
		t.Fatalf("replay changed execution or reran selection: %+v created=%v error=%v", replayed, created, err)
	}
	changed := f.request
	changed.Input++
	if _, _, err := f.acceptor.Accept(context.Background(), changed); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("changed input should conflict: %v", err)
	}
	changed = f.request
	changed.Notification = transition.NotifyDisable
	if _, _, err := f.acceptor.Accept(context.Background(), changed); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("changed override should conflict: %v", err)
	}
	changed = f.request
	changed.Key = "other"
	if _, _, err := f.acceptor.Accept(context.Background(), changed); !errors.Is(err, transition.ErrBusy) {
		t.Fatalf("different active request should be busy: %v", err)
	}
	f.codecAbsent.Store(true)
	if _, _, err := f.acceptor.Accept(context.Background(), f.request); !errors.Is(err, transition.ErrCompatibility) || !errors.Is(err, errInputCodecUnavailable) {
		t.Fatalf("missing original codec should fail explicitly: %v", err)
	}
	if f.selections.Load() != 1 {
		t.Fatal("a replay, conflict, or busy request reran selection")
	}
}
