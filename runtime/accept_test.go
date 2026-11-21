package runtime_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type state = string

type stringCodec struct{}

func (stringCodec) Encode(_ context.Context, value string) ([]byte, error) { return []byte(value), nil }
func (stringCodec) Decode(_ context.Context, data []byte) (string, error)  { return string(data), nil }
func (stringCodec) Equal(_ context.Context, data []byte, value string) (bool, error) {
	return string(data) == value, nil
}

type intCodec struct{}

func (intCodec) Encode(_ context.Context, value int) ([]byte, error) {
	return []byte(strconv.Itoa(value)), nil
}
func (intCodec) Decode(_ context.Context, data []byte) (int, error) {
	return strconv.Atoi(string(data))
}
func (intCodec) Equal(_ context.Context, data []byte, value int) (bool, error) {
	stored, err := strconv.Atoi(string(data))
	return stored == value, err
}

func TestAcceptInitialPersistsSelectedSnapshot(t *testing.T) {
	store := newMemoryStore()
	machine, err := fsm.NewMachine(fsm.Config[string, state, string, int]{
		ID: "task", Version: "1", InitialStates: []state{"open"},
		Handlers: map[state]fsm.Handler[string, state, string, int]{
			"open": fsm.NoopHandler[string, state, string, int]{},
		},
		SelectInitial: func(input int) (fsm.Selection[state], error) {
			if input != 7 {
				t.Fatalf("unexpected input: %d", input)
			}
			return fsm.Selection[state]{
				Target: "open", Notify: true,
				Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Second, OverallTimeout: time.Minute},
			}, nil
		},
		SelectTransition: func(source state, _ string, _ int) (fsm.Selection[state], error) {
			return fsm.Selection[state]{Target: source}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var registry fsm.Registry[string, state, string, int]
	if err := registry.Register(machine); err != nil {
		t.Fatal(err)
	}
	clock := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	acceptor, err := transition.NewAcceptor[string, state, string, int](
		store, &registry,
		func(ref transition.CodecVersion) (transition.Codec[int], error) {
			if ref != (transition.CodecVersion{Name: "input", Version: "1"}) {
				return nil, fmt.Errorf("unknown input codec")
			}
			return intCodec{}, nil
		},
		func(ref transition.CodecVersion) (transition.Codec[string], error) { return stringCodec{}, nil },
		func() (string, error) { return "execution-1", nil },
		func() time.Time { return clock },
	)
	if err != nil {
		t.Fatal(err)
	}
	record, created, err := acceptor.Accept(context.Background(), transition.Request[string, state, string, int]{
		Entity: "entity", Key: "request-key", Operation: transition.Initialize,
		MachineID: "task", MachineVersion: "1", Input: 7,
		InputCodec:   transition.CodecVersion{Name: "input", Version: "1"},
		Notification: transition.NotifyInherit,
	})
	if err != nil || !created {
		t.Fatalf("accept: created=%v error=%v", created, err)
	}
	if record.ID != "execution-1" || record.Entity != "entity" || record.Target != "open" ||
		record.HasPrevious || record.Status != transition.Waiting || !record.Notify ||
		record.Deadline != clock.Add(time.Minute) || string(record.Input.Data) != "7" {
		t.Fatalf("unexpected accepted record: %+v", record)
	}
	var codec transition.Codec[int] = intCodec{}
	input, err := codec.Decode(context.Background(), record.Input.Data)
	if err != nil || input != 7 {
		t.Fatalf("input could not be rehydrated: %d, %v", input, err)
	}
}
