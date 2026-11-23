package runtime_test

import (
	"context"
	"errors"
	"testing"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestSuccessorBindingReplaysOnlyIdenticalOriginAndSemantics(t *testing.T) {
	f := newAcceptanceFixture(t)
	request := f.request
	request.Operation = transition.Transition
	request.Previous, request.HasPrevious = "open", true
	request.Event, request.HasEvent = "finish", true
	request.EventCodec = transition.CodecVersion{Name: "event", Version: "1"}
	execution, _, err := f.acceptor.Accept(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	marker := continuationMarker("origin")
	marker.Entity, marker.Target = request.Entity, request.Previous
	marker.MachineID, marker.MachineVersion = request.MachineID, request.MachineVersion
	successor := transition.SuccessorRequest[string, string, string, int]{
		Origin: marker, OriginRevision: []byte("revision-1"), Request: request,
	}
	binding := transition.SuccessorBinding[string, string]{
		Origin: marker, OriginRevision: []byte("revision-1"), Execution: execution,
	}
	if err := f.acceptor.MatchSuccessor(context.Background(), binding, successor); err != nil {
		t.Fatal(err)
	}
	migrated := successor
	migrated.Request.MachineVersion = "2"
	migratedBinding := binding
	migratedBinding.Execution.MachineVersion = "2"
	if err := f.acceptor.MatchSuccessor(context.Background(), migratedBinding, migrated); err != nil {
		t.Fatalf("successor may use the host's destination version after migration: %v", err)
	}
	for _, tt := range []struct {
		name   string
		change func(*transition.SuccessorRequest[string, string, string, int])
	}{
		{"origin commit", func(s *transition.SuccessorRequest[string, string, string, int]) { s.Origin.OperationID = "another" }},
		{"revision ABA", func(s *transition.SuccessorRequest[string, string, string, int]) {
			s.OriginRevision = []byte("revision-3")
		}},
		{"entity", func(s *transition.SuccessorRequest[string, string, string, int]) { s.Request.Entity = "other" }},
		{"request key", func(s *transition.SuccessorRequest[string, string, string, int]) { s.Request.Key = "other" }},
		{"source", func(s *transition.SuccessorRequest[string, string, string, int]) { s.Request.Previous = "other" }},
		{"event", func(s *transition.SuccessorRequest[string, string, string, int]) { s.Request.Event = "other" }},
		{"input", func(s *transition.SuccessorRequest[string, string, string, int]) { s.Request.Input++ }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			changed := successor
			tt.change(&changed)
			if err := f.acceptor.MatchSuccessor(context.Background(), binding, changed); !errors.Is(err, transition.ErrConflict) {
				t.Fatalf("changed semantics must conflict: %v", err)
			}
		})
	}
}
