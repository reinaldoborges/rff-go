package runtime_test

import (
	"errors"
	"testing"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestContinuationMarkerReplayRequiresIdenticalSemantics(t *testing.T) {
	first := continuationMarker("origin")
	first.Policy.RetryDelays = []time.Duration{time.Second, time.Minute}
	tests := []struct {
		name   string
		change func(*transition.ContinuationMarker[string, string])
	}{
		{"operation", func(m *transition.ContinuationMarker[string, string]) { m.OperationID = "other" }},
		{"entity", func(m *transition.ContinuationMarker[string, string]) { m.Entity = "other" }},
		{"target", func(m *transition.ContinuationMarker[string, string]) { m.Target = "other" }},
		{"behavior", func(m *transition.ContinuationMarker[string, string]) { m.MachineVersion = "v2" }},
		{"policy", func(m *transition.ContinuationMarker[string, string]) {
			m.Policy.RetryDelays = []time.Duration{time.Minute}
		}},
	}
	if err := transition.MatchContinuationMarkers(first, first); err != nil {
		t.Fatal(err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			other := first
			tt.change(&other)
			if err := transition.MatchContinuationMarkers(first, other); !errors.Is(err, transition.ErrConflict) {
				t.Fatalf("changed marker must conflict: %v", err)
			}
		})
	}
}
