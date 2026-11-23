package runtime_test

import (
	"context"
	"errors"
	"slices"
	"sync"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

type memoryStore struct {
	mu              sync.Mutex
	records         map[requestKey]transition.Execution[string, state]
	active          map[string]string
	ids             map[string]bool
	fail            bool
	failAfterCommit bool
}

type requestKey struct {
	entity string
	key    string
}

func newMemoryStore() *memoryStore {
	return &memoryStore{
		records: make(map[requestKey]transition.Execution[string, state]),
		active:  make(map[string]string),
		ids:     make(map[string]bool),
	}
}

func (s *memoryStore) Accept(ctx context.Context, entity, key string, decide transition.AcceptanceDecisionFunc[string, state]) (transition.Execution[string, state], bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return transition.Execution[string, state]{}, false, err
	}
	record, exists := s.records[requestKey{entity: entity, key: key}]
	view := transition.AcceptanceView[string, state]{Active: s.active[entity] != ""}
	if exists {
		copy := cloneExecution(record)
		view.Existing = &copy
	}
	next, err := decide(view)
	if err != nil {
		return transition.Execution[string, state]{}, false, err
	}
	if exists {
		if next != nil {
			return transition.Execution[string, state]{}, false, transition.ErrInvalidDecision
		}
		return cloneExecution(record), false, nil
	}
	if next == nil || view.Active || next.Entity != entity || next.Key != key || next.ID == "" || s.ids[next.ID] ||
		next.SchemaVersion != transition.CurrentSchemaVersion || next.Status != transition.Waiting ||
		next.CommitOperationID != "" || next.CommitReceipt != nil || next.NotificationIntentID != "" || next.NotificationFailure != nil ||
		next.NotificationWaived || len(next.NotificationControls) != 0 ||
		next.Compatibility != nil || next.OriginalRequest != nil || len(next.Migrations) != 0 ||
		next.RecoveryStarted || next.Failure != nil || next.FailureDisposition != "" ||
		!next.CycleDeadline.IsZero() || !next.RejectionRecoveryDeadline.IsZero() || len(next.Controls) != 0 {
		return transition.Execution[string, state]{}, false, transition.ErrInvalidDecision
	}
	if s.fail {
		s.fail = false
		return transition.Execution[string, state]{}, false, errors.New("injected storage failure")
	}
	s.records[requestKey{entity: entity, key: key}] = cloneExecution(*next)
	s.active[entity] = next.ID
	s.ids[next.ID] = true
	if s.failAfterCommit {
		s.failAfterCommit = false
		return transition.Execution[string, state]{}, false, errors.New("injected lost commit response")
	}
	return cloneExecution(*next), true, nil
}

func cloneExecution(record transition.Execution[string, state]) transition.Execution[string, state] {
	if record.Continuation != nil {
		snapshot := *record.Continuation
		snapshot.Policy.RetryDelays = slices.Clone(snapshot.Policy.RetryDelays)
		record.Continuation = &snapshot
	}
	if record.OriginalRequest != nil {
		original := *record.OriginalRequest
		original.Input.Data = slices.Clone(original.Input.Data)
		original.Event.Data = slices.Clone(original.Event.Data)
		record.OriginalRequest = &original
	}
	record.Migrations = slices.Clone(record.Migrations)
	for i := range record.Migrations {
		record.Migrations[i].Request.Parameters = slices.Clone(record.Migrations[i].Request.Parameters)
	}
	if record.Compatibility != nil {
		diagnostic := *record.Compatibility
		record.Compatibility = &diagnostic
	}
	if record.NotificationFailure != nil {
		failure := *record.NotificationFailure
		record.NotificationFailure = &failure
	}
	record.NotificationControls = slices.Clone(record.NotificationControls)
	if record.CommitReceipt != nil {
		receipt := *record.CommitReceipt
		if receipt.Failure != nil {
			failure := *receipt.Failure
			receipt.Failure = &failure
		}
		record.CommitReceipt = &receipt
	}
	if record.Failure != nil {
		failure := *record.Failure
		record.Failure = &failure
	}
	record.Input.Data = slices.Clone(record.Input.Data)
	record.Event.Data = slices.Clone(record.Event.Data)
	record.Policy.RetryDelays = slices.Clone(record.Policy.RetryDelays)
	record.Controls = slices.Clone(record.Controls)
	for i := range record.Controls {
		if record.Controls[i].Failure != nil {
			failure := *record.Controls[i].Failure
			record.Controls[i].Failure = &failure
		}
	}
	return record
}
