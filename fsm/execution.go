package fsm

import (
	"errors"
	"fmt"
)

// ErrInvalidContext indicates inconsistent hook context data.
var ErrInvalidContext = errors.New("invalid execution context")

// Optional distinguishes an absent value from a present zero value.
type Optional[T any] struct {
	value   T
	present bool
}

// Some constructs a present value.
func Some[T any](value T) Optional[T] {
	return Optional[T]{value: value, present: true}
}

// None constructs an absent value.
func None[T any]() Optional[T] {
	return Optional[T]{}
}

// AttemptMetadata identifies one worker attempt.
type AttemptMetadata struct {
	ExecutionID            string
	Number                 uint
	LeaseGeneration        uint64
	RetryCycle             uint
	ActivityID             string
	ActivityIdempotencyKey string
}

// ExecutionContext contains the accepted data passed to a state hook.
// Applications must treat referenced values in entity, state, event, and input
// data as immutable for the life of the execution.
type ExecutionContext[EntityID comparable, StateID comparable, Event, Input any] struct {
	entityID       EntityID
	previous       Optional[StateID]
	next           StateID
	event          Optional[Event]
	input          Input
	idempotencyKey string
	attempt        AttemptMetadata
}

// NewExecutionContext creates a hook context for initialization or a transition.
func NewExecutionContext[EntityID comparable, StateID comparable, Event, Input any](
	entityID EntityID, previous Optional[StateID], next StateID, event Optional[Event],
	input Input, idempotencyKey string, attempt AttemptMetadata,
) (ExecutionContext[EntityID, StateID, Event, Input], error) {
	if previous.present != event.present {
		return ExecutionContext[EntityID, StateID, Event, Input]{}, fmt.Errorf("%w: previous and event must both be present or absent", ErrInvalidContext)
	}
	if idempotencyKey == "" {
		return ExecutionContext[EntityID, StateID, Event, Input]{}, fmt.Errorf("%w: idempotency key is required", ErrInvalidContext)
	}
	return ExecutionContext[EntityID, StateID, Event, Input]{
		entityID: entityID, previous: previous, next: next, event: event,
		input: input, idempotencyKey: idempotencyKey, attempt: attempt,
	}, nil
}

func (e ExecutionContext[EntityID, StateID, Event, Input]) EntityID() EntityID {
	return e.entityID
}

func (e ExecutionContext[EntityID, StateID, Event, Input]) Previous() (StateID, bool) {
	return e.previous.value, e.previous.present
}

func (e ExecutionContext[EntityID, StateID, Event, Input]) Next() StateID {
	return e.next
}

func (e ExecutionContext[EntityID, StateID, Event, Input]) Event() (Event, bool) {
	return e.event.value, e.event.present
}

func (e ExecutionContext[EntityID, StateID, Event, Input]) Input() Input {
	return e.input
}

func (e ExecutionContext[EntityID, StateID, Event, Input]) IdempotencyKey() string {
	return e.idempotencyKey
}

func (e ExecutionContext[EntityID, StateID, Event, Input]) Attempt() AttemptMetadata {
	return e.attempt
}
