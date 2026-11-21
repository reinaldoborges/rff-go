package fsm

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"time"
)

// ErrInvalidMachine indicates that a machine configuration is incomplete.
var ErrInvalidMachine = errors.New("invalid machine configuration")

// ErrInvalidInitialState indicates that selection returned a non-initial state.
var ErrInvalidInitialState = errors.New("selected state is not an initial state")

// ErrUnknownState indicates a state has no registered handler.
var ErrUnknownState = errors.New("state is not registered")

// ErrTerminalState indicates that an ordinary transition starts at a terminal state.
var ErrTerminalState = errors.New("state is terminal")

// ErrClassifierUnavailable indicates that the machine has no failure classifier.
var ErrClassifierUnavailable = errors.New("failure classifier is unavailable")

// ErrInvalidClassification indicates a classifier returned an unsupported disposition.
var ErrInvalidClassification = errors.New("invalid failure classification")

// Policy is the application-selected policy for one transition execution.
// MaxAttempts includes the first attempt. OverallTimeout is measured from
// acceptance; the runtime must persist its resulting absolute deadline.
// Applications supply all policy values; the package provides no defaults.
type Policy struct {
	MaxAttempts    uint
	AttemptTimeout time.Duration
	OverallTimeout time.Duration
	RetryDelays    []time.Duration
}

// Selection is the result of an application selector.
type Selection[StateID comparable] struct {
	Target StateID
	Policy Policy
	Notify bool
}

// Failure is application-classified data safe to persist and pass to hooks.
type Failure struct {
	Code    string
	Message string
}

// Disposition is an application decision about a failed attempt.
type Disposition string

const (
	DispositionRetry        Disposition = "retry"
	DispositionIntervention Disposition = "intervention"
)

// Handler provides the hooks for a target state.
type Handler[EntityID comparable, StateID comparable, Event, Input any] interface {
	Enter(context.Context, ExecutionContext[EntityID, StateID, Event, Input]) error
	Transition(context.Context, ExecutionContext[EntityID, StateID, Event, Input]) error
	Retry(context.Context, ExecutionContext[EntityID, StateID, Event, Input], Failure) error
	Recover(context.Context, ExecutionContext[EntityID, StateID, Event, Input], Failure) error
}

// NoopHandler provides explicit no-op implementations for unused hooks.
type NoopHandler[EntityID comparable, StateID comparable, Event, Input any] struct{}

func (NoopHandler[EntityID, StateID, Event, Input]) Enter(context.Context, ExecutionContext[EntityID, StateID, Event, Input]) error {
	return nil
}

func (NoopHandler[EntityID, StateID, Event, Input]) Transition(context.Context, ExecutionContext[EntityID, StateID, Event, Input]) error {
	return nil
}

func (NoopHandler[EntityID, StateID, Event, Input]) Retry(context.Context, ExecutionContext[EntityID, StateID, Event, Input], Failure) error {
	return nil
}

func (NoopHandler[EntityID, StateID, Event, Input]) Recover(context.Context, ExecutionContext[EntityID, StateID, Event, Input], Failure) error {
	return nil
}

// Config defines the application behavior for one machine version. Selectors
// and Reopens must be synchronous and side-effect-free. A classifier must
// return only failure data safe to persist. An absent classifier is reported
// when classification is requested.
type Config[EntityID comparable, StateID comparable, Event, Input any] struct {
	ID               string
	Version          string
	InitialStates    []StateID
	TerminalStates   []StateID
	Handlers         map[StateID]Handler[EntityID, StateID, Event, Input]
	SelectInitial    func(Input) (Selection[StateID], error)
	SelectTransition func(StateID, Event, Input) (Selection[StateID], error)
	// Reopens explicitly identifies allowed transitions from terminal states.
	Reopens         func(StateID, Event, Input) bool
	ClassifyFailure func(error) (Failure, Disposition)
}

// Machine selects transitions without executing hooks or committing state.
type Machine[EntityID comparable, StateID comparable, Event, Input any] struct {
	config Config[EntityID, StateID, Event, Input]
}

// NewMachine creates one version of an application state machine.
func NewMachine[EntityID comparable, StateID comparable, Event, Input any](config Config[EntityID, StateID, Event, Input]) (*Machine[EntityID, StateID, Event, Input], error) {
	if config.ID == "" || config.Version == "" {
		return nil, fmt.Errorf("%w: ID and version are required", ErrInvalidMachine)
	}
	if len(config.InitialStates) == 0 {
		return nil, fmt.Errorf("%w: at least one initial state is required", ErrInvalidMachine)
	}
	if config.SelectInitial == nil || config.SelectTransition == nil {
		return nil, fmt.Errorf("%w: both selectors are required", ErrInvalidMachine)
	}
	for _, handler := range config.Handlers {
		if handler == nil {
			return nil, fmt.Errorf("%w: state handler is nil", ErrInvalidMachine)
		}
		value := reflect.ValueOf(handler)
		switch value.Kind() {
		case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer, reflect.Slice:
			if value.IsNil() {
				return nil, fmt.Errorf("%w: state handler is nil", ErrInvalidMachine)
			}
		}
	}
	for _, initial := range config.InitialStates {
		if _, ok := config.Handlers[initial]; !ok {
			return nil, fmt.Errorf("%w: initial state is not registered", ErrInvalidMachine)
		}
	}
	for _, terminal := range config.TerminalStates {
		if _, ok := config.Handlers[terminal]; !ok {
			return nil, fmt.Errorf("%w: terminal state is not registered", ErrInvalidMachine)
		}
	}
	config.InitialStates = slices.Clone(config.InitialStates)
	config.TerminalStates = slices.Clone(config.TerminalStates)
	config.Handlers = maps.Clone(config.Handlers)
	return &Machine[EntityID, StateID, Event, Input]{config: config}, nil
}

// ID returns the stable application-owned machine identifier.
func (m *Machine[EntityID, StateID, Event, Input]) ID() string {
	return m.config.ID
}

// Version returns the version pinning the machine's application behavior.
func (m *Machine[EntityID, StateID, Event, Input]) Version() string {
	return m.config.Version
}

// SelectInitial selects the initial state for a new entity.
func (m *Machine[EntityID, StateID, Event, Input]) SelectInitial(input Input) (Selection[StateID], error) {
	selection, err := m.config.SelectInitial(input)
	if err != nil {
		return Selection[StateID]{}, err
	}
	for _, initial := range m.config.InitialStates {
		if selection.Target == initial {
			selection.Policy.RetryDelays = slices.Clone(selection.Policy.RetryDelays)
			return selection, nil
		}
	}
	return Selection[StateID]{}, ErrInvalidInitialState
}

// Select selects a transition from the committed source state.
func (m *Machine[EntityID, StateID, Event, Input]) Select(source StateID, event Event, input Input) (Selection[StateID], error) {
	if _, ok := m.config.Handlers[source]; !ok {
		return Selection[StateID]{}, ErrUnknownState
	}
	if slices.Contains(m.config.TerminalStates, source) && (m.config.Reopens == nil || !m.config.Reopens(source, event, input)) {
		return Selection[StateID]{}, ErrTerminalState
	}
	selection, err := m.config.SelectTransition(source, event, input)
	if err != nil {
		return Selection[StateID]{}, err
	}
	if _, ok := m.config.Handlers[selection.Target]; !ok {
		return Selection[StateID]{}, ErrUnknownState
	}
	selection.Policy.RetryDelays = slices.Clone(selection.Policy.RetryDelays)
	return selection, nil
}

// HandlerFor returns the handler registered for the target state.
func (m *Machine[EntityID, StateID, Event, Input]) HandlerFor(target StateID) (Handler[EntityID, StateID, Event, Input], error) {
	handler, ok := m.config.Handlers[target]
	if !ok {
		return nil, ErrUnknownState
	}
	return handler, nil
}

// HasFailureClassifier reports whether the pinned behavior can classify failures.
func (m *Machine[EntityID, StateID, Event, Input]) HasFailureClassifier() bool {
	return m.config.ClassifyFailure != nil
}

// ClassifyFailure converts a live error into application-provided safe data.
func (m *Machine[EntityID, StateID, Event, Input]) ClassifyFailure(err error) (Failure, Disposition, error) {
	if m.config.ClassifyFailure == nil {
		return Failure{}, "", ErrClassifierUnavailable
	}
	if err == nil {
		return Failure{}, "", ErrInvalidClassification
	}
	failure, disposition := m.config.ClassifyFailure(err)
	if disposition != DispositionRetry && disposition != DispositionIntervention {
		return Failure{}, "", ErrInvalidClassification
	}
	return failure, disposition, nil
}
