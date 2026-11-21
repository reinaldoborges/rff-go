package fsm

import (
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrDuplicateMachine indicates an identity and version is already registered.
	ErrDuplicateMachine = errors.New("machine version already registered")
	// ErrMachineNotFound indicates that an exact machine version is unavailable.
	ErrMachineNotFound = errors.New("machine version not found")
)

type machineKey struct {
	id      string
	version string
}

// Registry stores multiple versions of machines with the same Go types.
// Its zero value is ready to use.
type Registry[EntityID comparable, StateID comparable, Event, Input any] struct {
	mu       sync.RWMutex
	machines map[machineKey]*Machine[EntityID, StateID, Event, Input]
}

// Register adds one machine version without replacing an existing version.
func (r *Registry[EntityID, StateID, Event, Input]) Register(machine *Machine[EntityID, StateID, Event, Input]) error {
	if machine == nil || machine.config.ID == "" || machine.config.Version == "" {
		return fmt.Errorf("%w: machine must be created with NewMachine", ErrInvalidMachine)
	}
	key := machineKey{id: machine.config.ID, version: machine.config.Version}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.machines == nil {
		r.machines = make(map[machineKey]*Machine[EntityID, StateID, Event, Input])
	}
	if _, exists := r.machines[key]; exists {
		return ErrDuplicateMachine
	}
	r.machines[key] = machine
	return nil
}

// Lookup returns only the exact requested machine version.
func (r *Registry[EntityID, StateID, Event, Input]) Lookup(id, version string) (*Machine[EntityID, StateID, Event, Input], error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	machine, ok := r.machines[machineKey{id: id, version: version}]
	if !ok {
		return nil, ErrMachineNotFound
	}
	return machine, nil
}
