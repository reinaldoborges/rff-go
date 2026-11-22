package runtime

import (
	"context"
	"errors"
)

// ErrExecutionNotActive indicates that no active execution has the given ID.
var ErrExecutionNotActive = errors.New("execution is not active")

// ActiveExecutionReader allows reconciliation to discover persisted work.
// Enumeration returns IDs without decoding application state so an
// incompatible execution does not prevent discovery of other work.
type ActiveExecutionReader[EntityID comparable, StateID comparable] interface {
	ActiveExecutionIDs(context.Context) ([]string, error)
	ReadActiveExecution(context.Context, string) (Execution[EntityID, StateID], error)
}
