package runtime

import (
	"context"
	"errors"
	"fmt"
)

// ErrExecutionNotActive indicates that no active execution has the given ID.
var ErrExecutionNotActive = errors.New("execution is not active")

// ActiveExecutionReader allows reconciliation to discover persisted work.
// Enumeration returns IDs without decoding application state so an
// incompatible execution does not prevent discovery of other work.
type ActiveExecutionReader[EntityID comparable, StateID comparable] interface {
	ActiveExecutionIDs(context.Context) ([]string, error)
	ReadActiveExecution(context.Context, string) (Execution[EntityID, StateID], error)
	BlockUnreadableExecution(context.Context, string) error
}

func readForReconciliation[EntityID comparable, StateID comparable](
	ctx context.Context, store ActiveExecutionReader[EntityID, StateID], id string,
) (Execution[EntityID, StateID], error) {
	execution, err := store.ReadActiveExecution(ctx, id)
	var compatibility *CompatibilityError
	if errors.As(err, &compatibility) &&
		(compatibility.Diagnostic.Component == CompatibilityEntity || compatibility.Diagnostic.Component == CompatibilityState) {
		if blockErr := store.BlockUnreadableExecution(ctx, id); blockErr != nil {
			return execution, errors.Join(err, fmt.Errorf("record unreadable execution: %w", blockErr))
		}
	}
	return execution, err
}
