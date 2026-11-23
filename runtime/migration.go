package runtime

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	"github.com/reinaldoborges/rff-go/internal/nilcheck"
)

// OriginalRequest retains the acceptance identity when executable data migrates.
type OriginalRequest struct {
	Operation            Operation
	MachineID            string
	MachineVersion       string
	Event                EncodedValue
	Input                EncodedValue
	NotificationOverride NotificationOverride
}

// MigrationRequest identifies an authorized conversion and its stable parameters.
type MigrationRequest struct {
	ID             string
	MachineID      string
	MachineVersion string
	Parameters     []byte
}

// MigrationReceipt records the conversion identity; request parameters are
// omitted and represented by ParametersDigest for idempotent comparison.
type MigrationReceipt struct {
	Request          MigrationRequest
	ParametersDigest string
	AcceptedAt       time.Time
}

// MigrationSource is the persisted representation supplied to a converter.
// State bytes are supplied without decoding so unavailable old state behavior
// does not prevent conversion. The host owns the encoding and its versions.
type MigrationSource[EntityID comparable] struct {
	ExecutionID    string
	Entity         EntityID
	Operation      Operation
	MachineID      string
	MachineVersion string
	Previous       []byte
	HasPrevious    bool
	Target         []byte
	Event          EncodedValue
	Input          EncodedValue
	Policy         fsm.Policy
	Operations     []OperationInspection
}

// MigrationSnapshot is the executable behavior after conversion.
type MigrationSnapshot[StateID comparable] struct {
	MachineID      string
	MachineVersion string
	Previous       StateID
	HasPrevious    bool
	Target         StateID
	Event          EncodedValue
	Input          EncodedValue
	Policy         fsm.Policy
}

// MigrationStore applies conversion and validation atomically under a fence.
// Callbacks must be side-effect-free and must not re-enter the store.
type MigrationStore[EntityID comparable, StateID comparable] interface {
	ApplyExecutionMigration(
		context.Context, TransitionLease[EntityID], func() time.Time, MigrationRequest,
		func(context.Context, MigrationSource[EntityID]) (MigrationSnapshot[StateID], error),
		func(context.Context, MigrationSource[EntityID], MigrationSnapshot[StateID], Execution[EntityID, StateID], HookCall) error,
	) (MigrationReceipt, error)
}

// ExecutionMigrator coordinates authorized conversion and destination validation.
// The host authorizes and audits requests, coordinates domain changes, and
// verifies the converted source against authoritative state. The optional
// operation verifier is mandatory when any tracked operation exists; it must
// prove the destination can honor every original binding and outcome.
type ExecutionMigrator[EntityID comparable, StateID comparable, Event, Input any] struct {
	store            MigrationStore[EntityID, StateID]
	runner           *HookRunner[EntityID, StateID, Event, Input]
	convert          func(context.Context, MigrationSource[EntityID], MigrationRequest) (MigrationSnapshot[StateID], error)
	verify           func(context.Context, MigrationSource[EntityID], MigrationSnapshot[StateID]) error
	verifyOperations func(context.Context, MigrationSource[EntityID], MigrationSnapshot[StateID]) error
}

func NewExecutionMigrator[EntityID comparable, StateID comparable, Event, Input any](
	store MigrationStore[EntityID, StateID], runner *HookRunner[EntityID, StateID, Event, Input],
	convert func(context.Context, MigrationSource[EntityID], MigrationRequest) (MigrationSnapshot[StateID], error),
	verify func(context.Context, MigrationSource[EntityID], MigrationSnapshot[StateID]) error,
	verifyOperations ...func(context.Context, MigrationSource[EntityID], MigrationSnapshot[StateID]) error,
) (*ExecutionMigrator[EntityID, StateID, Event, Input], error) {
	if nilcheck.IsNil(store) || runner == nil || convert == nil || verify == nil || len(verifyOperations) > 1 || (len(verifyOperations) == 1 && verifyOperations[0] == nil) {
		return nil, errors.New("migration store, runner, converter, and domain verifier are required")
	}
	migrator := &ExecutionMigrator[EntityID, StateID, Event, Input]{store: store, runner: runner, convert: convert, verify: verify}
	if len(verifyOperations) == 1 {
		migrator.verifyOperations = verifyOperations[0]
	}
	return migrator, nil
}

func cloneMigrationSource[EntityID comparable](source MigrationSource[EntityID]) MigrationSource[EntityID] {
	source.Previous = slices.Clone(source.Previous)
	source.Target = slices.Clone(source.Target)
	source.Input.Data = slices.Clone(source.Input.Data)
	source.Event.Data = slices.Clone(source.Event.Data)
	source.Policy.RetryDelays = slices.Clone(source.Policy.RetryDelays)
	source.Operations = slices.Clone(source.Operations)
	for i := range source.Operations {
		operation := &source.Operations[i]
		operation.Binding.Input = slices.Clone(operation.Binding.Input)
		if operation.Result != nil {
			result := *operation.Result
			result.Data = slices.Clone(result.Data)
			operation.Result = &result
		}
	}
	return source
}

func cloneMigrationSnapshot[StateID comparable](snapshot MigrationSnapshot[StateID]) MigrationSnapshot[StateID] {
	snapshot.Input.Data = slices.Clone(snapshot.Input.Data)
	snapshot.Event.Data = slices.Clone(snapshot.Event.Data)
	snapshot.Policy.RetryDelays = slices.Clone(snapshot.Policy.RetryDelays)
	return snapshot
}

// Migrate applies a host-authorized conversion, or replays its recorded outcome.
func (m *ExecutionMigrator[EntityID, StateID, Event, Input]) Migrate(
	ctx context.Context, lease TransitionLease[EntityID], clock func() time.Time, request MigrationRequest,
) (MigrationReceipt, error) {
	if clock == nil || request.ID == "" || request.MachineID == "" || request.MachineVersion == "" {
		return MigrationReceipt{}, ErrInvalidRequest
	}
	request.Parameters = slices.Clone(request.Parameters)
	return m.store.ApplyExecutionMigration(ctx, lease, clock, request,
		func(ctx context.Context, source MigrationSource[EntityID]) (MigrationSnapshot[StateID], error) {
			parameters := request
			parameters.Parameters = slices.Clone(request.Parameters)
			return m.convert(ctx, cloneMigrationSource(source), parameters)
		},
		func(ctx context.Context, source MigrationSource[EntityID], snapshot MigrationSnapshot[StateID], execution Execution[EntityID, StateID], call HookCall) error {
			if snapshot.MachineID != request.MachineID || snapshot.MachineVersion != request.MachineVersion {
				return fmt.Errorf("%w: migration destination differs from request", ErrInvalidDecision)
			}
			if err := validatePolicy(snapshot.Policy); err != nil {
				return err
			}
			if _, err := m.runner.prepare(ctx, execution, call); err != nil {
				return err
			}
			if err := m.verify(ctx, cloneMigrationSource(source), cloneMigrationSnapshot(snapshot)); err != nil {
				return err
			}
			if len(source.Operations) != 0 {
				if m.verifyOperations == nil {
					return fmt.Errorf("%w: operation migration verifier is unavailable", ErrCompatibility)
				}
				return m.verifyOperations(ctx, cloneMigrationSource(source), cloneMigrationSnapshot(snapshot))
			}
			return nil
		},
	)
}
