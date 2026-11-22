package runtime

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/reinaldoborges/rff-go/internal/nilcheck"
)

// MigrationExecutor applies a migration with its separate pre-commit gate.
type MigrationExecutor[EntityID comparable] interface {
	Migrate(context.Context, TransitionLease[EntityID], func() time.Time, MigrationRequest) (MigrationReceipt, error)
}

// AuthorizedMigration invokes host authorization before migration and replay.
// The host owns actor attribution, audit, and protection of migration parameters.
type AuthorizedMigration[EntityID comparable] struct {
	migrator  MigrationExecutor[EntityID]
	authorize func(context.Context, ControlAuthorization) error
}

func NewAuthorizedMigration[EntityID comparable](
	migrator MigrationExecutor[EntityID], authorize func(context.Context, ControlAuthorization) error,
) (*AuthorizedMigration[EntityID], error) {
	if nilcheck.IsNil(migrator) || authorize == nil {
		return nil, errors.New("migrator and host authorization are required")
	}
	return &AuthorizedMigration[EntityID]{migrator: migrator, authorize: authorize}, nil
}

func (m *AuthorizedMigration[EntityID]) Migrate(
	ctx context.Context, lease TransitionLease[EntityID], clock func() time.Time, request MigrationRequest,
) (MigrationReceipt, error) {
	if lease.ExecutionID == "" || clock == nil || request.ID == "" || request.MachineID == "" || request.MachineVersion == "" {
		return MigrationReceipt{}, ErrInvalidRequest
	}
	request.Parameters = slices.Clone(request.Parameters)
	authRequest := request
	authRequest.Parameters = slices.Clone(request.Parameters)
	if err := m.authorize(ctx, ControlAuthorization{
		ExecutionID: lease.ExecutionID, RequestID: request.ID, Action: ControlMigrate, Migration: &authRequest,
	}); err != nil {
		return MigrationReceipt{}, err
	}
	return m.migrator.Migrate(ctx, lease, clock, request)
}
