package taskmanager

import (
	"context"
	"database/sql"
	"fmt"
)

// SeedOperator registers a local test-only operator, distinct from domain users.
func (f *Fixture) SeedOperator(ctx context.Context, operator string) error {
	if operator == "" {
		return ErrInvalidRequest
	}
	return f.transaction(ctx, func(tx *sql.Tx) error {
		var user bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tm_users WHERE id = ?)`, operator).Scan(&user); err != nil {
			return fmt.Errorf("check operator identity: %w", err)
		}
		if user {
			return ErrConflict
		}
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO tm_operators (id) VALUES (?)`, operator); err != nil {
			return fmt.Errorf("seed operator: %w", err)
		}
		return nil
	})
}

// InspectMembership gives a test-only operator access to retained history.
func (f *Fixture) InspectMembership(ctx context.Context, operator, id string) (Membership, error) {
	if operator == "" || id == "" {
		return Membership{}, ErrInvalidRequest
	}
	var known bool
	if err := f.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tm_operators WHERE id = ?)`, operator).Scan(&known); err != nil {
		return Membership{}, fmt.Errorf("authenticate test operator: %w", err)
	}
	if !known {
		return Membership{}, ErrDenied
	}
	membership, _, _, err := f.readMembership(ctx, id)
	return membership, err
}
