package taskmanager

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// CloseGroup revokes pending invitations and denies further group access.
func (f *Fixture) CloseGroup(ctx context.Context, actor, id string) error {
	if actor == "" || id == "" {
		return ErrInvalidRequest
	}
	return f.transaction(ctx, func(tx *sql.Tx) error {
		var owner string
		var closed bool
		err := tx.QueryRowContext(ctx, `SELECT owner_id, closed FROM tm_groups WHERE id = ?`, id).Scan(&owner, &closed)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrDenied
		}
		if err != nil {
			return fmt.Errorf("read group for closure: %w", err)
		}
		if closed || owner != actor {
			return ErrDenied
		}
		if _, err := tx.ExecContext(ctx, `UPDATE tm_groups SET closed = 1 WHERE id = ?`, id); err != nil {
			return fmt.Errorf("close group: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE tm_memberships SET status = 'Revoked', reason = 'group-closed'
			WHERE group_id = ? AND status = 'Invited'`, id); err != nil {
			return fmt.Errorf("revoke group invitations: %w", err)
		}
		return nil
	})
}
