package taskmanager

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// CreateGroup atomically creates a group and its owner's Active membership.
func (f *Fixture) CreateGroup(ctx context.Context, actor, id, ownerMembershipID string) error {
	if actor == "" || id == "" || ownerMembershipID == "" {
		return ErrInvalidRequest
	}
	return f.transaction(ctx, func(tx *sql.Tx) error {
		var known bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tm_users WHERE id = ?)`, actor).Scan(&known); err != nil {
			return fmt.Errorf("check group creator: %w", err)
		}
		if !known {
			return ErrDenied
		}
		var exists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tm_groups WHERE id = ?)
			OR EXISTS(SELECT 1 FROM tm_memberships WHERE id = ?)`, id, ownerMembershipID).Scan(&exists); err != nil {
			return fmt.Errorf("check group identity: %w", err)
		}
		if exists {
			return ErrConflict
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO tm_groups (id, owner_id, closed) VALUES (?, ?, 0)`, id, actor); err != nil {
			return fmt.Errorf("create group: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO tm_memberships (id, group_id, user_id, status, reason)
			VALUES (?, ?, ?, 'Active', '')`, ownerMembershipID, id, actor); err != nil {
			return fmt.Errorf("create owner membership: %w", err)
		}
		return nil
	})
}

// Group returns an open group only to its Active members.
func (f *Fixture) Group(ctx context.Context, actor, id string) (Group, error) {
	if actor == "" || id == "" {
		return Group{}, ErrInvalidRequest
	}

	var group Group
	err := f.db.QueryRowContext(ctx, `SELECT g.id, g.owner_id, g.closed FROM tm_groups AS g
		JOIN tm_memberships AS m ON m.group_id = g.id AND m.user_id = ? AND m.status = 'Active'
		WHERE g.id = ? AND g.closed = 0`, actor, id).Scan(&group.ID, &group.Owner, &group.Closed)
	if errors.Is(err, sql.ErrNoRows) {
		return Group{}, ErrDenied
	}
	if err != nil {
		return Group{}, fmt.Errorf("inspect group: %w", err)
	}
	return group, nil
}
