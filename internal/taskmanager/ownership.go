package taskmanager

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// OwnershipTransfer is the application-owned, durable transfer audit receipt.
type OwnershipTransfer struct {
	GroupID     string
	Actor       string
	FormerOwner string
	NewOwner    string
	At          time.Time
}

// TransferOwnership serializes authorization, ownership and audit in one transaction.
// The original seeded actor may replay an identical request even after losing access.
func (f *Fixture) TransferOwnership(ctx context.Context, actor, groupID, newOwner, key string) (receipt OwnershipTransfer, err error) {
	if actor == "" || groupID == "" || newOwner == "" || key == "" {
		return receipt, ErrInvalidRequest
	}
	err = f.transaction(ctx, func(tx *sql.Tx) error {
		var known bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tm_users WHERE id = ?)`, actor).Scan(&known); err != nil {
			return fmt.Errorf("authenticate transfer actor: %w", err)
		}
		if !known {
			return ErrDenied
		}
		var at string
		err := tx.QueryRowContext(ctx, `SELECT former_owner, new_owner, occurred_at FROM tm_ownership_transfers
			WHERE group_id = ? AND actor_id = ? AND request_key = ?`, groupID, actor, key).
			Scan(&receipt.FormerOwner, &receipt.NewOwner, &at)
		if err == nil {
			if receipt.NewOwner != newOwner {
				return ErrConflict
			}
			receipt.GroupID, receipt.Actor = groupID, actor
			receipt.At, err = time.Parse(time.RFC3339Nano, at)
			if err != nil {
				return fmt.Errorf("decode transfer audit time: %w", err)
			}
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read transfer receipt: %w", err)
		}
		var owner string
		var closed bool
		err = tx.QueryRowContext(ctx, `SELECT owner_id, closed FROM tm_groups WHERE id = ?`, groupID).Scan(&owner, &closed)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrDenied
		}
		if err != nil {
			return fmt.Errorf("read transfer group: %w", err)
		}
		if closed || actor != owner {
			return ErrDenied
		}
		if newOwner == owner {
			return ErrConflict
		}
		var active bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tm_memberships
			WHERE group_id = ? AND user_id = ? AND status = 'Active')`, groupID, newOwner).Scan(&active); err != nil {
			return fmt.Errorf("check transfer recipient: %w", err)
		}
		if !active {
			return ErrConflict
		}
		now := f.clock().UTC()
		if now.IsZero() {
			return ErrInvalidRequest
		}
		if _, err := tx.ExecContext(ctx, `UPDATE tm_groups SET owner_id = ? WHERE id = ?`, newOwner, groupID); err != nil {
			return fmt.Errorf("transfer group ownership: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO tm_ownership_transfers
			(group_id, actor_id, request_key, former_owner, new_owner, occurred_at) VALUES (?, ?, ?, ?, ?, ?)`,
			groupID, actor, key, owner, newOwner, now.Format(time.RFC3339Nano)); err != nil {
			return fmt.Errorf("record ownership transfer: %w", err)
		}
		receipt = OwnershipTransfer{GroupID: groupID, Actor: actor, FormerOwner: owner, NewOwner: newOwner, At: now}
		return nil
	})
	if err != nil {
		return OwnershipTransfer{}, err
	}
	return receipt, nil
}
