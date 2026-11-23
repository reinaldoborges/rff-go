package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/reinaldoborges/rff-go/internal/continuationrecord"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func releaseReservation(ctx context.Context, tx *sql.Tx, id string, now time.Time) error {
	result, err := tx.ExecContext(ctx, `DELETE FROM rff_active WHERE execution_id = ?`, id)
	if err != nil {
		return fmt.Errorf("release execution reservation: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect released reservation: %w", err)
	}
	if count != 1 {
		return transition.ErrStaleLease
	}
	digest := continuationrecord.Digest(id)
	var data []byte
	err = tx.QueryRowContext(ctx, `SELECT record FROM rff_continuations WHERE digest = ?`, digest[:]).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	data, err = continuationrecord.StartCycle(data, now)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE rff_continuations SET record = ? WHERE digest = ?`, data, digest[:])
	if err != nil {
		return err
	}
	return nil
}
