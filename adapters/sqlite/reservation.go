package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

func releaseReservation(ctx context.Context, tx *sql.Tx, id string) error {
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
	return nil
}
