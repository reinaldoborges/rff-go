package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"

	"github.com/reinaldoborges/rff-go/internal/continuationrecord"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

// AcceptSuccessorAtomic atomically reserves and accepts a successor together
// with the host's origin check, authorization, receipt and writer fence. The
// callback may use only tx for host writes; it must not commit, retain or
// re-enter the store. A pre-existing separate-store prepare remains pending.
func (s *Store[EntityID, StateID]) AcceptSuccessorAtomic(
	ctx context.Context,
	marker transition.ContinuationMarker[EntityID, StateID],
	decide func(*transition.SuccessorBinding[EntityID, StateID]) (*transition.SuccessorBinding[EntityID, StateID], error),
	accept func(context.Context, *sql.Tx, transition.SuccessorBinding[EntityID, StateID]) (transition.SuccessorReceipt[EntityID, StateID], error),
) (execution transition.Execution[EntityID, StateID], created bool, err error) {
	if accept == nil {
		return execution, false, transition.ErrInvalidRequest
	}
	return s.AcceptSuccessorAtomicDecision(ctx, marker, decide, func(ctx context.Context, tx *sql.Tx, binding transition.SuccessorBinding[EntityID, StateID]) (transition.SuccessorHostResult[EntityID, StateID], error) {
		receipt, err := accept(ctx, tx, binding)
		return transition.SuccessorHostResult[EntityID, StateID]{Outcome: transition.SuccessorHostAccepted, Receipt: &receipt}, err
	})
}

// AcceptSuccessorAtomicDecision also persists definitive denial and
// supersession. Host writes and decision evidence use the same transaction.
// A definitive nonacceptance must not leave host state fenced or accepted;
// errors roll the entire transaction back.
func (s *Store[EntityID, StateID]) AcceptSuccessorAtomicDecision(
	ctx context.Context,
	marker transition.ContinuationMarker[EntityID, StateID],
	decide func(*transition.SuccessorBinding[EntityID, StateID]) (*transition.SuccessorBinding[EntityID, StateID], error),
	accept func(context.Context, *sql.Tx, transition.SuccessorBinding[EntityID, StateID]) (transition.SuccessorHostResult[EntityID, StateID], error),
) (execution transition.Execution[EntityID, StateID], created bool, err error) {
	if decide == nil || accept == nil {
		return execution, false, transition.ErrInvalidRequest
	}
	var decisionError error
	err = s.hookTransaction(ctx, func(tx *sql.Tx) error {
		binding, isNew, err := s.prepareSuccessor(ctx, tx, marker, decide)
		if err != nil {
			return err
		}
		var receipt transition.SuccessorReceipt[EntityID, StateID]
		if isNew {
			result, err := accept(ctx, tx, binding)
			if err != nil {
				return err
			}
			switch result.Outcome {
			case transition.SuccessorHostDenied:
				if result.Receipt != nil || result.Cancellation != nil {
					return transition.ErrCompatibility
				}
				if err := s.recordSuccessorDenial(ctx, tx, binding, result.Principal); err != nil {
					return err
				}
				decisionError = transition.ErrSuccessorDenied
				return nil
			case transition.SuccessorHostCanceled, transition.SuccessorHostSuperseded:
				if result.Receipt != nil || result.Cancellation == nil || !reflect.DeepEqual(result.Cancellation.Binding, binding) ||
					result.Cancellation.Superseded != (result.Outcome == transition.SuccessorHostSuperseded) {
					return transition.ErrConflict
				}
				if err := s.cancelSuccessor(ctx, tx, *result.Cancellation); err != nil {
					return err
				}
				decisionError = transition.ErrSuccessorCancelled
				if result.Cancellation.Superseded {
					decisionError = transition.ErrSuccessorSuperseded
				}
				return nil
			case transition.SuccessorHostAccepted:
				if result.Receipt == nil || result.Cancellation != nil {
					return transition.ErrCompatibility
				}
				receipt = *result.Receipt
			default:
				return transition.ErrSuccessorPending
			}
			digest := continuationrecord.Digest(marker.ExecutionID)
			var entity, data []byte
			if err := tx.QueryRowContext(ctx, `SELECT entity, record FROM rff_successors WHERE digest = ?`, digest[:]).Scan(&entity, &data); err != nil {
				return err
			}
			stored, err := s.decodeSuccessor(digest[:], entity, data)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(receipt.Binding, stored) {
				return transition.ErrConflict
			}
		} else {
			digest := continuationrecord.Digest(marker.ExecutionID)
			var data []byte
			var phase string
			if err := tx.QueryRowContext(ctx, `SELECT phase, record FROM rff_successors WHERE digest = ?`, digest[:]).Scan(&phase, &data); err != nil {
				return err
			}
			if phase == "denied" {
				return transition.ErrSuccessorDenied
			}
			if phase != "accepted" {
				return transition.ErrSuccessorPending
			}
			var record successorRecord
			if err := json.Unmarshal(data, &record); err != nil {
				return ErrUnsupportedSchema
			}
			receipt = transition.SuccessorReceipt[EntityID, StateID]{
				Binding: binding, Principal: string(record.Principal), Decision: string(record.Decision), AcceptedAt: record.AcceptedAt,
			}
		}
		execution, err = s.finalizeSuccessor(ctx, tx, receipt)
		if err != nil {
			return err
		}
		created = isNew
		return nil
	})
	if err != nil {
		return transition.Execution[EntityID, StateID]{}, false, err
	}
	if decisionError != nil {
		return transition.Execution[EntityID, StateID]{}, false, decisionError
	}
	return execution, created, nil
}
