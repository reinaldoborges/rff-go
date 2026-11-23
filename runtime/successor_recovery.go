package runtime

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/reinaldoborges/rff-go/internal/nilcheck"
)

var (
	ErrSuccessorPending = errors.New("successor acceptance is unresolved")
	ErrSuccessorDenied  = errors.New("successor acceptance requires intervention")
)

type SuccessorHostOutcome string

const (
	SuccessorHostAbsent     SuccessorHostOutcome = "absent"
	SuccessorHostPending    SuccessorHostOutcome = "pending"
	SuccessorHostAccepted   SuccessorHostOutcome = "accepted"
	SuccessorHostDenied     SuccessorHostOutcome = "denied"
	SuccessorHostCanceled   SuccessorHostOutcome = "cancelled"
	SuccessorHostSuperseded SuccessorHostOutcome = "superseded"
)

// SuccessorHostResult is authoritative evidence from the host, not an
// inference from a timeout, lease expiry, or a failed lookup.
type SuccessorHostResult[EntityID comparable, StateID comparable] struct {
	Outcome      SuccessorHostOutcome
	Receipt      *SuccessorReceipt[EntityID, StateID]
	Cancellation *SuccessorCancellation[EntityID, StateID]
	Principal    string
}

// SuccessorHost owns authentication, authorization, non-reused revisions,
// and the fence enforced by every FSM-state writer. Lookup is an internal
// recovery operation and must not require permission to disclose the result.
// Accept atomically reconciles the identity before checking the origin,
// authenticates and authorizes new work, then records its receipt and fence.
// Absence must not be reported when an acceptance outcome is uncertain.
// Principals are safe audit identifiers, not credentials or sensitive details.
type SuccessorHost[EntityID comparable, StateID comparable] interface {
	LookupSuccessor(context.Context, SuccessorBinding[EntityID, StateID]) (SuccessorHostResult[EntityID, StateID], error)
	AcceptSuccessor(context.Context, SuccessorBinding[EntityID, StateID]) (SuccessorHostResult[EntityID, StateID], error)
}

type SuccessorRecoveryStore[EntityID comparable, StateID comparable] interface {
	InspectSuccessor(context.Context, ContinuationMarker[EntityID, StateID]) (SuccessorInspection[EntityID, StateID], error)
	FinalizeSuccessor(context.Context, SuccessorReceipt[EntityID, StateID]) (Execution[EntityID, StateID], error)
	CancelSuccessor(context.Context, SuccessorCancellation[EntityID, StateID]) error
	RecordSuccessorDenial(context.Context, SuccessorBinding[EntityID, StateID], string) error
}

// SuccessorRecovery reconciles uncertain host acceptance without disclosing
// protected execution data.
type SuccessorRecovery[EntityID comparable, StateID comparable] struct {
	store SuccessorRecoveryStore[EntityID, StateID]
	host  SuccessorHost[EntityID, StateID]
}

func NewSuccessorRecovery[EntityID comparable, StateID comparable](
	store SuccessorRecoveryStore[EntityID, StateID], host SuccessorHost[EntityID, StateID],
) (*SuccessorRecovery[EntityID, StateID], error) {
	if nilcheck.IsNil(store) || nilcheck.IsNil(host) {
		return nil, ErrContinuationUnavailable
	}
	return &SuccessorRecovery[EntityID, StateID]{store: store, host: host}, nil
}

// Reconcile finalizes previously accepted work even when allowNew is false.
// The caller must enforce continuation policy before setting allowNew.
func (r *SuccessorRecovery[EntityID, StateID]) Reconcile(
	ctx context.Context, marker ContinuationMarker[EntityID, StateID], allowNew bool,
) error {
	inspection, err := r.store.InspectSuccessor(ctx, marker)
	if err != nil {
		return err
	}
	switch inspection.Phase {
	case SuccessorAccepted:
		return nil
	case SuccessorCancelled:
		return ErrSuccessorCancelled
	case SuccessorSuperseded:
		return ErrSuccessorSuperseded
	case SuccessorPrepared, SuccessorDenied:
	default:
		return ErrCompatibility
	}
	result, err := r.host.LookupSuccessor(ctx, inspection.Binding)
	if err != nil {
		return fmt.Errorf("lookup successor: %w", err)
	}
	if result.Outcome == SuccessorHostAbsent {
		if inspection.Phase == SuccessorDenied {
			return ErrSuccessorDenied
		}
		if !allowNew {
			return ErrSuccessorPending
		}
		result, err = r.host.AcceptSuccessor(ctx, inspection.Binding)
		if err != nil {
			return fmt.Errorf("accept successor: %w", err)
		}
	}
	switch result.Outcome {
	case SuccessorHostAccepted:
		if result.Receipt == nil || result.Cancellation != nil {
			return ErrCompatibility
		}
		if !reflect.DeepEqual(result.Receipt.Binding, inspection.Binding) {
			return ErrConflict
		}
		_, err := r.store.FinalizeSuccessor(ctx, *result.Receipt)
		return err
	case SuccessorHostCanceled, SuccessorHostSuperseded:
		if result.Cancellation == nil || result.Receipt != nil {
			return ErrCompatibility
		}
		if result.Cancellation.Superseded != (result.Outcome == SuccessorHostSuperseded) {
			return ErrCompatibility
		}
		if !reflect.DeepEqual(result.Cancellation.Binding, inspection.Binding) {
			return ErrConflict
		}
		if err := r.store.CancelSuccessor(ctx, *result.Cancellation); err != nil {
			return err
		}
		if result.Cancellation.Superseded {
			return ErrSuccessorSuperseded
		}
		return ErrSuccessorCancelled
	case SuccessorHostDenied:
		if result.Principal == "" || result.Receipt != nil || result.Cancellation != nil {
			return ErrCompatibility
		}
		if err := r.store.RecordSuccessorDenial(ctx, inspection.Binding, result.Principal); err != nil {
			return err
		}
		return ErrSuccessorDenied
	case SuccessorHostPending, SuccessorHostAbsent:
		return ErrSuccessorPending
	default:
		return ErrCompatibility
	}
}
