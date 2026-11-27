package runtime

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/reinaldoborges/rff-go/fsm"
	"github.com/reinaldoborges/rff-go/internal/nilcheck"
)

// ErrContinuationPending means an opted-in commit's marker has not yet been
// durably reconciled into the runtime store.
var ErrContinuationPending = errors.New("continuation marker pending")

// ErrContinuationOriginMissing means the marker's originating execution is
// unavailable. Discovery must retain the host marker for retry.
var ErrContinuationOriginMissing = errors.New("continuation origin execution missing")

// ContinuationMarker is the host's durable evidence of an opted-in commit.
// The host must create it atomically with the successful commit and receipt.
type ContinuationMarker[EntityID comparable, StateID comparable] struct {
	SchemaVersion  uint
	ExecutionID    string
	OperationID    string
	Entity         EntityID
	Target         StateID
	MachineID      string
	MachineVersion string
	Policy         fsm.Policy
}

// ValidateContinuationMarker checks structural compatibility and the pinned
// policy. Stores must also validate the marker against its originating
// execution before making it eligible for evaluation.
func ValidateContinuationMarker[EntityID comparable, StateID comparable](marker ContinuationMarker[EntityID, StateID]) error {
	if marker.SchemaVersion != 1 {
		return fmt.Errorf("%w: unsupported continuation marker version", ErrCompatibility)
	}
	if err := ValidateDurableID(marker.ExecutionID); err != nil {
		return err
	}
	if marker.OperationID == "" || marker.MachineID == "" || marker.MachineVersion == "" {
		return fmt.Errorf("%w: incomplete continuation marker", ErrInvalidRequest)
	}
	return validatePolicy(marker.Policy)
}

// MatchContinuationMarkers rejects a replay whose identity or pinned contents
// differ. Unsupported marker versions must not be interpreted as a match.
func MatchContinuationMarkers[EntityID comparable, StateID comparable](stored, incoming ContinuationMarker[EntityID, StateID]) error {
	if err := ValidateContinuationMarker(stored); err != nil {
		return err
	}
	if err := ValidateContinuationMarker(incoming); err != nil {
		return err
	}
	if stored.SchemaVersion != incoming.SchemaVersion ||
		stored.ExecutionID != incoming.ExecutionID || stored.OperationID != incoming.OperationID ||
		stored.Entity != incoming.Entity || stored.Target != incoming.Target ||
		stored.MachineID != incoming.MachineID || stored.MachineVersion != incoming.MachineVersion ||
		stored.Policy.MaxAttempts != incoming.Policy.MaxAttempts ||
		stored.Policy.AttemptTimeout != incoming.Policy.AttemptTimeout ||
		stored.Policy.OverallTimeout != incoming.Policy.OverallTimeout ||
		!slices.Equal(stored.Policy.RetryDelays, incoming.Policy.RetryDelays) {
		return ErrConflict
	}
	return nil
}

// ContinuationMarkerSource supplies replayable pages of unacknowledged
// markers. Cursors must remain valid while earlier markers are acknowledged.
// Acknowledgement must match the complete marker, so it cannot discard a
// conflicting replacement. Unacknowledged markers must reappear in a later
// scan beginning with the empty cursor, including after process restart.
type ContinuationMarkerSource[EntityID comparable, StateID comparable] interface {
	ListContinuationMarkers(context.Context, string) ([]ContinuationMarker[EntityID, StateID], string, error)
	AcknowledgeContinuationMarker(context.Context, ContinuationMarker[EntityID, StateID]) error
}

// ContinuationMarkerStore durably records markers and detects conflicting
// replays. A nil error certifies durable persistence, including on replay.
// A missing origin is reported without acknowledging the host marker.
type ContinuationMarkerStore[EntityID comparable, StateID comparable] interface {
	RecordContinuationMarker(context.Context, ContinuationMarker[EntityID, StateID]) error
}

// ContinuationDiscovery reconciles a host's durable markers into a runtime
// store. It is a discovery substrate, not a Continuation evaluator or driver.
type ContinuationDiscovery[EntityID comparable, StateID comparable] struct {
	store ContinuationMarkerStore[EntityID, StateID]
	host  ContinuationMarkerSource[EntityID, StateID]
}

func NewContinuationDiscovery[EntityID comparable, StateID comparable](
	store ContinuationMarkerStore[EntityID, StateID], host ContinuationMarkerSource[EntityID, StateID],
) (*ContinuationDiscovery[EntityID, StateID], error) {
	if nilcheck.IsNil(store) || nilcheck.IsNil(host) {
		return nil, errors.New("continuation marker store and source are required")
	}
	return &ContinuationDiscovery[EntityID, StateID]{store: store, host: host}, nil
}

// Reconcile starts each scan at the beginning. Individual failures are
// reported but do not prevent later markers from being persisted; failed
// markers are never acknowledged and remain replayable.
func (d *ContinuationDiscovery[EntityID, StateID]) Reconcile(ctx context.Context) error {
	var result error
	cursor := ""
	seen := map[string]bool{"": true}
	for {
		if err := ctx.Err(); err != nil {
			return errors.Join(result, err)
		}
		markers, next, err := d.host.ListContinuationMarkers(ctx, cursor)
		if err != nil {
			return errors.Join(result, fmt.Errorf("list continuation markers: %w", err))
		}
		for _, marker := range markers {
			if err := ValidateContinuationMarker(marker); err != nil {
				result = errors.Join(result, fmt.Errorf("invalid continuation marker: %w", err))
				continue
			}
			stored := marker
			stored.Policy.RetryDelays = slices.Clone(marker.Policy.RetryDelays)
			if err := d.store.RecordContinuationMarker(ctx, stored); err != nil {
				result = errors.Join(result, fmt.Errorf("record continuation marker: %w", err))
				continue
			}
			if err := d.host.AcknowledgeContinuationMarker(ctx, marker); err != nil {
				result = errors.Join(result, fmt.Errorf("acknowledge continuation marker: %w", err))
			}
		}
		if next == "" {
			return result
		}
		if seen[next] {
			return errors.Join(result, errors.New("continuation marker pagination cursor repeated"))
		}
		seen[next] = true
		cursor = next
	}
}
