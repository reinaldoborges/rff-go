package runtime

import (
	"context"
	"errors"
	"fmt"

	"github.com/reinaldoborges/rff-go/fsm"
	"github.com/reinaldoborges/rff-go/internal/nilcheck"
)

// ContinuationLifecycleStore inventories all durable intents, including those
// persisted atomically with a host commit. Inventory must include nonterminal
// intents after restart; eligibility and fencing remain the driver's job.
type ContinuationLifecycleStore[E comparable, S comparable] interface {
	ContinuationDriveStore[E, S]
	ContinuationMarkerStore[E, S]
	ContinuationMarkers(context.Context) ([]ContinuationMarker[E, S], error)
}

// EnableContinuation installs the optional durable progress path before Start.
// source is required for separate-store commits and must be nil only when all
// intents are persisted atomically in store. Host acceptance remains authorized
// and revision-fenced by config.Host.
func (l *Lifecycle[E, S, Event, Input]) EnableContinuation(
	store ContinuationLifecycleStore[E, S], machines *fsm.Registry[E, S, Event, Input],
	config ContinuationDriverConfig[E, S, Event, Input], source ContinuationMarkerSource[E, S], owner string,
) error {
	if nilcheck.IsNil(store) || owner == "" || config.Acceptor == nil || nilcheck.IsNil(config.Host) ||
		config.OriginRevision == nil || config.LeaseDuration <= l.interval {
		return ErrInvalidRequest
	}
	host, ok := l.commitHost.(ContinuationCommitHost)
	if !ok || !host.ContinuationCommitSupported() {
		return ErrContinuationUnavailable
	}
	driver, err := NewContinuationDriver(store, machines, config)
	if err != nil {
		return err
	}
	var discovery *ContinuationDiscovery[E, S]
	if !nilcheck.IsNil(source) {
		discovery, err = NewContinuationDiscovery(store, source)
		if err != nil {
			return err
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.started {
		return errors.New("lifecycle already started")
	}
	for _, path := range l.paths {
		if path.name == "continuation" {
			return errors.New("continuation already enabled")
		}
	}
	wake := make(chan struct{}, 1)
	l.paths = append(l.paths, &lifecyclePath{
		name: "continuation", wake: wake,
		status: PathStatus{Name: "continuation"},
		step: func(ctx context.Context, checked func()) error {
			var result error
			if discovery != nil {
				result = discovery.Reconcile(ctx)
			}
			markers, err := store.ContinuationMarkers(ctx)
			if err != nil {
				return errors.Join(result, fmt.Errorf("list continuations: %w", err))
			}
			if checked == nil {
				select {
				case wake <- struct{}{}:
				default:
				}
				return result
			}
			for _, marker := range markers {
				if err := ctx.Err(); err != nil {
					return errors.Join(result, err)
				}
				err := driver.Drive(ctx, marker, owner)
				if continuationWaiting(err) {
					continue
				}
				if err != nil {
					result = errors.Join(result, fmt.Errorf("drive continuation: %w", err))
				}
			}
			return result
		},
	})
	return nil
}

func continuationWaiting(err error) bool {
	if err == nil {
		return true
	}
	for _, signal := range []error{ErrContinuationPending, ErrContinuationNotDue, ErrSuccessorPending,
		ErrBusy, ErrLeaseHeld, ErrStaleLease, ErrSuccessorSuperseded, ErrSuccessorCancelled} {
		if onlyPathSignal(err, signal) {
			return true
		}
	}
	return false
}
