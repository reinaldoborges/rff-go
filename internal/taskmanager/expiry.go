package taskmanager

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

func expiryTimerID(membershipID string) string { return "taskmanager-expiry:" + membershipID }

func (f *Fixture) driveExpiryTimers(ctx context.Context) error {
	rows, err := f.db.QueryContext(ctx, `SELECT m.id, m.deadline FROM tm_memberships AS m
		JOIN tm_groups AS g ON g.id = m.group_id WHERE m.status = 'Invited' AND g.closed = 0 ORDER BY m.id`)
	if err != nil {
		return fmt.Errorf("list pending invitations: %w", err)
	}
	type scheduled struct {
		id       string
		deadline time.Time
	}
	var invitations []scheduled
	for rows.Next() {
		var id, text string
		if err := rows.Scan(&id, &text); err != nil {
			return errors.Join(fmt.Errorf("read invitation deadline: %w", err), rows.Close())
		}
		deadline, err := time.Parse(time.RFC3339Nano, text)
		if err != nil {
			return errors.Join(fmt.Errorf("decode invitation deadline: %w", err), rows.Close())
		}
		invitations = append(invitations, scheduled{id: id, deadline: deadline})
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return fmt.Errorf("list pending invitations: %w", err)
	}
	scheduler, err := transition.NewTimerScheduler[string](f.store, resolveMembershipEvent)
	if err != nil {
		return err
	}
	for _, invitation := range invitations {
		if _, _, err := scheduler.Schedule(ctx, transition.TimerRequest[string]{
			ID: expiryTimerID(invitation.id), WorkflowID: invitation.id, Deadline: invitation.deadline,
			Event: invitation.id, EventCodec: membershipEventCodec,
		}); err != nil {
			return fmt.Errorf("schedule invitation expiry: %w", err)
		}
	}
	worker, err := transition.NewTimerWorker[string](f.store, resolveMembershipEvent, f.deliverExpiry,
		transition.TimerWorkerConfig{ID: "taskmanager-timer", Clock: fixtureClock{now: f.clock},
			LeaseTTL: time.Minute, RenewalInterval: 10 * time.Second})
	if err != nil {
		return err
	}
	blocked, err := f.store.BlockedTimers(ctx)
	if err != nil {
		return err
	}
	seen := make(map[string]bool, len(blocked))
	for _, timer := range blocked {
		seen[timer.ID] = true
	}
	var compatibilityErr error
	for {
		err := worker.RunOne(ctx)
		if errors.Is(err, transition.ErrNoDueTimer) {
			return compatibilityErr
		}
		if err != nil {
			compatibilityErr = errors.Join(compatibilityErr, err)
			if !onlyCompatibility(err) || ctx.Err() != nil {
				return compatibilityErr
			}
			blocked, listErr := f.store.BlockedTimers(ctx)
			if listErr != nil {
				return errors.Join(compatibilityErr, listErr)
			}
			// Retry only when a newly blocked timer can no longer starve other claims.
			newBlock := false
			for _, timer := range blocked {
				if !seen[timer.ID] {
					seen[timer.ID] = true
					newBlock = true
				}
			}
			if !newBlock {
				return compatibilityErr
			}
		}
	}
}

func (f *Fixture) deliverExpiry(ctx context.Context, delivery transition.TimerDelivery[string]) error {
	if delivery.ID != expiryTimerID(delivery.WorkflowID) || delivery.Event != delivery.WorkflowID {
		return transition.ErrInvalidRequest
	}
	var input fixtureInput
	var deadline sql.NullString
	var status MembershipStatus
	err := f.db.QueryRowContext(ctx, `SELECT group_id, user_id, status, deadline FROM tm_memberships WHERE id = ?`, delivery.WorkflowID).
		Scan(&input.GroupID, &input.UserID, &status, &deadline)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("read expiry membership: %w", err)
	}
	if status != Invited {
		return nil
	}
	if !deadline.Valid {
		return errors.New("invited membership has no expiry deadline")
	}
	input.Deadline, err = time.Parse(time.RFC3339Nano, deadline.String)
	if err != nil {
		return fmt.Errorf("decode expiry deadline: %w", err)
	}
	if !input.Deadline.Equal(delivery.Deadline) {
		return ErrConflict
	}
	input.MembershipID, input.Event = delivery.WorkflowID, expireInvitation
	ingress, err := f.decisionIngress()
	if err != nil {
		return err
	}
	outcome, err := ingress.Submit(ctx, transition.InputRequest[fixtureInput]{
		ID: delivery.ID, WorkflowID: input.MembershipID, Kind: transition.ExternalEvent, Codec: membershipInputCodec, Value: input,
	})
	if err != nil {
		return err
	}
	if outcome.Failure != nil && outcome.Failure.Code != "conflict" && outcome.Failure.Code != "denied" {
		return fmt.Errorf("expiry input rejected: %s", outcome.Failure.Code)
	}
	return nil
}
