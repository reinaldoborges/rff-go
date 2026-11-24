package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/mysql"
	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

var errDeferredInput = errors.New("input accepted for later reconciliation")

func (h *Host) demoSchema(ctx context.Context) error {
	_, err := h.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS renewal_lifecycle_effects
		(kind VARCHAR(32) NOT NULL, effect_id VARCHAR(255) NOT NULL, plan_id VARCHAR(255) NOT NULL,
		PRIMARY KEY (kind, effect_id))`)
	return err
}

func (h *Host) demoEffect(ctx context.Context, kind, id, plan string) error {
	_, err := h.db.ExecContext(ctx, `INSERT INTO renewal_lifecycle_effects (kind, effect_id, plan_id) VALUES (?, ?, ?)`, kind, id, plan)
	if err != nil && isDuplicate(err) {
		return nil
	}
	return err
}

func (h *Host) demoInput(deferDelivery bool) (*transition.InputIngress[Input], error) {
	codec := jsonCodec[Input]{version: "1"}
	resolve := func(version transition.CodecVersion) (transition.Codec[Input], error) {
		if version != codec.Version() {
			return nil, transition.ErrCompatibility
		}
		return codec, nil
	}
	return transition.NewInputIngress(h.store, "renewal-demo", resolve, func(ctx context.Context, d transition.InputDelivery[Input]) (transition.InputOutcome, error) {
		if deferDelivery {
			return transition.InputOutcome{}, errDeferredInput
		}
		if d.Kind != transition.Command || d.Value.PlanID != d.WorkflowID || d.Value.PeriodID == "" || d.Value.Principal != "operator" {
			return transition.InputOutcome{}, transition.ErrInvalidRequest
		}
		if err := h.demoEffect(ctx, "input", d.ID, d.WorkflowID); err != nil {
			return transition.InputOutcome{}, err
		}
		return transition.InputOutcome{Receipt: d.ID}, nil
	})
}

func (h *Host) enqueueDemoInput(ctx context.Context, plan, period string) error {
	input, err := h.demoInput(true)
	if err != nil {
		return err
	}
	_, err = input.Submit(ctx, transition.InputRequest[Input]{ID: "input:" + plan + ":" + period, WorkflowID: plan,
		Kind: transition.Command, Value: Input{PlanID: plan, PeriodID: period, Principal: "operator"}, Codec: (jsonCodec[Input]{version: "1"}).Version()})
	if errors.Is(err, errDeferredInput) {
		return nil
	}
	return err
}

func (h *Host) enqueueDemoTimer(ctx context.Context, plan, period string) error {
	codec := jsonCodec[Event]{version: "1"}
	scheduler, err := transition.NewTimerScheduler(h.store, func(version transition.CodecVersion) (transition.Codec[Event], error) {
		if version != codec.Version() {
			return nil, transition.ErrCompatibility
		}
		return codec, nil
	})
	if err != nil {
		return err
	}
	_, _, err = scheduler.Schedule(ctx, transition.TimerRequest[Event]{ID: "timer:" + plan + ":" + period,
		WorkflowID: plan, Deadline: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), Event: "reminder", EventCodec: codec.Version()})
	return err
}

func (h *Host) demoOutbox() (transition.NotificationHandoff[string, State], transition.NotificationActivityStore[string, State], error) {
	switch store := h.store.(type) {
	case *sqlite.Store[string, State]:
		outbox, err := sqlite.NewNotificationOutbox(store, h.clock)
		return outbox, outbox, err
	case *mysql.Store[string, State]:
		outbox, err := mysql.NewNotificationOutbox(store, h.clock)
		return outbox, outbox, err
	default:
		return nil, nil, errors.New("unsupported notification store")
	}
}

func (h *Host) lifecycle() (*transition.Lifecycle[string, State, Event, Input], error) {
	if err := h.demoSchema(context.Background()); err != nil {
		return nil, err
	}
	eventCodec := jsonCodec[Event]{version: "1"}
	inputCodec := jsonCodec[Input]{version: "1"}
	resolveEvent := func(version transition.CodecVersion) (transition.Codec[Event], error) {
		if version != eventCodec.Version() {
			return nil, transition.ErrCompatibility
		}
		return eventCodec, nil
	}
	resolveInput := func(version transition.CodecVersion) (transition.Codec[Input], error) {
		if version != inputCodec.Version() {
			return nil, transition.ErrCompatibility
		}
		return inputCodec, nil
	}
	runner, err := transition.NewHookRunner[string, State, Event, Input](h.machines, resolveInput, resolveEvent)
	if err != nil {
		return nil, err
	}
	ingress, err := h.demoInput(false)
	if err != nil {
		return nil, err
	}
	lifecycle, err := transition.NewLifecycle[string, State, Event, Input](h.store, runner, runner, h, transition.LifecycleConfig{
		Coordinator: transition.CoordinatorConfig{ID: "renewal-lifecycle", LeaseTTL: 5 * time.Second, Clock: h.clock},
		Worker: transition.WorkerConfig{ID: "renewal-lifecycle-worker", Capacity: 2, LeaseTTL: 5 * time.Second,
			RenewalInterval: time.Second, Clock: transition.SystemWorkerClock{}},
		PollInterval: 100 * time.Millisecond,
		Inputs:       []transition.LifecycleInput{ingress},
	})
	if err != nil {
		return nil, err
	}
	err = lifecycle.EnableContinuation(h.store, h.machines, transition.ContinuationDriverConfig[string, State, Event, Input]{
		Clock: h.clock, LeaseDuration: 30 * time.Second,
		EventCodec: eventCodec.Version(), InputCodec: inputCodec.Version(),
		ResolveEvent: resolveEvent, ResolveInput: resolveInput, Acceptor: h.acceptor, Host: h,
		OriginRevision: h.originRevision,
	}, h, "renewal-lifecycle-continuation")
	if err != nil {
		return nil, err
	}
	handoff, outbox, err := h.demoOutbox()
	if err != nil {
		return nil, err
	}
	if err := lifecycle.EnableNotifications(h.store, handoff, outbox, func(ctx context.Context, d transition.NotificationDelivery[string, State]) error {
		return h.demoEffect(ctx, "notification", d.Intent.ID, d.Intent.Entity)
	}, transition.LifecycleNotificationConfig{Capacity: 1, Worker: transition.NotificationWorkerConfig{
		ID: "renewal-notification", LeaseTTL: 5 * time.Second, RenewalInterval: time.Second,
		AttemptTimeout: 5 * time.Second, RetryDelay: time.Second, Clock: transition.SystemWorkerClock{},
	}}); err != nil {
		return nil, err
	}
	if err := lifecycle.EnableTimerDelivery(h.store, resolveEvent, func(ctx context.Context, d transition.TimerDelivery[Event]) error {
		if d.Event != "reminder" {
			return transition.ErrInvalidRequest
		}
		return h.demoEffect(ctx, "timer", d.ID, d.WorkflowID)
	}, transition.TimerLifecycleConfig{Capacity: 1, Worker: transition.TimerWorkerConfig{
		ID: "renewal-timer", LeaseTTL: 5 * time.Second, RenewalInterval: time.Second, Clock: transition.SystemWorkerClock{},
	}}); err != nil {
		return nil, err
	}
	return lifecycle, nil
}

func (h *Host) inspectDemo(ctx context.Context, plan string) error {
	if err := h.demoSchema(ctx); err != nil {
		return err
	}
	counts, err := h.demoCounts(ctx, plan)
	if err != nil {
		return err
	}
	for _, kind := range []string{"input", "timer", "notification"} {
		fmt.Printf("%s=%d ", kind, counts[kind])
	}
	fmt.Println()
	return nil
}

func (h *Host) demoCounts(ctx context.Context, plan string) (map[string]int, error) {
	counts := make(map[string]int)
	for _, kind := range []string{"input", "timer", "notification"} {
		var count int
		if err := h.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM renewal_lifecycle_effects WHERE kind = ? AND plan_id = ?`, kind, plan).Scan(&count); err != nil {
			return nil, err
		}
		counts[kind] = count
	}
	return counts, nil
}
