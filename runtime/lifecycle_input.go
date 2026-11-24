package runtime

import (
	"context"
	"errors"
	"fmt"

	"github.com/reinaldoborges/rff-go/internal/nilcheck"
)

// LifecycleInput is a typed ingress enabled for automatic reconciliation.
// Construct one using NewInputIngress; absent inputs do not start a path.
type LifecycleInput interface {
	lifecycleInput() (string, func(context.Context) error, error)
}

func (i *InputIngress[Value]) lifecycleInput() (string, func(context.Context) error, error) {
	if i == nil || nilcheck.IsNil(i.store) || i.channel == "" || i.resolve == nil || i.handle == nil {
		return "", nil, errors.New("input store, channel, codec resolver, and handler are required")
	}
	return "input:" + i.channel, i.Reconcile, nil
}

func (l *Lifecycle[EntityID, StateID, Event, Input]) addInputs(inputs []LifecycleInput, add func(string, bool, func(context.Context, func()) error)) error {
	names := make(map[string]bool, len(inputs))
	for _, input := range inputs {
		if nilcheck.IsNil(input) {
			return errors.New("input ingress is required")
		}
		name, reconcile, err := input.lifecycleInput()
		if err != nil {
			return err
		}
		if names[name] {
			return fmt.Errorf("duplicate input channel %q", name)
		}
		names[name] = true
		add(name, false, func(ctx context.Context, _ func()) error { return reconcile(ctx) })
	}
	return nil
}
