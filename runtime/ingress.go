package runtime

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/reinaldoborges/rff-go/fsm"
	"github.com/reinaldoborges/rff-go/internal/nilcheck"
)

type InputKind string

const (
	Command       InputKind = "command"
	ExternalEvent InputKind = "event"
	HumanDecision InputKind = "decision"
)

// InputRequest is an idempotent typed command, event, or human decision.
// IDs are globally unique across input channels.
type InputRequest[Value any] struct {
	ID         string
	WorkflowID string
	Kind       InputKind
	Value      Value
	Codec      CodecVersion
}

// InputOutcome is a safe, durable application receipt or definitive rejection.
// The host owns the receipt and must not put secrets or raw errors here.
type InputOutcome struct {
	Receipt string
	Failure *fsm.Failure
}

// InputRecord is the adapter representation of a typed input and its outcome.
type InputRecord struct {
	SchemaVersion uint
	ID            string
	Channel       string
	WorkflowID    string
	Kind          InputKind
	Value         EncodedValue
	Outcome       *InputOutcome
}

// InputDelivery is passed to the host with its stable input identity.
type InputDelivery[Value any] struct {
	ID         string
	WorkflowID string
	Kind       InputKind
	Value      Value
}

// InputStore durably accepts, lists and completes inputs. Acceptance is atomic
// by ID; its callback must not re-enter the store. A nil decision replays the
// existing input. Completion must reject a different previously saved outcome.
type InputStore interface {
	AcceptInput(context.Context, string, func(*InputRecord) (*InputRecord, error)) (InputRecord, bool, error)
	ReadInput(context.Context, string) (InputRecord, error)
	PendingInputIDs(context.Context, string) ([]string, error)
	CompleteInput(context.Context, string, InputOutcome) (InputRecord, error)
}

// InputIngress accepts typed inputs and reconciles interrupted host calls.
// Hosts authorize every Submit, including replay. The handler must durably
// deduplicate by input ID and atomically record its outcome with domain effects,
// returning that original outcome on repeat. Handler errors mean unknown or
// transient outcomes; definitive rejection belongs in InputOutcome.Failure.
// The host schedules Reconcile and ensures channel names map to the same codecs
// and handler after restart. Concurrent calls may invoke the handler more than once.
type InputIngress[Value any] struct {
	store   InputStore
	channel string
	resolve CodecResolver[Value]
	handle  func(context.Context, InputDelivery[Value]) (InputOutcome, error)
}

func NewInputIngress[Value any](store InputStore, channel string, resolve CodecResolver[Value],
	handle func(context.Context, InputDelivery[Value]) (InputOutcome, error),
) (*InputIngress[Value], error) {
	if nilcheck.IsNil(store) || channel == "" || resolve == nil || handle == nil {
		return nil, errors.New("input store, channel, codec resolver, and handler are required")
	}
	return &InputIngress[Value]{store: store, channel: channel, resolve: resolve, handle: handle}, nil
}

// Submit durably accepts an input before invoking the host, or returns its
// previously recorded outcome. An error after acceptance can be reconciled.
func (i *InputIngress[Value]) Submit(ctx context.Context, request InputRequest[Value]) (InputOutcome, error) {
	if request.ID == "" || request.WorkflowID == "" || request.Codec.Name == "" || request.Codec.Version == "" ||
		(request.Kind != Command && request.Kind != ExternalEvent && request.Kind != HumanDecision) {
		return InputOutcome{}, ErrInvalidRequest
	}
	record, _, err := i.store.AcceptInput(ctx, request.ID, func(existing *InputRecord) (*InputRecord, error) {
		if existing != nil {
			if existing.Channel != i.channel || existing.WorkflowID != request.WorkflowID || existing.Kind != request.Kind {
				return nil, ErrConflict
			}
			codec, err := resolveCodec(i.resolve, existing.Value.Codec)
			if err != nil {
				return nil, err
			}
			equal, err := codec.Equal(ctx, slices.Clone(existing.Value.Data), request.Value)
			if err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return nil, fmt.Errorf("%w: input equality: %w", ErrCompatibility, err)
			}
			if !equal {
				return nil, ErrConflict
			}
			return nil, nil
		}
		codec, err := resolveCodec(i.resolve, request.Codec)
		if err != nil {
			return nil, err
		}
		data, err := codec.Encode(ctx, request.Value)
		if err != nil {
			return nil, fmt.Errorf("encode input: %w", err)
		}
		return &InputRecord{
			SchemaVersion: 1, ID: request.ID, Channel: i.channel, WorkflowID: request.WorkflowID,
			Kind: request.Kind, Value: EncodedValue{Codec: request.Codec, Data: slices.Clone(data)},
		}, nil
	})
	if err != nil {
		return InputOutcome{}, err
	}
	return i.process(ctx, record)
}

// Reconcile processes all currently pending inputs in this channel.
func (i *InputIngress[Value]) Reconcile(ctx context.Context) error {
	ids, err := i.store.PendingInputIDs(ctx, i.channel)
	if err != nil {
		return err
	}
	var result error
	for _, id := range ids {
		record, err := i.store.ReadInput(ctx, id)
		if err == nil {
			_, err = i.process(ctx, record)
		}
		if err != nil {
			result = errors.Join(result, fmt.Errorf("reconcile input %q: %w", id, err))
		}
	}
	return result
}

func (i *InputIngress[Value]) process(ctx context.Context, record InputRecord) (InputOutcome, error) {
	if record.Channel != i.channel {
		return InputOutcome{}, ErrConflict
	}
	if record.Outcome != nil {
		return *record.Outcome, nil
	}
	codec, err := resolveCodec(i.resolve, record.Value.Codec)
	if err != nil {
		if ctx.Err() != nil {
			return InputOutcome{}, ctx.Err()
		}
		return InputOutcome{}, err
	}
	value, err := codec.Decode(ctx, slices.Clone(record.Value.Data))
	if err != nil {
		if ctx.Err() != nil {
			return InputOutcome{}, ctx.Err()
		}
		return InputOutcome{}, compatibilityError(CompatibilityInputCodec, record.Value.Codec.Name, record.Value.Codec.Version, err)
	}
	outcome, err := i.handle(ctx, InputDelivery[Value]{
		ID: record.ID, WorkflowID: record.WorkflowID, Kind: record.Kind, Value: value,
	})
	if err != nil {
		return InputOutcome{}, fmt.Errorf("handle input %q: %w", record.ID, err)
	}
	record, err = i.store.CompleteInput(ctx, record.ID, outcome)
	if err != nil {
		return InputOutcome{}, err
	}
	if record.Outcome == nil {
		return InputOutcome{}, ErrInvalidDecision
	}
	return *record.Outcome, nil
}
