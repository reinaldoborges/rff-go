package runtime

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/reinaldoborges/rff-go/internal/nilcheck"
)

// TimerRequest schedules a typed event for a workflow at a persisted deadline.
type TimerRequest[Event any] struct {
	ID         string
	WorkflowID string
	Deadline   time.Time
	Event      Event
	EventCodec CodecVersion
}

// TimerReceipt identifies the original durable scheduling outcome.
type TimerReceipt struct {
	ID         string
	WorkflowID string
	Deadline   time.Time
}

// CurrentTimerSchemaVersion identifies the current durable timer representation.
const CurrentTimerSchemaVersion uint = 2

// TimerRecord is the versioned representation persisted by a timer adapter.
type TimerRecord struct {
	SchemaVersion uint
	TimerReceipt
	Event          EncodedValue
	Completed      bool
	Owner          string
	Generation     uint64
	LeaseExpiresAt time.Time
	Compatibility  *CompatibilityDiagnostic `json:",omitempty"`
}

// TimerScheduleStore serializes scheduling by timer ID. A nil decision replays
// the existing record. Decisions must not re-enter the store.
type TimerScheduleStore interface {
	ScheduleTimer(context.Context, string, func(*TimerRecord) (*TimerRecord, error)) (TimerRecord, bool, error)
}

// TimerScheduler durably accepts and deduplicates typed timer events.
type TimerScheduler[Event any] struct {
	store   TimerScheduleStore
	resolve CodecResolver[Event]
}

func NewTimerScheduler[Event any](store TimerScheduleStore, resolve CodecResolver[Event]) (*TimerScheduler[Event], error) {
	if nilcheck.IsNil(store) || resolve == nil {
		return nil, errors.New("timer store and event codec resolver are required")
	}
	return &TimerScheduler[Event]{store: store, resolve: resolve}, nil
}

// Schedule returns the first receipt for semantically identical requests.
func (s *TimerScheduler[Event]) Schedule(ctx context.Context, request TimerRequest[Event]) (TimerReceipt, bool, error) {
	if request.ID == "" || request.WorkflowID == "" || request.Deadline.IsZero() ||
		request.EventCodec.Name == "" || request.EventCodec.Version == "" {
		return TimerReceipt{}, false, ErrInvalidRequest
	}
	record, created, err := s.store.ScheduleTimer(ctx, request.ID, func(existing *TimerRecord) (*TimerRecord, error) {
		if existing != nil {
			if existing.WorkflowID != request.WorkflowID || !existing.Deadline.Equal(request.Deadline) {
				return nil, ErrConflict
			}
			codec, err := resolveCodec(s.resolve, existing.Event.Codec)
			if err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return nil, err
			}
			equal, err := codec.Equal(ctx, slices.Clone(existing.Event.Data), request.Event)
			if err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return nil, fmt.Errorf("%w: timer event equality: %w", ErrCompatibility, err)
			}
			if !equal {
				return nil, ErrConflict
			}
			return nil, nil
		}
		codec, err := resolveCodec(s.resolve, request.EventCodec)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, err
		}
		data, err := codec.Encode(ctx, request.Event)
		if err != nil {
			return nil, fmt.Errorf("encode timer event: %w", err)
		}
		return &TimerRecord{
			SchemaVersion: CurrentTimerSchemaVersion,
			TimerReceipt:  TimerReceipt{ID: request.ID, WorkflowID: request.WorkflowID, Deadline: request.Deadline.UTC()},
			Event:         EncodedValue{Codec: request.EventCodec, Data: slices.Clone(data)},
		}, nil
	})
	if err != nil {
		return TimerReceipt{}, false, err
	}
	return record.TimerReceipt, created, nil
}
