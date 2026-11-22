// Package runtime defines host-injected transition execution contracts.
package runtime

import (
	"context"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
)

// CurrentSchemaVersion is the version of newly accepted execution records.
const CurrentSchemaVersion uint = 1

// Operation distinguishes initialization from an ordinary transition.
type Operation string

const (
	Initialize Operation = "initialize"
	Transition Operation = "transition"
)

// NotificationOverride chooses whether to inherit or override the selection.
type NotificationOverride string

const (
	NotifyInherit NotificationOverride = "inherit"
	NotifyEnable  NotificationOverride = "enable"
	NotifyDisable NotificationOverride = "disable"
)

// Status describes execution progress, not the domain state.
type Status string

const (
	Waiting            Status = "Waiting"
	Running            Status = "Running"
	Notifying          Status = "Notifying"
	NotificationRepair Status = "NotificationRepair"
	NeedsIntervention  Status = "NeedsIntervention"
	CommitRejected     Status = "CommitRejected"
	Aborting           Status = "Aborting"
	Complete           Status = "Complete"
	Aborted            Status = "Aborted"
)

// CodecVersion identifies an application-owned codec.
type CodecVersion struct {
	Name    string
	Version string
}

// EncodedValue carries a version-pinned representation for rehydration.
type EncodedValue struct {
	Codec CodecVersion
	Data  []byte
}

// Codec persists values or durable immutable references and compares semantics.
// Equal must use stable application-defined semantics and fail if it cannot
// verify equality. Implementations must not re-enter the acceptance store.
type Codec[T any] interface {
	Encode(context.Context, T) ([]byte, error)
	Decode(context.Context, []byte) (T, error)
	Equal(ctx context.Context, stored []byte, incoming T) (bool, error)
}

// CodecResolver looks up an exact codec version.
type CodecResolver[T any] func(CodecVersion) (Codec[T], error)

// Request contains a prepared initialization or transition request.
type Request[EntityID comparable, StateID comparable, Event, Input any] struct {
	Entity         EntityID
	Key            string
	Operation      Operation
	MachineID      string
	MachineVersion string
	Previous       StateID
	HasPrevious    bool
	Event          Event
	HasEvent       bool
	EventCodec     CodecVersion
	Input          Input
	InputCodec     CodecVersion
	Notification   NotificationOverride
}

// Execution is the durable accepted transition snapshot.
type Execution[EntityID comparable, StateID comparable] struct {
	SchemaVersion             uint
	ID                        string
	Entity                    EntityID
	Key                       string
	Operation                 Operation
	MachineID                 string
	MachineVersion            string
	Previous                  StateID
	HasPrevious               bool
	Target                    StateID
	Event                     EncodedValue
	Input                     EncodedValue
	NotificationOverride      NotificationOverride
	Notify                    bool
	Policy                    fsm.Policy
	AcceptedAt                time.Time
	Deadline                  time.Time
	CycleDeadline             time.Time
	Controls                  []HookControlReceipt
	Status                    Status
	CommitOperationID         string
	CommitReceipt             *CommitReceipt
	NotificationIntentID      string
	NotificationFailure       *fsm.Failure
	NotificationWaived        bool
	NotificationControls      []NotificationControlReceipt
	Compatibility             *CompatibilityDiagnostic
	OriginalRequest           *OriginalRequest
	Migrations                []MigrationReceipt
	RecoveryStarted           bool
	RejectionRecoveryDeadline time.Time
	Failure                   *fsm.Failure
	FailureDisposition        fsm.Disposition
}

// EffectiveDeadline is the current control-cycle deadline, or the original
// accepted deadline before any authorized control cycle.
func (e Execution[EntityID, StateID]) EffectiveDeadline() time.Time {
	if !e.CycleDeadline.IsZero() {
		return e.CycleDeadline
	}
	return e.Deadline
}

// AcceptanceView is read atomically for one entity and idempotency key.
type AcceptanceView[EntityID comparable, StateID comparable] struct {
	Existing *Execution[EntityID, StateID]
	Active   bool
}

// AcceptanceDecisionFunc returns a new execution, or nil to replay Existing.
type AcceptanceDecisionFunc[EntityID comparable, StateID comparable] func(AcceptanceView[EntityID, StateID]) (*Execution[EntityID, StateID], error)

// AcceptanceStore serializes acceptance per entity. It calls decide once while
// holding the acceptance transaction; decide must not re-enter the store.
// A new execution, key mapping, and active
// reservation must commit atomically before success is returned. A nil
// decision is valid only when Existing is present. A new execution is valid
// only without an existing mapping or active reservation, with matching
// entity and key, a unique nonempty execution ID, the current schema version,
// Waiting status, no commit operation, and no failure. Invalid decisions return
// ErrInvalidDecision. A callback error must leave no writes. A storage error
// after commit may have an unknown outcome; a retry must reconcile against the
// original key. Records and byte slices must not alias stored data.
type AcceptanceStore[EntityID comparable, StateID comparable] interface {
	Accept(ctx context.Context, entity EntityID, key string, decide AcceptanceDecisionFunc[EntityID, StateID]) (execution Execution[EntityID, StateID], created bool, err error)
}
