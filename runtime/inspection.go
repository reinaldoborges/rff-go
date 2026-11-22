package runtime

import (
	"context"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
)

// ExecutionInspection contains protocol information without application payloads.
// The host must authorize inspection and protect application-provided failures.
type ExecutionInspection struct {
	ID                        string
	Operation                 Operation
	MachineID                 string
	MachineVersion            string
	Status                    Status
	AcceptedAt                time.Time
	Deadline                  time.Time
	CycleDeadline             time.Time
	RejectionRecoveryDeadline time.Time
	Failure                   *fsm.Failure
	FailureDisposition        fsm.Disposition
	Compatibility             *CompatibilityDiagnostic
	CommitReceipt             *CommitReceipt
	NotificationIntentID      string
	NotificationFailure       *fsm.Failure
	NotificationWaived        bool
	Controls                  []HookControlReceipt
	NotificationControls      []NotificationControlReceipt
	Migrations                []MigrationReceipt
	Hook                      *HookProgress
}

// InspectionStore lists retained execution IDs and inspects any execution,
// including records whose application entity or state can no longer be decoded.
// Hosts own authorization and may restrict or filter the results.
type InspectionStore interface {
	ListExecutionIDs(context.Context) ([]string, error)
	InspectExecution(context.Context, string) (ExecutionInspection, error)
}
