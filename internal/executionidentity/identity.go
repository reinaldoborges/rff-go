// Package executionidentity defines the non-lossy execution identity envelope.
package executionidentity

type Fields struct {
	ID, Key, MachineID, MachineVersion, CommitOperationID []byte
	ContinuationMachineID, ContinuationMachineVersion     []byte
	OriginalMachineID, OriginalMachineVersion             []byte
	ReceiptExecutionID, ReceiptOperationID                []byte
	EventCodecName, EventCodecVersion                     []byte
	InputCodecName, InputCodecVersion                     []byte
	OriginalEventCodecName, OriginalEventCodecVersion     []byte
	OriginalInputCodecName, OriginalInputCodecVersion     []byte
}

// Matches verifies the JSON string rendering of a byte-exact identity.
func Matches(display string, raw []byte) bool {
	return raw != nil && display == string([]rune(string(raw)))
}
