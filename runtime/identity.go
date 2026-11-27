package runtime

import (
	"fmt"
	"unicode/utf8"
)

// MaxDurableIDBytes is the maximum UTF-8 byte length of an execution, timer,
// or input ID.
const MaxDurableIDBytes = 256

// ErrInvalidDurableID identifies a malformed execution, timer, or input ID.
var ErrInvalidDurableID = fmt.Errorf("%w: invalid durable ID", ErrInvalidRequest)

// ValidateDurableID validates an execution, timer, or input ID without
// changing its bytes.
func ValidateDurableID(id string) error {
	if id == "" || len(id) > MaxDurableIDBytes || !utf8.ValidString(id) {
		return fmt.Errorf("%w: must be nonempty valid UTF-8 of at most %d bytes", ErrInvalidDurableID, MaxDurableIDBytes)
	}
	return nil
}
