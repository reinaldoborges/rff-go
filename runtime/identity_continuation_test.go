package runtime_test

import (
	"errors"
	"strings"
	"testing"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestContinuationRejectsInvalidExecutionID(t *testing.T) {
	for _, id := range []string{"", string([]byte{0xff}), strings.Repeat("x", 257)} {
		marker := continuationMarker(id)
		if err := transition.ValidateContinuationMarker(marker); !errors.Is(err, transition.ErrInvalidDurableID) {
			t.Fatalf("marker ID %q: %v", id, err)
		}
	}
}
