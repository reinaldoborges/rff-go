package mysql_test

import (
	"testing"

	"github.com/reinaldoborges/rff-go/internal/durableidentitytest"
)

func TestDurableIdentityByteExactRoundTrip(t *testing.T) {
	durableidentitytest.Check(t, openMySQL(t))
}
