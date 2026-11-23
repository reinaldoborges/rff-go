package mysql_test

import (
	"testing"

	"github.com/reinaldoborges/rff-go/internal/continuationcontract"
)

func TestContinuationWaitsForReceiptAndReservationRelease(t *testing.T) {
	continuationcontract.RunReadiness(t, openMySQL(t))
}

func TestContinuationPreservesExecutionIdentities(t *testing.T) {
	continuationcontract.RunIdentitySafety(t, openMySQL(t))
}

func TestContinuationMigrationContract(t *testing.T) {
	continuationcontract.RunMigration(t, openMySQL(t))
}

func TestContinuationDiscoveryPaginationContract(t *testing.T) {
	continuationcontract.RunPagination(t, openMySQL(t))
}
