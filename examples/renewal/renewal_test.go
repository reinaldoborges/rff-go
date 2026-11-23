package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestRunnableRenewalScenarioUsesSQLiteByDefault(t *testing.T) {
	db := filepath.Join(".", ".renewal-test-"+strconv.Itoa(os.Getpid())+".db")
	t.Cleanup(func() {
		for _, suffix := range []string{"", "-shm", "-wal"} {
			_ = os.Remove(db + suffix)
		}
	})
	command := exec.Command("go", "run", ".", "-db", db)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run renewal example: %v\n%s", err, output)
	}
	for _, expected := range []string{
		"states=[AwaitingApproval Due PaymentCaptured Renewed]",
		"Renewed",
		"charge=confirmed",
	} {
		if !strings.Contains(string(output), expected) {
			t.Fatalf("renewal output does not contain %q:\n%s", expected, output)
		}
	}
}
