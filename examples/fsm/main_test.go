package main

import (
	"os/exec"
	"strings"
	"testing"
)

func TestStandaloneExample(t *testing.T) {
	command := exec.Command("go", "run", ".")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run example: %v\n%s", err, output)
	}
	if strings.TrimSpace(string(output)) != "open -> done" {
		t.Fatalf("unexpected example output: %s", output)
	}
}
