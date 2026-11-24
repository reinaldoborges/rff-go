package scripts_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckFormatsWorkingTreeGoFiles(t *testing.T) {
	check, err := filepath.Abs("check.sh")
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	bin := t.TempDir()
	write := func(name, contents string) {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	git := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, output)
		}
	}
	git("init", "-q")
	write(".gitignore", ".scratch/\n")
	write("tracked.go", "package sample\nfunc tracked( ){ }\n")
	write("removed.go", "package sample\n")
	git("add", ".gitignore", "tracked.go", "removed.go")
	if err := os.Remove(filepath.Join(dir, "removed.go")); err != nil {
		t.Fatal(err)
	}
	write("new file.go", "package sample\nfunc added( ){ }\n")
	write(".scratch/generated.go", "package generated\nfunc generated( ){ }\n")
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	run := func() ([]byte, error) {
		t.Helper()
		cmd := exec.Command("sh", check)
		cmd.Dir = dir
		return cmd.CombinedOutput()
	}
	output, err := run()
	if err == nil || !strings.Contains(string(output), "tracked.go") ||
		!strings.Contains(string(output), "new file.go") || strings.Contains(string(output), ".scratch/") {
		t.Fatalf("check did not select only working-tree Go files: %v\n%s", err, output)
	}
	cmd := exec.Command("gofmt", "-w", "tracked.go", "new file.go")
	cmd.Dir = dir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("gofmt: %v\n%s", err, output)
	}
	if output, err := run(); err != nil {
		t.Fatalf("ignored scratch or removed file broke check: %v\n%s", err, output)
	}
}
