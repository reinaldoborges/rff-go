package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLifecycleDemoProcessRecoversAcceptedTransition(t *testing.T) {
	runLifecycleDemoProcess(t, []string{"-db", filepath.Join(t.TempDir(), "renewal.db")})
}

func TestLifecycleDemoMySQLProcessRecoversAcceptedTransition(t *testing.T) {
	dsn := os.Getenv("RFF_MYSQL_DSN")
	if dsn == "" {
		t.Skip("RFF_MYSQL_DSN is required for MySQL lifecycle coverage")
	}
	databases := newRenewalMySQLDatabases(t, dsn, 1)
	runLifecycleDemoProcess(t, []string{"-backend", "mysql", "-dsn", databases[0]})
}

func runLifecycleDemoProcess(t *testing.T, backend []string) {
	t.Helper()
	dir := t.TempDir()
	binary := filepath.Join(dir, "renewal")
	build := exec.Command("go", "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}

	command := func(action string) string {
		t.Helper()
		args := append(append([]string{}, backend...), "-action", action)
		cmd := exec.Command(binary, args...)
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s: %v\n%s", action, err, output)
		}
		return string(output)
	}
	command("enqueue-start-notify")
	command("enqueue-input")
	command("enqueue-timer")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var server *exec.Cmd
	defer func() {
		if server != nil && server.ProcessState == nil {
			_ = server.Process.Kill()
			_ = server.Wait()
		}
	}()
	start := func() {
		t.Helper()
		args := append(append([]string{}, backend...), "-action", "serve")
		server = exec.CommandContext(ctx, binary, args...)
		server.Stdout = os.Stdout
		server.Stderr = os.Stderr
		if err := server.Start(); err != nil {
			t.Fatal(err)
		}
	}
	stop := func() {
		t.Helper()
		if err := server.Process.Signal(os.Interrupt); err != nil {
			t.Fatal(err)
		}
		if err := server.Wait(); err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	}
	start()
	for {
		if strings.Contains(command("inspect"), "state=AwaitingApproval") {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("accepted transition did not recover")
		case <-time.After(20 * time.Millisecond):
		}
	}
	stop()
	command("approve") // Accepted while no lifecycle is running and without a wakeup.
	start()
	for {
		if strings.Contains(command("inspect"), "state=Renewed") {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("committed approval did not continue to renewal")
		case <-time.After(20 * time.Millisecond):
		}
	}
	for {
		status := command("inspect-lifecycle")
		if strings.Contains(status, "input=1") && strings.Contains(status, "timer=1") && strings.Contains(status, "notification=1") {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("durable input, timer, or notification did not complete: %s", status)
		case <-time.After(20 * time.Millisecond):
		}
	}
	stop()
}
