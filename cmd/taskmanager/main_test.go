package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/internal/taskmanager"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestLocalFixtureCLIStartsSignalsInspectsAndAuthorizes(t *testing.T) {
	db := filepath.Join(t.TempDir(), "fixture.db")
	invoke := func(args ...string) (string, error) {
		t.Helper()
		var output bytes.Buffer
		err := run(context.Background(), append([]string{"-db", db}, args...), &output)
		return strings.TrimSpace(output.String()), err
	}
	for _, args := range [][]string{
		{"seed-user", "owner", "invitee"},
		{"seed-operator", "operator"},
		{"create-group", "owner", "group", "owner-membership"},
	} {
		if _, err := invoke(args...); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
	}
	raw, err := invoke("start-invitation", "owner", "group", "invitee", "membership", "invite", time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
	if err != nil {
		t.Fatal(err)
	}
	var execution string
	if err := json.Unmarshal([]byte(raw), &execution); err != nil || execution == "" {
		t.Fatalf("invalid execution output %q: %v", raw, err)
	}
	if _, err := invoke("drive"); err != nil {
		t.Fatal(err)
	}
	if _, err := invoke("signal-invitation", "invitee", "membership", "decision", "accept"); err != nil {
		t.Fatal(err)
	}
	if _, err := invoke("drive"); err != nil {
		t.Fatal(err)
	}
	if _, err := invoke("list", "owner"); err == nil {
		t.Fatal("domain owner acquired operator inspection")
	}
	list, err := invoke("list", "operator")
	if err != nil || !strings.Contains(list, execution) {
		t.Fatalf("list did not include execution: %q, %v", list, err)
	}
	show, err := invoke("show", "operator", execution)
	if err != nil || !strings.Contains(show, `"Status":"Complete"`) {
		t.Fatalf("show did not inspect completed execution: %q, %v", show, err)
	}
	for _, action := range []string{"retry", "abort", "notification-retry", "notification-waive"} {
		if _, err := invoke("control", "owner", execution, action, action); !errors.Is(err, taskmanager.ErrDenied) {
			t.Fatalf("domain owner acquired %s control: %v", action, err)
		}
		if _, err := invoke("control", "operator", execution, action, action); !errors.Is(err, transition.ErrExecutionNotActive) {
			t.Fatalf("completed execution accepted %s control: %v", action, err)
		}
	}
	if _, err := invoke("start-task", "invitee", "group", "task", "Task", "create-task"); err != nil {
		t.Fatal(err)
	}
	if _, err := invoke("drive"); err != nil {
		t.Fatal(err)
	}
	if _, err := invoke("signal-task", "invitee", "task", "start", "start"); err != nil {
		t.Fatal(err)
	}
	if _, err := invoke("drive"); err != nil {
		t.Fatal(err)
	}
}

func TestLocalFixtureCLIAuthorizedRetryAndAbort(t *testing.T) {
	for _, action := range []string{"retry", "abort"} {
		t.Run(action, func(t *testing.T) {
			testLocalFixtureCLIControl(t, action)
		})
	}
}

func testLocalFixtureCLIControl(t *testing.T, action string) {
	t.Helper()
	ctx := context.Background()
	db := filepath.Join(t.TempDir(), "retry.db")
	past := time.Now().Add(-2 * time.Hour)
	fixture, err := taskmanager.Open(db, func() time.Time { return past })
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.SeedUsers(ctx, "owner", "invitee"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.SeedOperator(ctx, "operator"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
		t.Fatal(err)
	}
	id, err := fixture.Invite(ctx, "owner", "group", "invitee", "membership", "invite", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.Close(); err != nil {
		t.Fatal(err)
	}
	invoke := func(args ...string) error {
		t.Helper()
		return run(ctx, append([]string{"-db", db}, args...), &bytes.Buffer{})
	}
	if err := invoke("drive"); err != nil {
		t.Fatal(err)
	}
	if err := invoke("control", "operator", id, action, action); err != nil {
		t.Fatalf("authorized %s failed: %v", action, err)
	}
	if err := invoke("drive"); err != nil {
		t.Fatal(err)
	}
	if action == "abort" {
		if err := invoke("drive"); err != nil {
			t.Fatal(err)
		}
	}
	fixture, err = taskmanager.Open(db, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	})
	membership, err := fixture.Membership(ctx, "owner", "membership")
	if action == "abort" {
		if !errors.Is(err, taskmanager.ErrNotFound) {
			t.Fatalf("aborted execution committed membership: %+v, %v", membership, err)
		}
	} else if err != nil || membership.Status != taskmanager.Invited {
		t.Fatalf("retried execution did not commit: %+v, %v", membership, err)
	}
}

func TestLocalFixtureCLIExecutable(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "taskmanager")
	build := exec.Command("go", "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v\n%s", err, output)
	}
	db := filepath.Join(dir, "fixture.db")
	invoke := func(args ...string) []byte {
		t.Helper()
		command := exec.Command(binary, append([]string{"-db", db}, args...)...)
		output, err := command.CombinedOutput()
		if err != nil || !json.Valid(output) {
			t.Fatalf("execute CLI %v: output=%q error=%v", args, output, err)
		}
		return output
	}
	for _, args := range [][]string{
		{"seed-user", "owner", "invitee"},
		{"seed-operator", "operator"},
		{"create-group", "owner", "group", "owner-membership"},
	} {
		invoke(args...)
	}
	var execution string
	if err := json.Unmarshal(invoke("start-invitation", "owner", "group", "invitee", "membership", "invite-1",
		"2099-01-01T00:00:00Z"), &execution); err != nil || execution == "" {
		t.Fatalf("parse invitation execution: %q, %v", execution, err)
	}
	for _, args := range [][]string{
		{"drive"},
		{"signal-invitation", "invitee", "membership", "decision-1", "accept"},
		{"drive"},
		{"start-task", "invitee", "group", "task", "Example task", "create-task-1"},
		{"drive"},
		{"signal-task", "invitee", "task", "start-1", "start"},
		{"drive"},
	} {
		invoke(args...)
	}
	if list := invoke("list", "operator"); !strings.Contains(string(list), execution) {
		t.Fatalf("list omitted invitation execution: %s", list)
	}
	if show := invoke("show", "operator", execution); !strings.Contains(string(show), `"Status":"Complete"`) {
		t.Fatalf("show did not inspect completed invitation: %s", show)
	}
	var task taskmanager.Task
	if err := json.Unmarshal(invoke("task", "invitee", "task"), &task); err != nil || task.Status != taskmanager.TaskInProgress {
		t.Fatalf("task did not progress across CLI processes: %+v, %v", task, err)
	}
	command := exec.Command(binary, "-db", db, "unknown")
	if output, err := command.CombinedOutput(); err == nil || !strings.Contains(string(output), "unknown command") {
		t.Fatalf("invalid command succeeded: output=%q error=%v", output, err)
	}
}
