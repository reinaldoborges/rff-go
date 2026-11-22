// Command taskmanager operates the local, test-only task-manager fixture.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/reinaldoborges/rff-go/internal/taskmanager"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, out io.Writer) (result error) {
	flags := flag.NewFlagSet("taskmanager", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	path := flags.String("db", "", "fixture SQLite database")
	if err := flags.Parse(args); err != nil {
		return err
	}
	args = flags.Args()
	if *path == "" || len(args) == 0 {
		return errors.New("usage: taskmanager -db PATH COMMAND [ARGS...]")
	}
	counts := map[string]int{
		"seed-operator": 1, "create-group": 3, "start-invitation": 6, "signal-invitation": 4,
		"start-task": 5, "signal-task": 4, "drive": 0, "list": 1, "show": 2, "control": 4,
		"group": 2, "membership": 2, "task": 2, "update-task": 3, "assign-task": 3,
		"remove-member": 3, "transfer": 4, "close-group": 2,
	}
	command, params := args[0], args[1:]
	expected, known := counts[command]
	if command == "seed-user" {
		if len(params) == 0 {
			return errors.New("seed-user requires at least one identity")
		}
	} else if !known || len(params) != expected {
		return fmt.Errorf("unknown command or incorrect argument count: %s", command)
	}
	fixture, err := taskmanager.Open(*path, time.Now)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, fixture.Close()) }()
	var value any = "ok"
	switch command {
	case "seed-user":
		err = fixture.SeedUsers(ctx, params...)
	case "seed-operator":
		err = fixture.SeedOperator(ctx, params[0])
	case "create-group":
		err = fixture.CreateGroup(ctx, params[0], params[1], params[2])
	case "start-invitation":
		deadline, parseErr := time.Parse(time.RFC3339, params[5])
		if parseErr != nil {
			return fmt.Errorf("parse invitation deadline: %w", parseErr)
		}
		value, err = fixture.Invite(ctx, params[0], params[1], params[2], params[3], params[4], deadline)
	case "signal-invitation":
		value, err = fixture.Decide(ctx, params[0], params[1], params[2], taskmanager.InvitationDecision(params[3]))
	case "start-task":
		value, err = fixture.CreateTask(ctx, params[0], params[1], params[2], params[3], params[4])
	case "signal-task":
		value, err = fixture.TransitionTask(ctx, params[0], params[1], params[2], taskmanager.TaskEvent(params[3]))
	case "drive":
		err = fixture.Drive(ctx)
	case "list":
		value, err = fixture.ListExecutions(ctx, params[0])
	case "show":
		value, err = fixture.InspectExecution(ctx, params[0], params[1])
	case "control":
		err = fixture.ControlExecution(ctx, params[0], params[1], params[2], transition.ControlAction(params[3]))
	case "group":
		value, err = fixture.Group(ctx, params[0], params[1])
	case "membership":
		value, err = fixture.Membership(ctx, params[0], params[1])
	case "task":
		value, err = fixture.Task(ctx, params[0], params[1])
	case "update-task":
		err = fixture.UpdateTask(ctx, params[0], params[1], params[2])
	case "assign-task":
		err = fixture.AssignTask(ctx, params[0], params[1], params[2])
	case "remove-member":
		value, err = fixture.RemoveMember(ctx, params[0], params[1], params[2])
	case "transfer":
		value, err = fixture.TransferOwnership(ctx, params[0], params[1], params[2], params[3])
	case "close-group":
		err = fixture.CloseGroup(ctx, params[0], params[1])
	}
	if err != nil {
		return fmt.Errorf("%s: %w", command, err)
	}
	if err := json.NewEncoder(out).Encode(value); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}
