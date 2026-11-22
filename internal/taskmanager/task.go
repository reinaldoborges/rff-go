package taskmanager

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

type TaskStatus string
type TaskEvent string

const (
	TaskOpen       TaskStatus = "Open"
	TaskInProgress TaskStatus = "InProgress"
	TaskDone       TaskStatus = "Done"
	TaskCancelled  TaskStatus = "Cancelled"

	StartTask    TaskEvent = "start"
	CompleteTask TaskEvent = "complete"
	CancelTask   TaskEvent = "cancel"
)

type Task struct {
	ID       string
	GroupID  string
	Title    string
	Status   TaskStatus
	Assignee string
}

type taskInput struct {
	ID    string
	Title string
	Event TaskEvent
}

func taskEntity(id string) string { return "taskmanager-task:" + id }

// Task returns a task to an Active member of its open group.
func (f *Fixture) Task(ctx context.Context, actor, id string) (Task, error) {
	if actor == "" || id == "" {
		return Task{}, ErrInvalidRequest
	}
	var task Task
	err := f.db.QueryRowContext(ctx, `SELECT t.id, t.group_id, t.title, t.status, t.assignee FROM tm_tasks AS t
		JOIN tm_groups AS g ON g.id = t.group_id AND g.closed = 0
		JOIN tm_memberships AS m ON m.group_id = t.group_id AND m.user_id = ? AND m.status = 'Active'
		WHERE t.id = ?`, actor, id).Scan(&task.ID, &task.GroupID, &task.Title, &task.Status, &task.Assignee)
	if errors.Is(err, sql.ErrNoRows) {
		return Task{}, ErrDenied
	}
	if err != nil {
		return Task{}, fmt.Errorf("read task: %w", err)
	}
	return task, nil
}

// CreateTask durably accepts initialization of a task in an open group.
func (f *Fixture) CreateTask(ctx context.Context, actor, groupID, id, title, key string) (string, error) {
	if actor == "" || groupID == "" || id == "" || title == "" || key == "" {
		return "", ErrInvalidRequest
	}
	if _, err := f.Group(ctx, actor, groupID); err != nil {
		return "", err
	}
	return f.submitTask(ctx, key, fixtureInput{Actor: actor, GroupID: groupID, Task: &taskInput{ID: id, Title: title}})
}

// TransitionTask durably accepts a lifecycle event for a task.
func (f *Fixture) TransitionTask(ctx context.Context, actor, id, key string, event TaskEvent) (string, error) {
	if actor == "" || id == "" || key == "" || (event != StartTask && event != CompleteTask && event != CancelTask) {
		return "", ErrInvalidRequest
	}
	task, err := f.Task(ctx, actor, id)
	if err != nil {
		return "", err
	}
	return f.submitTask(ctx, key, fixtureInput{Actor: actor, GroupID: task.GroupID, Task: &taskInput{ID: id, Event: event}})
}

func (f *Fixture) submitTask(ctx context.Context, key string, input fixtureInput) (string, error) {
	id, err := inputID(input.Actor, input.GroupID, key)
	if err != nil {
		return "", err
	}
	ingress, err := f.taskIngress()
	if err != nil {
		return "", err
	}
	outcome, err := ingress.Submit(ctx, transition.InputRequest[fixtureInput]{
		ID: id, WorkflowID: taskEntity(input.Task.ID), Kind: transition.Command, Codec: taskInputCodec, Value: input,
	})
	if errors.Is(err, transition.ErrConflict) {
		return "", ErrConflict
	}
	if err != nil {
		return "", err
	}
	if outcome.Failure != nil {
		switch outcome.Failure.Code {
		case "denied":
			return "", ErrDenied
		case "conflict":
			return "", ErrConflict
		default:
			return "", ErrInvalidRequest
		}
	}
	return outcome.Receipt, nil
}
