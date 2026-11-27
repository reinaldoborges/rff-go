// Package selectionevidence captures plans over runtime-valid adapter fixtures.
package selectionevidence

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type Store interface {
	Accept(context.Context, string, string, transition.AcceptanceDecisionFunc[string, string]) (transition.Execution[string, string], bool, error)
	AcquireTransitionLease(context.Context, string, string, string, func() time.Time, time.Duration) (transition.TransitionLease[string], error)
	DispatchHook(context.Context, transition.TransitionLease[string], func() time.Time, transition.HookDispatch) (string, error)
	ScheduleTimer(context.Context, string, func(*transition.TimerRecord) (*transition.TimerRecord, error)) (transition.TimerRecord, bool, error)
	AcceptInput(context.Context, string, func(*transition.InputRecord) (*transition.InputRecord, error)) (transition.InputRecord, bool, error)
}

func Run(t *testing.T, db *sql.DB, store Store, dialect string, notification func(string, time.Time)) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 123456789, time.UTC)
	clock := func() time.Time { return now }
	for i := 0; i < 2; i++ {
		notification(fmt.Sprintf("notification-%02d", i), now)
	}
	count := 24
	if dialect == "mysql" {
		count = 256
	}
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("%02d", i)
		entity := "hook-" + id
		_, _, err := store.Accept(ctx, entity, "request", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			return &transition.Execution[string, string]{
				SchemaVersion: transition.CurrentSchemaVersion, ID: entity, Entity: entity, Key: "request",
				Operation: transition.Initialize, Target: "ready", Status: transition.Waiting,
				Deadline: now.Add(time.Hour), Policy: fsm.Policy{MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
			}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		fence, err := store.AcquireTransitionLease(ctx, entity, entity, "coordinator", clock, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		due := now
		if i >= count/2 {
			due = now.Add(time.Hour)
		}
		if _, err := store.DispatchHook(ctx, fence, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: due}); err != nil {
			t.Fatal(err)
		}
		timerID := "timer-" + id
		_, _, err = store.ScheduleTimer(ctx, timerID, func(*transition.TimerRecord) (*transition.TimerRecord, error) {
			return &transition.TimerRecord{SchemaVersion: transition.CurrentTimerSchemaVersion,
				TimerReceipt: transition.TimerReceipt{ID: timerID, WorkflowID: "workflow", Deadline: due},
				Event:        transition.EncodedValue{Codec: transition.CodecVersion{Name: "event", Version: "1"}}}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		inputID := "input-" + id
		channel := "channel"
		if i >= count/2 {
			channel = "other"
		}
		_, _, err = store.AcceptInput(ctx, inputID, func(*transition.InputRecord) (*transition.InputRecord, error) {
			return &transition.InputRecord{SchemaVersion: 1, ID: inputID, Channel: channel, WorkflowID: "workflow",
				Kind: transition.Command, Value: transition.EncodedValue{Codec: transition.CodecVersion{Name: "input", Version: "1"}}}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	fingerprint(t, db, dialect)
	t.Logf("fixture: 2 notifications; %d hooks and timers (half due, half future); %d pending inputs (half per channel)", count, count)
	seconds, nanos := now.Unix(), now.Nanosecond()
	for _, spec := range []struct {
		name, file, function, table string
		predicates                  []string
		args                        []any
	}{
		{"hook", "activity_worker.go", "ClaimHook", "rff_hook_work", []string{"w.completed = 0", "w.started = 0", "w.available_at_seconds", "w.lease_expires_at_seconds", "e.selection_compatibility = 0", "ORDER BY w.execution_id"}, []any{seconds, seconds, nanos, seconds, seconds, nanos}},
		{"timer", "timer.go", "ClaimDueTimer", "rff_timers", []string{"t.completed = 0", "t.compatibility_blocked = 0", "t.deadline_seconds", "t.lease_expires_seconds", "ORDER BY t.deadline_seconds, t.deadline_nanoseconds, t.id"}, []any{seconds, seconds, nanos, seconds, seconds, nanos}},
		{"notification", "notification_outbox.go", "ClaimNotification", "rff_notification_work", []string{"w.completed = 0", "w.execution_id > ?", "w.due_at_seconds", "w.lease_expires_at_seconds", "ORDER BY w.execution_id"}, []any{"", seconds, seconds, nanos, seconds, seconds, nanos}},
		{"input", "ingress.go", "PendingInputIDs", "rff_inputs", []string{"channel = ?", "outcome_present = 0", "ORDER BY id"}, []any{[]byte("channel")}},
	} {
		query := sourceQuery(t, spec.file, spec.function, spec.table)
		if dialect == "sqlite" && spec.name == "timer" {
			spec.args = append([]any{seconds}, spec.args...)
		}
		for _, required := range spec.predicates {
			if !strings.Contains(query, required) {
				t.Fatalf("%s selection lacks %q", spec.name, required)
			}
		}
		prefix := "EXPLAIN QUERY PLAN "
		if dialect == "mysql" {
			prefix = "EXPLAIN FORMAT=JSON "
		}
		rows, err := db.QueryContext(ctx, prefix+query, spec.args...)
		if err != nil {
			t.Fatalf("%s explain: %v", spec.name, err)
		}
		plan := readRows(t, rows)
		if len(plan) == 0 {
			t.Fatalf("%s returned an empty plan", spec.name)
		}
		query = strings.Join(strings.Fields(query), " ")
		encoded, err := json.Marshal(plan)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("QUERY %s %s", spec.name, query)
		t.Logf("PLAN %s %s", spec.name, encoded)
		if dialect == "mysql" && spec.name == "hook" {
			earlier := now.Add(-time.Hour).Unix()
			rows, err := db.QueryContext(ctx, "EXPLAIN ANALYZE "+query, earlier, earlier, nanos, earlier, earlier, nanos)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("NO-DUE HOOK ANALYZE %v", readRows(t, rows))
		}
		if dialect == "sqlite" && spec.name == "timer" {
			rows, err := db.QueryContext(ctx, "EXPLAIN "+query, spec.args...)
			if err != nil {
				t.Fatal(err)
			}
			bytecode := readRows(t, rows)
			if unboundedTimerWalk(plan, bytecode) {
				t.Fatal("SQLite timer deadline index is walked to exhaustion when no timer is due")
			}
		}
	}
}

// Reject only the specific proven full-index walk; other optimizer choices are
// not asserted. With all deadlines after the bound, this loop visits each row.
func unboundedTimerWalk(plan, bytecode [][]string) bool {
	scansDeadlineIndex := false
	for _, row := range plan {
		if len(row) >= 4 && strings.Contains(row[3], "SCAN t USING INDEX rff_timer_deadline_lookup") {
			scansDeadlineIndex = true
		}
	}
	if !scansDeadlineIndex {
		return false
	}
	for _, rewind := range bytecode {
		if len(rewind) < 4 || rewind[1] != "Rewind" {
			continue
		}
		cursor, hasNext, hasBound := rewind[2], false, false
		for _, op := range bytecode {
			if len(op) < 4 || op[2] != cursor {
				continue
			}
			switch op[1] {
			case "Next":
				hasNext = true
			case "IdxGT", "IdxGE", "SeekGT", "SeekGE", "SeekLT", "SeekLE":
				hasBound = true
			}
		}
		if hasNext && !hasBound {
			return true
		}
	}
	return false
}

func sourceQuery(t *testing.T, file, function, table string) string {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var matches []string
	for _, declaration := range parsed.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Name.Name != function {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			literal, ok := n.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(literal.Value)
			if err == nil && strings.Contains(value, "FROM "+table) && strings.Contains(value, "ORDER BY") {
				matches = append(matches, value)
			}
			return true
		})
	}
	if len(matches) != 1 {
		t.Fatalf("%s.%s: found %d candidate queries", file, function, len(matches))
	}
	return matches[0]
}

func readRows(t *testing.T, rows *sql.Rows) [][]string {
	t.Helper()
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var result [][]string
	for rows.Next() {
		values := make([]sql.RawBytes, len(columns))
		dest := make([]any, len(columns))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			t.Fatal(err)
		}
		row := make([]string, len(values))
		for i := range values {
			row[i] = string(values[i])
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func fingerprint(t *testing.T, db *sql.DB, dialect string) {
	t.Helper()
	ctx := context.Background()
	versionQuery := "SELECT sqlite_version()"
	if dialect == "mysql" {
		versionQuery = "SELECT VERSION()"
	}
	var version string
	if err := db.QueryRowContext(ctx, versionQuery).Scan(&version); err != nil {
		t.Fatal(err)
	}
	var definitions [][]string
	if dialect == "sqlite" {
		rows, err := db.QueryContext(ctx, `SELECT type, name, tbl_name, COALESCE(sql, '') FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name`)
		if err != nil {
			t.Fatal(err)
		}
		definitions = readRows(t, rows)
	} else {
		rows, err := db.QueryContext(ctx, `SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() ORDER BY table_name`)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range readRows(t, rows) {
			var name, ddl string
			if err := db.QueryRowContext(ctx, "SHOW CREATE TABLE `"+row[0]+"`").Scan(&name, &ddl); err != nil {
				t.Fatal(err)
			}
			ddl = regexp.MustCompile(` AUTO_INCREMENT=[0-9]+`).ReplaceAllString(ddl, "")
			definitions = append(definitions, []string{"table", name, ddl})
		}
		rows, err = db.QueryContext(ctx, `SELECT trigger_name, event_object_table, action_timing, event_manipulation, action_statement FROM information_schema.triggers WHERE trigger_schema = DATABASE() ORDER BY trigger_name`)
		if err != nil {
			t.Fatal(err)
		}
		definitions = append(definitions, readRows(t, rows)...)
	}
	encoded, err := json.Marshal(definitions)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("DATABASE %s %s", dialect, version)
	t.Logf("SCHEMA sha256=%x objects=%d (JSON of ordered schema definitions)", sha256.Sum256(encoded), len(definitions))
}
