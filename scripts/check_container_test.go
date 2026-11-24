package scripts_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestContainerRunnerSerializesFullChecks(t *testing.T) {
	flock, err := exec.LookPath("flock")
	if err != nil {
		t.Skip("flock is unavailable")
	}
	runner, err := filepath.Abs("check-container.sh")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.Mkdir(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	docker := "#!/bin/sh\nprintf '%s\\n' \"$RUNNER_MODE\" >> \"$RUNNER_LOG\"\n" +
		"if [ \"$RUNNER_MODE\" = first ]; then while [ ! -f \"$RUNNER_RELEASE\" ]; do sleep 0.01; done; fi\n"
	lock := "#!/bin/sh\nprintf '%s\\n' \"$RUNNER_MODE\" >> \"$RUNNER_LOCK_LOG\"\nexec " + flock + " \"$@\"\n"
	for name, content := range map[string]string{"docker": docker, "flock": lock} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(content), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("XDG_CACHE_HOME", filepath.Join(dir, "cache"))
	log := filepath.Join(dir, "docker.log")
	lockLog := filepath.Join(dir, "lock.log")
	release := filepath.Join(dir, "release")
	t.Setenv("RUNNER_LOG", log)
	t.Setenv("RUNNER_LOCK_LOG", lockLog)
	t.Setenv("RUNNER_RELEASE", release)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(func() {
		if err := os.WriteFile(release, nil, 0o600); err != nil {
			t.Errorf("release runner: %v", err)
		}
		cancel()
	})
	start := func(mode string, args ...string) *exec.Cmd {
		t.Helper()
		cmd := exec.CommandContext(ctx, "sh", append([]string{runner}, args...)...)
		cmd.Env = append(os.Environ(), "RUNNER_MODE="+mode)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		return cmd
	}
	waitFor := func(path, value string) {
		t.Helper()
		for ctx.Err() == nil {
			data, _ := os.ReadFile(path)
			if strings.Contains(string(data), value+"\n") {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %s in %s", value, path)
	}
	first := start("first", "--full")
	waitFor(log, "first")
	second := start("second", "--full")
	waitFor(lockLog, "second")
	targeted := start("targeted", "--", "go", "test", "./runtime")
	if err := targeted.Wait(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(log)
	if err != nil || strings.Contains(string(data), "second\n") || !strings.Contains(string(data), "targeted\n") {
		t.Fatalf("full checks overlapped or targeted check blocked: %v\n%s", err, data)
	}
	if err := os.WriteFile(release, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := first.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := second.Wait(); err != nil {
		t.Fatal(err)
	}
	waitFor(log, "second")
}
