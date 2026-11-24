# Runtime conformance verification

Run the baseline from the repository root with the Go version pinned by
`go.mod`, CGO enabled, and a C compiler:

```sh
./scripts/check.sh
```

When using Docker, the runner uses the pinned Go image and shared per-user
module and build cache volumes:

```sh
./scripts/check-container.sh --full
./scripts/check-container.sh -- go test ./runtime -count=1
```

Full checks sharing the same user cache directory are serialized with `flock`;
targeted commands may run concurrently. The runner mounts linked-worktree Git
metadata read-only and uses the container's temporary directory. Targeted
checks shorten iteration but do not replace full checks on the stable candidate
and final integration revision. Record the checkout's exact commit, dirty state,
Go version, and check results.

## Optional MySQL gate

The standard baseline and CI do not run MySQL integration tests. Run this
separate gate when MySQL verification is needed; skipped tests are unverified.
The MySQL gate requires a disposable MySQL 8.4 service. Use a container name
and host port unique to the current checkout; do not reuse another worktree's
service. The test account must be able to create and drop databases:

```sh
MYSQL_CONTAINER='<unique-container-name>'
MYSQL_PORT='<unused-host-port>'
docker run -d --rm --name "$MYSQL_CONTAINER" \
  -e MYSQL_ROOT_PASSWORD=rff-test -e MYSQL_ROOT_HOST=% \
  -e MYSQL_DATABASE=rff_test -p "$MYSQL_PORT:3306" \
  --health-cmd='mysqladmin ping --host=127.0.0.1 --user=root --password=rff-test' \
  --health-interval=10s --health-timeout=5s --health-retries=10 mysql:8.4
until [ "$(docker inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER")" = healthy ]; do sleep 2; done

MYSQL_GATEWAY="$(docker network inspect -f '{{range .IPAM.Config}}{{.Gateway}}{{end}}' bridge)"
export RFF_MYSQL_DSN="root:rff-test@tcp(${MYSQL_GATEWAY}:${MYSQL_PORT})/rff_test?parseTime=true"
./scripts/check-container.sh -- ./scripts/check-mysql.sh
```

`scripts/check-mysql.sh` fails if `RFF_MYSQL_DSN` is unset. It runs all MySQL
adapter contracts and process-restart cases, then explicitly runs both
public-host scenario suites against fresh MySQL 8.4 databases:

```sh
./scripts/check-container.sh -- go test ./examples/renewal -run '^TestMySQLHostScenarioAndProcessRestart$' -count=1 -v
./scripts/check-container.sh -- go test ./examples/renewal -run '^TestBatchMySQLScenarioAndProcessRestart$' -count=1 -v
docker stop "$MYSQL_CONTAINER"
```

These are not adapter-only checks: the host scenario covers shared and
separate runtime/domain stores, and the batch scenario covers both store
layouts, real child-process termination and restart after a confirmed effect,
unknown-effect blocking and resolution, fan-out partial-acceptance process
restart, durable fan-out, and concurrent host record claims.
The host test logs the server version; the batch test uses the same MySQL 8.4
version assertion. A skipped MySQL host or batch test is unverified, not a
passing gate.

The standard Go baseline also runs the SQLite host and batch scenarios,
including shared and separate stores. The adapter suites contain the
cross-feature compatibility, migration, control, acceptance, and durable
process-restart contracts for both adapters. The public-host matrix additionally
tests replayed retry/abort identities and short, expired, mismatched and valid
provider idempotency guarantees against both adapters and store layouts.
