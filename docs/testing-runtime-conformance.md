# Runtime conformance verification

Run the repository baseline from the repository root in the Go version pinned
by `go.mod`:

```sh
./scripts/check.sh
```

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

CHECKS_CONTAINER='<unique-Go-container-name>'
MYSQL_GATEWAY="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.Gateway}}{{end}}' "$CHECKS_CONTAINER")"
RFF_MYSQL_DSN="root:rff-test@tcp(${MYSQL_GATEWAY}:${MYSQL_PORT})/rff_test?parseTime=true"
docker exec -e RFF_MYSQL_DSN="$RFF_MYSQL_DSN" "$CHECKS_CONTAINER" ./scripts/check-mysql.sh
docker stop "$MYSQL_CONTAINER"
```

`scripts/check-mysql.sh` fails if `RFF_MYSQL_DSN` is unset. It runs all MySQL
adapter contracts and process-restart cases, then explicitly runs both
public-host scenario suites against fresh MySQL 8.4 databases:

```sh
docker exec -e RFF_MYSQL_DSN="$RFF_MYSQL_DSN" "$CHECKS_CONTAINER" \
  go test ./examples/renewal -run '^TestMySQLHostScenarioAndProcessRestart$' -count=1 -v
docker exec -e RFF_MYSQL_DSN="$RFF_MYSQL_DSN" "$CHECKS_CONTAINER" \
  go test ./examples/renewal -run '^TestBatchMySQLScenarioAndProcessRestart$' -count=1 -v
```

These are not adapter-only checks: the host scenario covers shared and
separate runtime/domain stores, and the batch scenario covers both store
layouts, real child-process termination and restart after a confirmed effect,
unknown-effect blocking, durable fan-out, and concurrent host record claims.
The host test logs the server version; the batch test uses the same MySQL 8.4
version assertion. A skipped MySQL host or batch test is unverified, not a
passing gate.

The standard Go baseline also runs the SQLite host and batch scenarios,
including shared and separate stores. The adapter suites contain the
cross-feature compatibility, migration, control, acceptance, and durable
process-restart contracts for both adapters.
