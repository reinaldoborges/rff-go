# Implementation guidance

Use the Go version pinned in `go.mod`, with CGO enabled and a supported C
compiler for the SQLite adapter and race detection. The standard repository
check and CI do not depend on MySQL. Local MySQL adapter verification uses
`scripts/check-mysql.sh` with a disposable MySQL 8.4 database and a DSN with
permission to create and drop test databases.

For implementation work, read the home repository's
[architecture baseline](https://github.com/reinaldoborges/rff-home/blob/main/docs/architecture.md)
and [implementation contract](https://github.com/reinaldoborges/rff-home/blob/main/docs/implementation-agent-contract.md),
then follow the design revisions and acceptance criteria pinned by the home issue.
Link implementation PRs to the home issue and relevant decisions. Raise missing
or conflicting design decisions in the home issue before affected work proceeds.

For repeated local checks, reuse a running Go validation container with this
checkout mounted at `/work`. If none exists, choose a unique container name for
this checkout and start one from the repository root:

```sh
CHECKS_CONTAINER="rff-go-checks-$(id -u)-$$"
docker run -d --name "$CHECKS_CONTAINER" \
  --mount "type=bind,src=$PWD,dst=/work" -w /work \
  "golang:$(awk '/^go / {print $2; exit}' go.mod)" sleep infinity
printf 'Validation container: %s\n' "$CHECKS_CONTAINER"
```

Verify the container is running, its `/work` mount is this checkout, and
`docker exec "$CHECKS_CONTAINER" go version` matches `go.mod`. Record its name,
set `CHECKS_CONTAINER` to that name in later shells, and reuse it across test
rounds; leave it running for subsequent work. Run
issue-specific checks and the repository baseline inside it, for example:

```sh
docker exec "$CHECKS_CONTAINER" ./scripts/check.sh
```

For local checks, start a disposable MySQL 8.4 service with the following
database and root credentials (the contract tests create and drop isolated
databases):

```sh
docker run -d --rm --name rff-go-mysql-check \
  -e MYSQL_ROOT_PASSWORD=rff-test -e MYSQL_ROOT_HOST=% \
  -e MYSQL_DATABASE=rff_test -p 3307:3306 \
  --health-cmd='mysqladmin ping --host=127.0.0.1 --user=root --password=rff-test' \
  --health-interval=10s --health-timeout=5s --health-retries=10 mysql:8.4
until [ "$(docker inspect -f '{{.State.Health.Status}}' rff-go-mysql-check)" = healthy ]; do sleep 2; done
MYSQL_GATEWAY="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.Gateway}}{{end}}' "$CHECKS_CONTAINER")"
RFF_MYSQL_DSN="root:rff-test@tcp($MYSQL_GATEWAY:3307)/rff_test?parseTime=true"
docker exec -e RFF_MYSQL_DSN="$RFF_MYSQL_DSN" "$CHECKS_CONTAINER" ./scripts/check-mysql.sh
docker stop rff-go-mysql-check
```

The adapter contract tests skip when run directly without `RFF_MYSQL_DSN`;
`scripts/check-mysql.sh` requires it so local MySQL verification cannot silently
be omitted. CI remains unchanged and does not run MySQL integration tests.

Report checks that cannot run. Documentation-only changes need applicable
documentation checks rather than unrelated Go checks.
