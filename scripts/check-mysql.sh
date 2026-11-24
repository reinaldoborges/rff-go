#!/bin/sh
set -eu

if [ -z "${RFF_MYSQL_DSN:-}" ]; then
  printf 'RFF_MYSQL_DSN must point to a disposable MySQL 8.4 database\n' >&2
  exit 1
fi

go test ./adapters/mysql -count=1
go test -race ./adapters/mysql -count=1
go test ./examples/renewal -run '^TestMySQLHostScenarioAndProcessRestart$' -count=1 -v
go test ./examples/renewal -run '^TestBatchMySQLScenarioAndProcessRestart$' -count=1 -v
