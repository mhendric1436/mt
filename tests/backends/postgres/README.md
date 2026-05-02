# PostgreSQL Backend Tests

This directory contains optional PostgreSQL backend integration tests. They cover private
table bootstrap, schema snapshots, lifecycle/clock behavior, document storage, list/query
behavior, unique indexes, generated table workflows, and overlapping transaction
regressions.

Tests that require PostgreSQL-specific dependencies or a running database should be gated
so `make check` remains usable for core development without optional backend services.

Run PostgreSQL integration checks with:

```sh
MT_POSTGRES_TEST_DSN='postgresql://user:password@localhost:5432/mt_test' make postgres-check
```

Without `MT_POSTGRES_TEST_DSN`, `make postgres-check` prints a skip message and does not
require `libpq`.

When `MT_POSTGRES_TEST_DSN` is set, `pkg-config --libs libpq` must succeed. If Homebrew
installed `libpq` as keg-only, set `PKG_CONFIG_PATH` to its `lib/pkgconfig` directory.

For the usual Homebrew setup:

```sh
make postgres-configure-bash-profile
source ~/.bash_profile
make postgres-check
```
