# PostgreSQL Backend

This directory contains the optional PostgreSQL backend implementation.

The public dependency-free backend header lives at:

```text
include/mt/backends/postgres.hpp
```

Do not add a PostgreSQL client dependency to the core library build.

Current implementation status:

- libpq connection wrapper
- backend state and public backend entry points
- private table bootstrap
- collection metadata and schema snapshots
- session lifecycle, clock, transaction id, and active transaction metadata
- document history/current writes, point reads, list, query, and unique index enforcement
- transaction/table integration through generated mappings

Schema behavior:

- Compatible schema changes are accepted by comparing stored private schema snapshots with
  requested JSON metadata and updating the snapshot inside the backend transaction.
- Incompatible schema changes are rejected before descriptor or snapshot metadata changes.
- Explicit user-defined `Migration` transforms are not supported by this backend yet and
  are rejected rather than ignored.

Optional backend implementation and tests should use `libpq` through `pkg-config`.
`make postgres-check` skips when `MT_POSTGRES_TEST_DSN` is unset. When the variable is
set, the PostgreSQL test harness builds with `POSTGRES_CFLAGS` and `POSTGRES_LIBS` and
connects to the configured database.

If `libpq` is installed outside the default `pkg-config` search path, set
`PKG_CONFIG_PATH` before running PostgreSQL targets.

Local test setup:

```sh
make postgres-configure-bash-profile
source ~/.bash_profile
make postgres-check
```

The test target uses `MT_POSTGRES_TEST_DSN` and creates the configured `mt_test`
database when it is missing. The database is treated as project-owned test state and
the test harness resets mt private tables on each run.
