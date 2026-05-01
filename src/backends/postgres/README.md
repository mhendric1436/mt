# PostgreSQL Backend

This directory is reserved for the future optional PostgreSQL backend implementation.

The public dependency-free skeleton currently lives at:

```text
include/mt/backends/postgres.hpp
```

Do not add a PostgreSQL client dependency to the core library build.

Optional backend implementation and tests should use `libpq` through `pkg-config`.
`make postgres-check` skips when `MT_POSTGRES_TEST_DSN` is unset. When the variable is
set, the PostgreSQL test harness builds with `POSTGRES_CFLAGS` and `POSTGRES_LIBS` and
connects to the configured database.

If `libpq` is installed outside the default `pkg-config` search path, set
`PKG_CONFIG_PATH` before running PostgreSQL targets.
