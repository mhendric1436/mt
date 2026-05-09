# Backend Implementations

Backend implementation files belong in backend-specific subdirectories here when a
backend needs non-header implementation code.

Current backend status:

- `memory`: header-only, implemented in `include/mt/backends/memory.hpp`
- `sqlite`: dependency-free public header in `include/mt/backends/sqlite.hpp`;
  optional implementation under `src/backends/sqlite/`
- `postgres`: dependency-free public header in `include/mt/backends/postgres.hpp`;
  partial optional implementation under `src/backends/postgres/`; optional tests are
  gated by `MT_POSTGRES_TEST_DSN`

Backend storage direction:

- generated mapping `table_name` is the logical user table name
- each logical user table maps to one physical row store named `mt_user_<table_name>`
- backend-private metadata continues to use separate `mt_*` tables
- SQL implementations should quote generated physical identifiers when emitting SQL
