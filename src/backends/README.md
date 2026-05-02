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
