# Backend Implementations

Backend implementation files belong in backend-specific subdirectories here when a
backend needs non-header implementation code.

Current backend status:

- `memory`: header-only, implemented in `include/mt/backends/memory.hpp`
- `sqlite`: dependency-free public skeleton in `include/mt/backends/sqlite.hpp`;
  implementation planned under `src/backends/sqlite/`
- `postgres`: dependency-free public skeleton in `include/mt/backends/postgres.hpp`;
  implementation planned under `src/backends/postgres/`
