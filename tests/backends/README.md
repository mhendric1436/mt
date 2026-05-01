# Backend Tests

Backend-specific tests belong in backend-specific subdirectories here.

## Shared Support

`backend_test_support.hpp` contains backend-neutral test helpers:

- assertion macros
- `test_hash(std::uint8_t)`
- generated example user fixture helpers used across backend suites

Backend-specific support headers should keep setup that depends on a concrete storage
engine, such as backend construction, temporary storage paths, connection helpers, or
backend-specific wrapper names for shared fixtures.

## Test Targets

Core tests should stay in `tests/` and must not require optional external services.
The memory backend tests are dependency-free and run with `make test` or
`make memory-check`. SQLite and PostgreSQL tests should be separately gated by
build flags or environment variables when they require optional dependencies.

Current dependency-free PostgreSQL skeleton coverage lives in
`tests/mt_core_tests.cpp`.
