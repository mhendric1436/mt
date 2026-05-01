# Backend Tests

Backend-specific tests belong in backend-specific subdirectories here.

Core tests should stay in `tests/` and must not require optional external services.
The memory backend tests are dependency-free and run with `make test` or
`make memory-check`. SQLite and PostgreSQL tests should be separately gated by
build flags or environment variables when they require optional dependencies.

Current dependency-free PostgreSQL skeleton coverage lives in
`tests/mt_core_tests.cpp`.
