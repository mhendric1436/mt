# Backend Tests

Backend-specific tests belong in backend-specific subdirectories here.

Core tests should stay in `tests/` and must not require optional external services.
Future SQLite and PostgreSQL tests should be separately gated by build flags or
environment variables.

Current dependency-free skeleton coverage lives in `tests/mt_core_tests.cpp`.
