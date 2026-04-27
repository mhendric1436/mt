# Backend Tests

Backend-specific tests belong in backend-specific subdirectories here.

Core tests should stay in `tests/` and must not require optional external services.
Future SQLite and PostgreSQL tests should be separately gated by build flags or
environment variables.
