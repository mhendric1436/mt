# Codex Session State

## Current Repository State

- The worktree was clean before this handoff file was created.
- The PostgreSQL backend implementation plan is complete through the operational polish pass.
- The last verified baseline passed:
  - `make format`
  - `make check`
  - `make memory-check`
  - `make sqlite-check`
  - `source ~/.bash_profile && make postgres-build`
  - `make postgres-check`

## PostgreSQL Status

- PostgreSQL now uses `mt_tx_id_seq` for transaction id allocation.
- `mt_clock` row locking is reserved for commit serialization.
- A regression test verifies overlapping Postgres transactions can both commit when they write orthogonal rows.
- Postgres docs and test docs were refreshed with current setup and status.

## Important Design Note

SQLite still starts backend transactions with `BEGIN IMMEDIATE`. That means SQLite takes the write lock at logical transaction start. `TransactionProvider::begin()` also calls `create_transaction_id()` and `register_active_transaction()`, so SQLite currently serializes overlapping write-capable transactions at begin time.

This is expected for the current SQLite implementation, but it differs from the improved Postgres behavior. The next design task is to allow multiple overlapping SQLite logical transactions while still serializing the actual SQLite write/commit phase.

## Next Steps

1. Plan and implement SQLite overlapping write-capable transaction support.
   - Change SQLite transaction start from `BEGIN IMMEDIATE` to `BEGIN DEFERRED`.
   - Avoid writes during logical `TransactionProvider::begin()`.
   - Defer SQLite transaction id allocation and active transaction registration until commit, or make active registration optional for SQLite.
   - Acquire SQLite write lock during commit validation/write phase.
   - Map commit-time `SQLITE_BUSY` to a retryable conflict where appropriate.

2. Add SQLite regression tests.
   - Two overlapping transactions writing different keys can both commit.
   - Overlapping writes to the same key still conflict.
   - Read/write conflicts still abort stale readers.
   - List/query predicate conflicts still work.
   - Transaction ids remain unique if SQLite continues to persist them.

3. Re-run the full verification baseline after SQLite changes.
   - `make format`
   - `make check`
   - `make memory-check`
   - `make sqlite-check`
   - `source ~/.bash_profile && make postgres-build`
   - `make postgres-check`

## Suggested Commit Message For This Handoff

```text
Document Codex handoff state
```
