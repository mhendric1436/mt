# Backend Contract

This document defines the contract for implementations of `mt::IDatabaseBackend` and
`mt::IBackendSession`.

The core invariant is:

```text
All mt commit mutations for a transaction must be applied inside one atomic backend
transaction. Partial visibility is forbidden.
```

`insert_history()` and `upsert_current()` must participate in the same backend
transaction. `commit_backend_transaction()` is the visibility boundary. If the backend
commit fails, no mt commit mutations from that transaction may become visible.

`abort_backend_transaction()` aborts or closes the backend transaction and releases
resources. It is not a logical undo API for committed mt changes.

## Core Commit Sequence

Core commits a transaction in this order:

```text
lock commit clock
validate point reads
validate write conflicts
validate predicate reads
allocate commit version
stage each history row
stage each current row
remove active transaction metadata
commit backend transaction
```

The allocated commit version applies to every history row and current metadata row
written by that transaction. History and current state must become visible together when
`commit_backend_transaction()` succeeds.

If validation, staging, cleanup, or backend commit fails, core calls
`abort_backend_transaction()`. A valid backend must ensure that failed commits do not
make staged history rows, current rows, or active transaction cleanup partially visible.

## Session Lifecycle

A normal session follows this flow:

```text
open_session()
begin_backend_transaction()
read, validate, and write operations
commit_backend_transaction() or abort_backend_transaction()
destroy session
```

A session represents one logical backend transaction and is not required to be
thread-safe. Backend implementations may require all session methods except destruction
to be called by one thread.

`abort_backend_transaction()` must be `noexcept`. It should be best-effort cleanup and
should tolerate repeated cleanup attempts where practical. Destructors must not throw.

## Clock And Version Semantics

`read_clock()` returns the latest committed version that can be used as the snapshot
boundary for a new transaction.

`lock_clock_and_read()` serializes commits. A backend may implement this with a database
row lock, advisory lock, compare-and-swap loop, or another mechanism, but only one
committing session may own the commit clock at a time.

`increment_clock_and_return()` creates a new strictly increasing commit version. It may
only be called by the session that owns the clock lock. A transaction's `start_version`
is its snapshot boundary.

## Transaction IDs

`TxId` is backend-owned and opaque to core. Core stores and passes transaction IDs but
must not parse, order, or compare their internal structure.

Transaction IDs only need to be unique within the backend's active or retained
transaction metadata. Distributed backends may use native transaction identifiers,
UUIDs, prefixed counters, or another backend-specific form.

## Read Semantics

`read_snapshot(collection, key, version)` returns the latest document version with a
commit version less than or equal to `version`. It may return a tombstone for a deleted
document. It returns `std::nullopt` when no version is visible.

`read_current_metadata(collection, key)` returns the latest committed metadata for the
document, including tombstones when present. Metadata must be sufficient for conflict
detection without requiring the full JSON value.

`list_snapshot()` and `query_snapshot()` return visible documents at the requested
snapshot version. `list_current_metadata()` and `query_current_metadata()` return latest
metadata using the same predicate semantics as their snapshot counterparts.

Ordering and pagination must match `BackendCapabilities`. When key ordering is reported
as supported, `after_key` pagination must be stable under key ordering.

## Write Semantics

`insert_history()` appends the committed document version or delete tombstone to history.
`upsert_current()` updates the latest current state and metadata.

Core calls both after validation and before `commit_backend_transaction()`. They must
remain invisible outside the backend transaction until the backend commit succeeds.

Unique and index constraints must be enforced atomically with the commit. A backend that
reports a schema capability as supported must apply it consistently for all affected
writes.

## Active Transactions

`register_active_transaction(tx_id, start_version)` records the transaction ID and
snapshot start version. `unregister_active_transaction(tx_id)` is cleanup and should
tolerate missing IDs.

Active transaction metadata is intended for backend cleanup, retention, or future garbage
collection policies. Current core conflict detection does not require scanning active
transactions.

## Capability Reporting

`BackendCapabilities` must be truthful. If a feature is reported as supported, snapshot
and metadata methods must implement it consistently. If a feature is not supported,
public APIs may reject the operation before opening a session.

Backend implementations should still validate defensively. Capability checks improve
discoverability; they do not replace backend-side validation.

## Production Backend Requirements

A production backend must provide:

- atomic backend transactions for all mt commit mutations
- strictly increasing commit versions
- stable snapshot reads by version
- current metadata suitable for conflict detection
- consistent query/list semantics across snapshot and metadata methods
- truthful capability reporting
- best-effort, non-throwing abort cleanup

The in-memory backend is useful for tests and local development, but it is not a
production storage engine.
