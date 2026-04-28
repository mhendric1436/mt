# Backend Implementation Guide

Backend authors should start with `docs/backend_contract.md`. This guide describes the
expected repository shape and implementation practices for optional storage engines.

## Directory Layout

Public backend headers live under:

```text
include/mt/backends/
```

Backend implementation files live under:

```text
src/backends/<backend>/
```

Backend-specific tests live under:

```text
tests/backends/<backend>/
```

The default `make check` target must remain usable without optional database client
libraries or running database services.

## Dependency Isolation

Keep database client dependencies out of the core library. A backend may add its own
build target, package configuration, or test target, but including `mt/core.hpp` and
running the default checks must not require SQLite, PostgreSQL, or any other production
database dependency.

Dependency-free skeleton headers are acceptable in `include/mt/backends/` when a backend
is planned but not implemented yet.

## Capability Reporting

Every backend must implement `IDatabaseBackend::capabilities()` truthfully.

If a capability is reported as supported, the backend must implement it consistently
across:

- snapshot reads
- current metadata reads
- list/query result filtering
- predicate validation paths
- schema/index operations

Unsupported features should be reported as `false` and still rejected defensively by the
backend if called directly.

## Transaction Lifecycle

Each `IBackendSession` represents one logical backend transaction:

```text
begin_backend_transaction()
read/validate/write operations
commit_backend_transaction() or rollback_backend_transaction()
```

Commit mutations must be staged inside one atomic backend transaction. History rows,
current metadata, and active transaction cleanup must not become partially visible.

Rollback is cleanup for the backend transaction and resources. It is not a compensating
undo mechanism for committed mt changes.

## Required Tests

Backend tests should cover:

- bootstrapping metadata storage
- collection creation and lookup
- transaction ID creation
- strictly increasing commit versions
- snapshot read stability
- current metadata and tombstone visibility
- history/current version alignment
- read/write conflict detection through core transactions
- predicate read validation
- every reported capability
- unsupported feature rejection

Use the memory backend tests as a behavioral reference, but production backends should
also test persistence and database-specific failure handling.
