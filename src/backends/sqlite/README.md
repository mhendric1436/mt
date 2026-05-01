# SQLite Backend

This directory contains the optional SQLite backend implementation. The public
dependency-free backend interface lives at:

```text
include/mt/backends/sqlite.hpp
```

Do not add a SQLite client dependency to the core library build.

## Implementation Layout

```text
sqlite_backend.cpp       Public SqliteBackend surface: construction, capabilities,
                         session creation, bootstrap, and collection lookup/update.
sqlite_session.*         Backend session implementation: transaction lifecycle,
                         clocks, tx ids, document reads/writes, list/query operations.
sqlite_state.*           Shared backend state, bootstrap gating, connection setup,
                         and the SQLite :memory: per-connection exception.
sqlite_schema.*          Collection schema metadata storage, field/index
                         serialization, and collection descriptor helpers.
sqlite_document.*        Document encoding helpers: hash text decoding, stored JSON
                         parsing, tombstone checks, and canonical value encoding.
sqlite_constraints.*     Unique JSON index enforcement.
sqlite_detail.hpp        Low-level SQLite RAII wrappers, storage path helpers, and
                         private SQL statement strings.
```

All SQLite implementation files are intentionally private to this optional
backend directory. Public mt headers should remain free of SQLite headers and
link requirements.
