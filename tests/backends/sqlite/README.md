# SQLite Backend Tests

This directory contains tests for the optional SQLite backend. These tests are
run by the SQLite-specific Makefile target:

```text
make sqlite-check
```

SQLite-specific dependencies stay out of the default core `make check` path.

## Test Layout

```text
sqlite_backend_tests.cpp        Test runner and main().
sqlite_test_support.hpp         Shared generated User row helpers, assertions,
                                temp database paths, and schema/hash helpers.
sqlite_backend_bootstrap_tests.cpp
                                Capabilities, unsupported operations, and private
                                metadata bootstrap behavior.
sqlite_backend_collection_tests.cpp
                                Collection creation, persistence, and compatible
                                or incompatible schema changes.
sqlite_backend_session_tests.cpp
                                Session lifecycle, clock behavior, transaction
                                ids, and active transaction metadata.
sqlite_backend_document_tests.cpp
                                Document history/current writes, snapshot reads,
                                tombstones, list/query behavior, and unique
                                index enforcement.
sqlite_backend_core_tests.cpp   Typed table/core integration over the SQLite
                                backend using generated User metadata.
sqlite_detail_tests.cpp         Low-level SQLite Connection and Statement RAII
                                helper behavior.
```
