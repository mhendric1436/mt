# Agent Guidelines

## Project Shape

This repository is a small C++20 micro-transaction core library.

- `include/mt/core.hpp` is the umbrella header for the full core API.
- `include/mt/json.hpp`, `include/mt/errors.hpp`, `include/mt/query.hpp`, `include/mt/collection.hpp`, and `include/mt/types.hpp` contain passive public types.
- `include/mt/backend.hpp`, `include/mt/metadata_cache.hpp`, and `include/mt/database.hpp` contain backend and database shell interfaces.
- `include/mt/transaction.hpp` contains transaction state, validation, retry policy, and `TransactionProvider`.
- `include/mt/table.hpp` contains the mapping concept, `TableProvider`, and typed `Table` facade.
- `include/mt/memory_backend.hpp` contains the in-memory backend used for tests and local development.
- `tools/mt_codegen.py` generates row and mapping headers from user-owned JSON metadata.
- `examples/schemas/user.mt.json` is an example schema used for documentation and tests.
- `tests/mt_core_tests.cpp` contains the current test suite.
- `tests/mt_codegen_tests.cpp` contains generated-code tests.
- `src/mt_core.cpp` contains the header syntax-check translation unit.
- `Makefile` builds and runs the checks.

## Build And Test

Use the existing Makefile targets:

```sh
make check
```

This runs a header syntax check through `src/mt_core.cpp` and the test binaries.

Useful targeted commands:

```sh
make build
make test
make clean
```

## Working Rules

- Do not make code changes when the request is analysis-only.
- Prefer small, focused changes that match the existing header-only style.
- Do not introduce external dependencies unless explicitly requested.
- Preserve C++20 compatibility.
- Use `rg`/`rg --files` for search.
- Run `make check` after code changes when feasible.
- Keep repository schemas limited to examples/test fixtures; users provide their own local schemas to `tools/mt_codegen.py`.

## Current Design Notes

- `mt::Json` is a small built-in JSON value type with stable canonical hashing.
- Transactional point reads support read-your-own-writes.
- Transactional `list` and `query` overlay pending transaction writes for read-your-writes semantics.
- The memory backend applies key-prefix query filtering before pagination; JSON predicates and index semantics are not fully implemented.
- The memory backend is intended for tests and local development, not as a production storage engine.

## Suggested Fix Priority

1. Implement or explicitly reject unsupported query predicates.
2. Expand tests around JSON predicates, unsupported-query behavior, and ordering semantics.
3. Improve `TransactionProvider::retry` ergonomics after behavioral fixes.

## Decomposition Layout

Public headers live under `include/mt`. Prefer narrower includes for implementation files when possible:

- `mt/errors.hpp`
- `mt/types.hpp`
- `mt/query.hpp`
- `mt/collection.hpp`
- `mt/backend.hpp`
- `mt/metadata_cache.hpp`
- `mt/transaction.hpp`
- `mt/table.hpp`
