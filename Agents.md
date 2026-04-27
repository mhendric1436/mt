# Agent Guidelines

## Project Shape

This repository is a small C++20 micro-transaction core library.

- `mt_core.hpp` is the compatibility umbrella header for the full core API.
- `mt_json.hpp`, `mt_errors.hpp`, `mt_query.hpp`, `mt_collection.hpp`, and `mt_types.hpp` contain passive public types.
- `mt_backend.hpp`, `mt_metadata_cache.hpp`, and `mt_database.hpp` contain backend and database shell interfaces.
- `mt_transaction.hpp` contains transaction state, validation, retry policy, and `TransactionProvider`.
- `mt_table.hpp` contains the mapping concept, `TableProvider`, and typed `Table` facade.
- `mt_memory_backend.hpp` contains the in-memory backend used for tests and local development.
- `mt_core_tests.cpp` contains the current test suite.
- `Makefile` builds and runs the checks.

## Build And Test

Use the existing Makefile targets:

```sh
make check
```

This runs a header syntax check and the test binary. A `#pragma once in main file` warning from compiling `mt_core.hpp` directly is expected.

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

## Current Design Notes

- `mt::Json` is a small built-in JSON value type with stable canonical hashing.
- Transactional point reads support read-your-own-writes.
- Transactional `list` and `query` overlay pending transaction writes for read-your-writes semantics.
- The memory backend only implements key-prefix query filtering; JSON predicates and index semantics are not fully implemented.
- The memory backend is intended for tests and local development, not as a production storage engine.

## Suggested Fix Priority

1. Fix memory backend query filtering so predicates apply before limit/pagination.
2. Implement or explicitly reject unsupported query predicates.
3. Expand tests around query limits, JSON predicates, and unsupported-query behavior.
4. Improve `TransactionProvider::retry` ergonomics after behavioral fixes.

## Decomposition Layout

`mt_core.hpp` has been decomposed while preserving the original include path. Prefer narrower includes for implementation files when possible:

- `mt_errors.hpp`
- `mt_types.hpp`
- `mt_query.hpp`
- `mt_collection.hpp`
- `mt_backend.hpp`
- `mt_metadata_cache.hpp`
- `mt_transaction.hpp`
- `mt_table.hpp`
