# Agent Guidelines

## Project Shape

This repository is a small C++20 micro-transaction core library.

- `mt_core.hpp` contains the backend-agnostic transaction core, public table API, query model, mapping concept, and backend interfaces.
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

- `mt::Json` and `hash_json` are placeholders. Treat value-level behavior and hash-based checks cautiously until a real JSON representation and stable hash are introduced.
- Transactional point reads support read-your-own-writes.
- Transactional `list` and `query` currently return snapshot rows without overlaying pending transaction writes.
- The memory backend only implements key-prefix query filtering; JSON predicates and index semantics are not fully implemented.
- The memory backend is intended for tests and local development, not as a production storage engine.

## Suggested Fix Priority

1. Replace placeholder JSON and hashing with real value semantics.
2. Add pending-write overlays for transactional `list` and `query`.
3. Fix memory backend query filtering so predicates apply before limit/pagination.
4. Implement or explicitly reject unsupported query predicates.
5. Expand tests around value round trips, query/list overlays, deletes, limits, and JSON predicates.
6. Improve `TransactionProvider::retry` ergonomics after behavioral fixes.

## Decomposition Direction

If decomposing `mt_core.hpp`, keep a compatibility umbrella header and extract low-risk passive headers first:

- `errors.hpp`
- `types.hpp`
- `query.hpp`
- `collection.hpp`
- `backend.hpp`
- `metadata_cache.hpp`
- `transaction.hpp`
- `table.hpp`

Move behavior-heavy pieces after passive types and backend interfaces are separated.
