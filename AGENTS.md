# Agent Guidelines

## Project Shape

This repository is a small C++20 micro-transaction core library.

- `include/mt/core.hpp` is the umbrella header for the full core API.
- `include/mt/json.hpp`, `include/mt/hash.hpp`, `include/mt/errors.hpp`, `include/mt/query.hpp`, `include/mt/collection.hpp`, and `include/mt/types.hpp` contain passive public types.
- `include/mt/backend.hpp`, `include/mt/metadata_cache.hpp`, and `include/mt/database.hpp` contain backend and database shell interfaces.
- `include/mt/transaction.hpp` contains transaction state, validation, retry policy, and `TransactionProvider`.
- `include/mt/table.hpp` contains the mapping concept, `TableProvider`, and typed `Table` facade.
- `include/mt/backends/memory.hpp` is the public include for the process-local, non-durable in-memory backend used for tests, local development, and application-owned ephemeral use cases.
- `include/mt/backends/memory/` contains memory backend decomposition headers. Treat these as implementation structure; user-facing code should include `mt/backends/memory.hpp`.
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

## C++ Parameter Passing

Rule of thumb:

- Use by value for cheap scalar types or when the function takes ownership.
- Use `const T&` for larger read-only objects.
- Use `T&` only when mutating the caller's object.
- Use `std::string_view` by value for non-owning string inputs.
- Use `std::unique_ptr<T>` by value for ownership transfer.
- Use `std::shared_ptr<T>` by value when the callee stores/shared-owns it.

Repo-specific guidance:

- `CollectionId`, `Version`, `bool`, and enum values: pass by value.
- `std::string`, `Key`, and `TxId`: pass by value when storing/moving; otherwise prefer `std::string_view` or `const std::string&`.
- `Json`: pass by `const Json&` for reading; pass by value when storing/moving.
- `QuerySpec`, `ListOptions`, and `WriteEnvelope`: pass by `const&` for reading; pass by value only when intentionally storing or moving a copy.

## Current Design Notes

- `mt::Json` is a small built-in JSON value type with stable canonical hashing.
- Transactional point reads support read-your-own-writes.
- Transactional `list` and `query` overlay pending transaction writes for read-your-writes semantics.
- The memory backend supports key-prefix and JSON equality predicates, applies filtering before pagination, enforces unique indexes, and rejects unsupported query/migration features explicitly.
- The memory backend is process-local and non-durable; it may be appropriate for application-owned caches, ephemeral projections, or single-process embedded workflows where that lifecycle is acceptable.

## Suggested Fix Priority

1. Expand tests around remaining backend behavior and ordering semantics.
2. Improve `TransactionProvider::retry` ergonomics after behavioral fixes.

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

Backend users should include the backend public wrapper, such as
`mt/backends/memory.hpp`. Subheaders below `mt/backends/<backend>/` are for backend
implementation decomposition unless that backend explicitly documents them otherwise.
