# mt

`mt` is a small C++20 micro-transaction core library. It provides typed table access,
snapshot-style reads, optimistic conflict detection, predicate read validation, and a
backend interface for plugging in storage engines.

The core is backend-agnostic: it contains no SQL and no database-specific types. The
repository includes an in-memory backend for tests and local development.

## Status

This project is early-stage. The core transaction model is usable for experimentation,
but the public API and backend contracts may still change.

Implemented:

- typed table facade through user-defined mappings
- code generation for row and mapping headers from JSON metadata
- backend-neutral transaction provider
- snapshot reads
- optimistic read/write conflict detection
- predicate read conflict detection
- read-your-writes for point reads, `list`, and key-prefix `query`
- small built-in JSON value type with stable canonical hashing
- in-memory backend for tests and local development

Known limitations:

- the memory backend is not a production storage engine
- memory backend query filtering supports key-prefix and JSON equality predicates only
- JSON contains predicates and non-key ordering are explicitly rejected by the memory backend
- migrations are modeled but explicitly rejected by the memory backend
- `TransactionProvider::retry` currently requires a named callable

## Key Findings

Current priority areas before treating `mt` as a stable library:

1. Improve predicate and query validation, especially around read-your-writes behavior
   for JSON predicates and backend-specific query limitations.
2. Add production backend skeletons, starting with SQLite or PostgreSQL, to prove the
   interface outside the in-memory backend.
3. Expand generated schema validation so invalid metadata fails with clear diagnostics
   before C++ generation.
4. Polish the public API, including retry callable ergonomics, constness, parameter
   passing, and table/query construction.
5. Add open-source project infrastructure such as a license, CI workflow, contribution
   guide, install guidance, and backend implementation documentation.

## Requirements

- C++20 compiler
- `make`
- `clang-format` for formatting

The default build uses `c++`, but you can override it:

```sh
make CXX=clang++
```

## Build And Test

Run the full check:

```sh
make check
```

Useful targets:

```sh
make build
make test
make format
make docs-png
make clean-docs
make clean
```

`make check` syntax-checks the public headers through `src/mt_core.cpp` and runs the test
binaries.

## Code Generation

`tools/mt_codegen.py` generates a row struct and `FooMapping` class from JSON metadata.
The library repository only includes example schemas for documentation and tests. Users
should keep their application schemas in their own project and pass those local paths to
the generator.

Example:

```sh
python3 tools/mt_codegen.py ./my_schemas/user.mt.json -o ./generated/user.hpp
```

The example schema in this repository lives at:

```text
examples/schemas/user.mt.json
```

`make check` generates this example into `build/generated/user.hpp` and compiles a small
test against it. Generated files under `build/` are build artifacts and are not intended
to be committed.

Supported field types in the first generator version:

- `string`
- `bool`
- `int64`
- `double`

## Quick Example

```cpp
#include "mt/core.hpp"
#include "mt/backends/memory.hpp"

#include <memory>
#include <string>
#include <string_view>

struct User
{
    std::string id;
    std::string email;
    std::string name;
};

struct UserMapping
{
    static constexpr std::string_view table_name = "users";

    static std::string key(const User& user)
    {
        return user.id;
    }

    static mt::Json to_json(const User& user)
    {
        return mt::Json::object(
            {
                {"id", user.id},
                {"email", user.email},
                {"name", user.name}
            }
        );
    }

    static User from_json(const mt::Json& json)
    {
        return User{
            .id = json["id"].as_string(),
            .email = json["email"].as_string(),
            .name = json["name"].as_string()
        };
    }
};

int main()
{
    auto backend = std::make_shared<mt::backends::memory::MemoryBackend>();
    mt::Database db{backend};
    mt::TransactionProvider txs{db};
    mt::TableProvider tables{db};

    auto users = tables.table<User, UserMapping>();

    txs.run(
        [&](mt::Transaction& tx)
        {
            users.put(
                tx,
                User{
                    .id = "user:1",
                    .email = "alice@example.com",
                    .name = "Alice"
                }
            );
        }
    );

    auto alice = users.require("user:1");
}
```

## Repository Layout

- `include/mt/core.hpp`: umbrella header for the full public API
- `include/mt/json.hpp`: JSON value and stable hashing
- `include/mt/hash.hpp`: hash value type
- `include/mt/errors.hpp`: exception types
- `include/mt/query.hpp`: query, list, and index model
- `include/mt/collection.hpp`: collection descriptors and migration specs
- `include/mt/types.hpp`: document envelopes and write envelopes
- `include/mt/backend.hpp`: backend interfaces
- `include/mt/backends/memory.hpp`: in-memory backend
- `include/mt/database.hpp`: database facade
- `include/mt/transaction.hpp`: transaction state, validation, retry provider
- `include/mt/table.hpp`: mapping concept and typed table facade
- `src/backends/`: optional backend implementation files
- `tools/mt_codegen.py`: JSON metadata to C++ header generator
- `examples/schemas/user.mt.json`: example schema used by documentation and tests
- `tests/mt_core_tests.cpp`: test suite
- `tests/mt_codegen_tests.cpp`: generated-code test suite
- `src/mt_core.cpp`: header syntax-check translation unit
- `docs/backend_contract.md`: backend implementation contract

## Backend Model

Backends implement two interfaces:

- `mt::IDatabaseBackend`
- `mt::IBackendSession`

The core expects the backend to provide snapshot reads, current metadata reads,
history insertion, current-row upserts, clock/version operations, and backend-specific
transaction IDs.

Backends also report `BackendCapabilities`. The typed table API uses those capabilities
to reject unsupported query, ordering, index, and migration features before invoking a
backend session. Backend implementations should still validate defensively.

Backend authors should read `docs/backend_contract.md` before implementing
`IDatabaseBackend`.

For production storage engines, implement these interfaces in a separate backend module
and include `mt/backend.hpp` rather than the full umbrella header when possible.

Backend-specific public headers should live under:

```text
include/mt/backends/
```

Backend-specific implementation and tests should live under:

```text
src/backends/
tests/backends/
```

The memory backend is header-only and available as:

```cpp
#include "mt/backends/memory.hpp"
```

Future SQLite and PostgreSQL backends should be optional so core users do not need those
dependencies.

## Formatting

The project uses `.clang-format`.

```sh
make format
```

## Contributing

Keep changes small and covered by `make check`. Prefer extending the existing backend
interfaces and tests before changing public API behavior.

Good first areas:

- extend JSON predicate support beyond equality
- extend code generation for optional fields and richer JSON shapes
- improve retry callable ergonomics

## License

No license has been selected yet. Add a license before publishing this repository for
public reuse.
