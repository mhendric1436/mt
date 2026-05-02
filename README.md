![mt logo and project overview](images/mt_combined.png)

# mt

`mt` is a small C++20 micro-transaction core library. It provides typed table access,
snapshot-style reads, optimistic conflict detection, predicate read validation, and a
backend interface for plugging in storage engines.

The core is backend-agnostic: it contains no SQL and no database-specific types. The
repository includes an in-memory backend for tests, local development, and intentional
process-local use cases.

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
- read-your-writes for point reads, `list`, key-prefix `query`, and JSON equality `query`
- small built-in JSON value type with stable canonical hashing
- in-memory backend for tests, local development, and process-local use cases

Known limitations:

- the memory backend is process-local and non-durable; use it only where that lifecycle
  is acceptable
- memory backend query filtering supports key-prefix and JSON equality predicates only
- JSON contains predicates and non-key ordering are explicitly rejected by the memory backend
- migrations are modeled but intentionally unsupported by the memory backend because
  memory state does not persist across process restarts

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

## Using mt

The core library is header-only. Add this repository's `include/` directory to your
compiler include path:

```sh
c++ -std=c++20 -I/path/to/mt/include ...
```

For the full public API, include:

```cpp
#include "mt/core.hpp"
```

For local development, tests, or intentional process-local use cases, include the memory
backend separately:

```cpp
#include "mt/backends/memory.hpp"
```

SQLite has an optional backend implementation under `src/backends/sqlite/`. PostgreSQL
has a partial optional backend implementation under `src/backends/postgres/`. Concrete
backend implementations should remain optional so users of the core library do not need
database client dependencies.

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

Supported field types:

- `string`
- `bool`
- `int64`
- `double`
- `optional`
- `array`
- `object`

Generated mappings expose table name, schema version, key field, field metadata, and
index metadata. Backends use that metadata to track accepted schema snapshots.

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

## Transaction Retry

`TransactionProvider::retry()` is optional. Callers can manage transaction boundaries
directly with `begin()` and `Transaction::commit()` when they need full control.

```cpp
auto tx = txs.begin();
try
{
    auto user = users.require(tx, "user:1");
    user.name = "Alice Updated";
    users.put(tx, user);
    tx.commit();
}
catch (...)
{
    if (tx.is_open())
    {
        tx.abort();
    }
    throw;
}
```

`retry()` wraps the common optimistic-concurrency case where `commit()` throws
`TransactionConflict`. It re-runs the whole transaction body from a fresh snapshot, so it
can fix conflicts caused by concurrent commits that changed data read by the transaction.

Use `retry()` only when the transaction body is safe to run more than once. Avoid
non-idempotent external side effects inside a retry body, such as sending email,
publishing messages, calling external APIs, or charging payments. Those workflows should
handle retries at a higher level.

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
- `include/mt/backends/memory/`: memory backend implementation subheaders
- `include/mt/database.hpp`: database facade
- `include/mt/transaction.hpp`: transaction state, validation, retry provider
- `include/mt/table.hpp`: mapping concept and typed table facade
- `include/mt/backends/sqlite.hpp`: dependency-free SQLite backend public header
- `include/mt/backends/postgres.hpp`: dependency-free PostgreSQL backend public header
- `src/backends/`: optional backend implementation files
- `src/backends/sqlite/README.md`: SQLite backend implementation layout
- `src/backends/postgres/README.md`: PostgreSQL backend implementation layout
- `tests/backends/sqlite/README.md`: SQLite backend test layout
- `tools/mt_codegen.py`: JSON metadata to C++ header generator
- `examples/schemas/user.mt.json`: example schema used by documentation and tests
- `tests/mt_core_tests.cpp`: test suite
- `tests/mt_codegen_tests.cpp`: generated-code test suite
- `src/mt_core.cpp`: header syntax-check translation unit
- `docs/backend_contract.md`: backend implementation contract
- `docs/backend_implementation.md`: backend implementation guide
- `.github/workflows/ci.yml`: GitHub Actions workflow

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
`IDatabaseBackend`. They should also follow `docs/backend_implementation.md` for
directory layout, dependency isolation, capability reporting, and backend test coverage.

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

Application code should include `mt/backends/memory.hpp`. Headers under
`mt/backends/memory/` are decomposition headers for the memory backend implementation
and are not the stable include boundary for users.

It can be useful for application-owned caches, ephemeral projections, or single-process
embedded workflows. It is not a durable shared backend for restart-safe persistence or
cross-process consistency.

SQLite is implemented as an optional backend with source layout documentation in
`src/backends/sqlite/README.md` and test layout documentation in
`tests/backends/sqlite/README.md`. PostgreSQL has a partial optional backend
implementation and remains gated so core users do not need libpq.

## Schema Evolution

`mt` treats generated schema metadata as the source for automatic, backwards-compatible
schema evolution. Users do not write separate migration files for simple compatible
changes. When a table is opened, the backend compares the requested schema with the last
accepted schema snapshot for that collection.

Accepted changes currently include:

- adding an `optional` field
- adding a non-required field
- adding a field with a default value
- applying those same additions inside nested object fields

Rejected changes currently include:

- changing the key field
- removing a field
- changing a field type
- changing an `optional` or `array` value type
- changing a default value
- making an existing non-required field required
- adding a required field without a default

The memory backend stores accepted schema snapshots in process-local state and enforces
the same compatibility rules during a process lifetime. It intentionally does not
support migrations: after a process restart there is no persisted memory schema or data
to migrate, and tables are recreated from the latest row/mapping code in the new binary.
Durable backends should store snapshots in private backend metadata tables and apply the
compare/update atomically. See `docs/backend_contract.md` and
`docs/backend_implementation.md` for backend requirements.

## Documentation Diagrams

PlantUML diagrams live under `docs/`.

```sh
make docs-png
make clean-docs
```

## Formatting

The project uses `.clang-format`.

```sh
make format
```

## Contributing

See `CONTRIBUTING.md`.

## License

MIT. See `LICENSE`.
