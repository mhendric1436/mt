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
- backend-neutral transaction provider
- snapshot reads
- optimistic read/write conflict detection
- predicate read conflict detection
- read-your-writes for point reads, `list`, and key-prefix `query`
- small built-in JSON value type with stable canonical hashing
- in-memory backend for tests and local development

Known limitations:

- the memory backend is not a production storage engine
- memory backend query filtering currently supports key-prefix predicates only
- JSON query predicates and index semantics are not fully implemented
- migrations are modeled but not applied by the memory backend
- `TransactionProvider::retry` currently requires a named callable

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
make clean
```

`make check` syntax-checks the public headers through `mt_core.cpp` and runs the test
binary.

## Quick Example

```cpp
#include "mt_core.hpp"
#include "mt_memory_backend.hpp"

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
    auto backend = std::make_shared<mt::memory::MemoryBackend>();
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

- `mt_core.hpp`: umbrella header for the full public API
- `mt_json.hpp`: JSON value and stable hashing
- `mt_errors.hpp`: exception types
- `mt_query.hpp`: query, list, and index model
- `mt_collection.hpp`: collection descriptors and migration specs
- `mt_types.hpp`: document envelopes and write envelopes
- `mt_backend.hpp`: backend interfaces
- `mt_database.hpp`: database facade
- `mt_transaction.hpp`: transaction state, validation, retry provider
- `mt_table.hpp`: mapping concept and typed table facade
- `mt_memory_backend.hpp`: in-memory backend
- `mt_core_tests.cpp`: test suite
- `mt_core.cpp`: header syntax-check translation unit

## Backend Model

Backends implement two interfaces:

- `mt::IDatabaseBackend`
- `mt::IBackendSession`

The core expects the backend to provide snapshot reads, current metadata reads,
history insertion, current-row upserts, clock/version operations, and backend-specific
transaction IDs.

For production storage engines, implement these interfaces in a separate backend module
and include `mt_backend.hpp` rather than the full umbrella header when possible.

## Formatting

The project uses `.clang-format`.

```sh
make format
```

## Contributing

Keep changes small and covered by `make check`. Prefer extending the existing backend
interfaces and tests before changing public API behavior.

Good first areas:

- fix memory backend query filtering so predicates apply before pagination
- implement or explicitly reject unsupported query predicates
- add JSON predicate tests
- improve retry callable ergonomics

## License

No license has been selected yet. Add a license before publishing this repository for
public reuse.
