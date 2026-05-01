#pragma once

#include "../backend_test_support.hpp"

#include "mt/backends/memory.hpp"
#include "mt/core.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace memory_test_support
{

using User = BackendTestUser;
using UserMapping = BackendTestUserMapping;

struct MigratingUserMapping : UserMapping
{
    static constexpr std::string_view table_name = "migrating_users";

    static std::vector<mt::Migration> migrations()
    {
        return {mt::Migration{.from_version = 1, .to_version = 2, .transform = [](mt::Json&) {}}};
    }
};

struct Harness
{
    std::shared_ptr<mt::backends::memory::MemoryBackend> backend =
        std::make_shared<mt::backends::memory::MemoryBackend>();
    mt::Database db{backend};
    mt::TransactionProvider txs{db};
    mt::TableProvider tables{db};
    mt::Table<User, UserMapping> users = tables.table<User, UserMapping>();
};

inline User memory_user(
    std::string id,
    std::string email,
    bool active = true
)
{
    return backend_test_user(std::move(id), std::move(email), active, "Memory Test User");
}

inline mt::WriteEnvelope user_write(
    mt::CollectionId collection,
    std::string key,
    std::string email,
    bool active,
    std::uint8_t hash
)
{
    return mt::WriteEnvelope{
        .collection = collection,
        .key = key,
        .kind = mt::WriteKind::Put,
        .value = UserMapping::to_json(memory_user(key, std::move(email), active)),
        .value_hash = test_hash(hash)
    };
}

inline mt::WriteEnvelope delete_write(
    mt::CollectionId collection,
    std::string key,
    std::uint8_t hash
)
{
    return mt::WriteEnvelope{
        .collection = collection,
        .key = std::move(key),
        .kind = mt::WriteKind::Delete,
        .value_hash = test_hash(hash)
    };
}

inline mt::CollectionSpec user_schema_spec()
{
    return backend_test_user_schema("schema_users");
}

} // namespace memory_test_support
