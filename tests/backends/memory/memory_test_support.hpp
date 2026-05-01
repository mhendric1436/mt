#pragma once

#include "../backend_test_support.hpp"

#include "../../../build/generated/user.hpp"
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

using User = mt_examples::User;
using UserMapping = mt_examples::UserMapping;

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

inline mt::Hash test_hash(std::uint8_t value)
{
    return mt::Hash{.bytes = {value, static_cast<std::uint8_t>(value + 1)}};
}

inline User memory_user(
    std::string id,
    std::string email,
    bool active = true
)
{
    return User{
        .id = std::move(id),
        .email = std::move(email),
        .name = "Memory Test User",
        .tags = {},
        .address = mt_examples::Address{.city = "Denver", .postal_code = "80202", .labels = {}},
        .active = active
    };
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
    return mt::CollectionSpec{
        .logical_name = "schema_users",
        .indexes = UserMapping::indexes(),
        .schema_version = UserMapping::schema_version,
        .key_field = std::string(UserMapping::key_field),
        .fields = UserMapping::fields()
    };
}

} // namespace memory_test_support
