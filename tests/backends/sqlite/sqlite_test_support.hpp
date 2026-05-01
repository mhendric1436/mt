#pragma once

#include "../backend_test_support.hpp"

#include "../../../build/generated/user.hpp"
#include "mt/backends/sqlite.hpp"
#include "mt/core.hpp"

#include "../../../src/backends/sqlite/sqlite_detail.hpp"

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

using SqliteUser = mt_examples::User;
using SqliteUserMapping = mt_examples::UserMapping;

inline SqliteUser sqlite_user(
    std::string id,
    std::string email,
    bool active = true
)
{
    return SqliteUser{
        .id = std::move(id),
        .email = std::move(email),
        .name = "SQLite Test User",
        .tags = {},
        .address = mt_examples::Address{.city = "Denver", .postal_code = "80202", .labels = {}},
        .active = active
    };
}

inline mt::Json sqlite_user_json(
    std::string id,
    std::string email,
    bool active = true
)
{
    return SqliteUserMapping::to_json(sqlite_user(std::move(id), std::move(email), active));
}

inline std::filesystem::path sqlite_test_path(std::string_view name)
{
    auto path = std::filesystem::temp_directory_path() / std::string(name);
    std::filesystem::remove(path);
    return path;
}

inline mt::CollectionSpec sqlite_user_schema(int schema_version = 1)
{
    return mt::CollectionSpec{
        .logical_name = std::string(SqliteUserMapping::table_name),
        .indexes = SqliteUserMapping::indexes(),
        .schema_version = schema_version,
        .key_field = std::string(SqliteUserMapping::key_field),
        .fields = SqliteUserMapping::fields()
    };
}

inline mt::Hash test_hash(std::uint8_t value)
{
    return mt::Hash{.bytes = {value, static_cast<std::uint8_t>(value + 1)}};
}
