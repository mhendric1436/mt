#pragma once

#include "../backend_test_support.hpp"

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

using SqliteUser = BackendTestUser;
using SqliteUserMapping = BackendTestUserMapping;

inline SqliteUser sqlite_user(
    std::string id,
    std::string email,
    bool active = true
)
{
    return backend_test_user(std::move(id), std::move(email), active, "SQLite Test User");
}

inline mt::Json sqlite_user_json(
    std::string id,
    std::string email,
    bool active = true
)
{
    return backend_test_user_json(std::move(id), std::move(email), active, "SQLite Test User");
}

inline std::filesystem::path sqlite_test_path(std::string_view name)
{
    auto path = std::filesystem::temp_directory_path() / std::string(name);
    std::filesystem::remove(path);
    return path;
}

inline mt::CollectionSpec sqlite_user_schema(int schema_version = 1)
{
    return backend_test_user_schema(std::string(SqliteUserMapping::table_name), schema_version);
}
