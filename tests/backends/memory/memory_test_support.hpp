#pragma once

#include "mt/backends/memory.hpp"
#include "mt/core.hpp"

#include <cassert>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#define EXPECT_TRUE(expr) assert((expr))
#define EXPECT_FALSE(expr) assert(!(expr))
#define EXPECT_EQ(a, b) assert((a) == (b))

#define EXPECT_THROW_AS(statement, exception_type)                                                 \
    do                                                                                             \
    {                                                                                              \
        bool did_throw = false;                                                                    \
        try                                                                                        \
        {                                                                                          \
            statement;                                                                             \
        }                                                                                          \
        catch (const exception_type&)                                                              \
        {                                                                                          \
            did_throw = true;                                                                      \
        }                                                                                          \
        assert(did_throw && "expected exception not thrown");                                      \
    } while (false)

namespace memory_test_support
{

struct User
{
    std::string id;
    std::string email;
    std::string name;
    bool active = true;
    std::int64_t login_count = 0;
};

struct UserMapping
{
    static constexpr std::string_view table_name = "users";
    static constexpr int schema_version = 1;

    static std::string key(const User& user)
    {
        return user.id;
    }

    static mt::Json to_json(const User& user)
    {
        return mt::Json::object(
            {{"id", user.id},
             {"email", user.email},
             {"name", user.name},
             {"active", user.active},
             {"login_count", user.login_count}}
        );
    }

    static User from_json(const mt::Json& json)
    {
        return User{
            .id = json["id"].as_string(),
            .email = json["email"].as_string(),
            .name = json["name"].as_string(),
            .active = json["active"].as_bool(),
            .login_count = json["login_count"].as_int64()
        };
    }

    static std::vector<mt::IndexSpec> indexes()
    {
        return {
            mt::IndexSpec::json_path_index("email", "$.email").make_unique(),
            mt::IndexSpec::json_path_index("active", "$.active")
        };
    }
};

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
        .value = UserMapping::to_json(
            User{
                .id = key,
                .email = std::move(email),
                .name = key,
                .active = active,
                .login_count = 0
            }
        ),
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
        .schema_version = 1,
        .key_field = "id",
        .fields = {
            mt::FieldSpec::string("id"), mt::FieldSpec::string("email"),
            mt::FieldSpec::boolean("active").mark_required(false)
        }
    };
}

} // namespace memory_test_support
