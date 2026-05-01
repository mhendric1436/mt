#pragma once

#include "../../build/generated/user.hpp"

#include "mt/collection.hpp"
#include "mt/hash.hpp"
#include "mt/json.hpp"

#include <cassert>
#include <cstdint>
#include <string>
#include <utility>

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

inline mt::Hash test_hash(std::uint8_t value)
{
    return mt::Hash{.bytes = {value, static_cast<std::uint8_t>(value + 1)}};
}

using BackendTestUser = mt_examples::User;
using BackendTestUserMapping = mt_examples::UserMapping;

inline BackendTestUser backend_test_user(
    std::string id,
    std::string email,
    bool active = true,
    std::string name = "Backend Test User"
)
{
    return BackendTestUser{
        .id = std::move(id),
        .email = std::move(email),
        .name = std::move(name),
        .tags = {},
        .address = mt_examples::Address{.city = "Denver", .postal_code = "80202", .labels = {}},
        .active = active
    };
}

inline mt::Json backend_test_user_json(
    std::string id,
    std::string email,
    bool active = true,
    std::string name = "Backend Test User"
)
{
    return BackendTestUserMapping::to_json(
        backend_test_user(std::move(id), std::move(email), active, std::move(name))
    );
}

inline mt::CollectionSpec backend_test_user_schema(
    std::string logical_name = std::string(BackendTestUserMapping::table_name),
    int schema_version = BackendTestUserMapping::schema_version
)
{
    return mt::CollectionSpec{
        .logical_name = std::move(logical_name),
        .indexes = BackendTestUserMapping::indexes(),
        .schema_version = schema_version,
        .key_field = std::string(BackendTestUserMapping::key_field),
        .fields = BackendTestUserMapping::fields()
    };
}
