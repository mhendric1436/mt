#include "../build/generated/user.hpp"

#include "mt/backends/memory.hpp"

#include <cassert>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#define EXPECT_TRUE(expr) assert((expr))
#define EXPECT_FALSE(expr) assert(!(expr))
#define EXPECT_EQ(a, b) assert((a) == (b))

void test_generated_user_mapping_round_trips()
{
    mt_examples::User user{
        .id = "user:1",
        .email = "alice@example.com",
        .name = "Alice",
        .nickname = std::string("ally"),
        .tags = {"admin", "tester"},
        .address =
            mt_examples::Address{
                .city = "Denver",
                .postal_code = "80202",
                .unit = std::string("5A"),
                .labels = {"home", "primary"}
            },
        .active = false,
        .login_count = 7
    };

    auto json = mt_examples::UserMapping::to_json(user);
    auto decoded = mt_examples::UserMapping::from_json(json);

    EXPECT_EQ(decoded, user);
    EXPECT_TRUE(decoded.nickname.has_value());
    EXPECT_EQ(*decoded.nickname, std::string("ally"));
    EXPECT_EQ(decoded.tags.size(), std::size_t{2});
    EXPECT_EQ(decoded.tags[0], std::string("admin"));
    EXPECT_EQ(decoded.tags[1], std::string("tester"));
    EXPECT_EQ(decoded.address.city, std::string("Denver"));
    EXPECT_EQ(decoded.address.postal_code, std::string("80202"));
    EXPECT_TRUE(decoded.address.unit.has_value());
    EXPECT_EQ(*decoded.address.unit, std::string("5A"));
    EXPECT_EQ(decoded.address.labels.size(), std::size_t{2});
    EXPECT_EQ(decoded.address.labels[0], std::string("home"));
    EXPECT_EQ(decoded.address.labels[1], std::string("primary"));
}

void test_generated_user_mapping_round_trips_null_optional()
{
    mt_examples::User user{
        .id = "user:2",
        .email = "bob@example.com",
        .name = "Bob",
        .tags = {},
        .address = mt_examples::Address{.city = "Boulder", .postal_code = "80301", .labels = {}},
        .active = true,
        .login_count = 1
    };

    auto json = mt_examples::UserMapping::to_json(user);
    EXPECT_TRUE(json["nickname"].is_null());
    EXPECT_TRUE(json["tags"].is_array());
    EXPECT_TRUE(json["tags"].as_array().empty());

    auto decoded = mt_examples::UserMapping::from_json(json);
    EXPECT_FALSE(decoded.nickname.has_value());
    EXPECT_FALSE(decoded.address.unit.has_value());
    EXPECT_TRUE(decoded.address.labels.empty());
    EXPECT_EQ(decoded, user);
}

void test_generated_user_table_works_with_memory_backend()
{
    auto backend = std::make_shared<mt::backends::memory::MemoryBackend>();
    mt::Database db{backend};
    mt::TransactionProvider txs{db};
    mt::TableProvider tables{db};

    auto users = tables.table<mt_examples::User, mt_examples::UserMapping>();

    txs.run(
        [&](mt::Transaction& tx)
        {
            users.put(
                tx, mt_examples::User{
                        .id = "user:1",
                        .email = "alice@example.com",
                        .name = "Alice",
                        .nickname = std::string("ally"),
                        .tags = {"admin", "tester"},
                        .address =
                            mt_examples::Address{
                                .city = "Denver",
                                .postal_code = "80202",
                                .unit = std::string("5A"),
                                .labels = {"home", "primary"}
                            },
                        .active = true,
                        .login_count = 3
                    }
            );
        }
    );

    auto loaded = users.require("user:1");
    EXPECT_EQ(loaded.email, std::string("alice@example.com"));
    EXPECT_TRUE(loaded.nickname.has_value());
    EXPECT_EQ(*loaded.nickname, std::string("ally"));
    EXPECT_EQ(loaded.tags.size(), std::size_t{2});
    EXPECT_EQ(loaded.tags[0], std::string("admin"));
    EXPECT_EQ(loaded.tags[1], std::string("tester"));
    EXPECT_EQ(loaded.address.city, std::string("Denver"));
    EXPECT_EQ(loaded.address.postal_code, std::string("80202"));
    EXPECT_TRUE(loaded.address.unit.has_value());
    EXPECT_EQ(*loaded.address.unit, std::string("5A"));
    EXPECT_EQ(loaded.address.labels.size(), std::size_t{2});
    EXPECT_EQ(loaded.address.labels[0], std::string("home"));
    EXPECT_EQ(loaded.address.labels[1], std::string("primary"));
    EXPECT_EQ(loaded.login_count, std::int64_t{3});

    auto indexes = mt_examples::UserMapping::indexes();
    EXPECT_EQ(indexes.size(), std::size_t{2});
    EXPECT_TRUE(indexes[0].unique);
}

int main()
{
    test_generated_user_mapping_round_trips();
    test_generated_user_mapping_round_trips_null_optional();
    test_generated_user_table_works_with_memory_backend();

    std::cout << "All mt_codegen tests passed.\n";
    return 0;
}
