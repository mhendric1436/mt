#include "../build/generated/order.hpp"
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
        .metadata = mt::Json::object({{"role", "operator"}}),
        .logins = {mt_examples::Login{.provider = "password", .successful = true}},
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
    EXPECT_EQ(decoded.metadata["role"].as_string(), std::string("operator"));
    EXPECT_EQ(decoded.logins.size(), std::size_t{1});
    EXPECT_EQ(decoded.logins[0].provider, std::string("password"));
    EXPECT_TRUE(decoded.logins[0].successful);
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
        .logins = {},
        .address = mt_examples::Address{.city = "Boulder", .postal_code = "80301", .labels = {}},
        .active = true,
        .login_count = 1
    };

    auto json = mt_examples::UserMapping::to_json(user);
    EXPECT_TRUE(json["nickname"].is_null());
    EXPECT_TRUE(json["tags"].is_array());
    EXPECT_TRUE(json["tags"].as_array().empty());
    EXPECT_TRUE(json["metadata"].is_object());
    EXPECT_TRUE(json["metadata"].as_object().empty());
    EXPECT_TRUE(json["logins"].is_array());
    EXPECT_TRUE(json["logins"].as_array().empty());

    auto decoded = mt_examples::UserMapping::from_json(json);
    EXPECT_FALSE(decoded.nickname.has_value());
    EXPECT_FALSE(decoded.address.unit.has_value());
    EXPECT_TRUE(decoded.address.labels.empty());
    EXPECT_EQ(decoded, user);
}

void test_generated_user_mapping_exposes_schema_metadata()
{
    EXPECT_EQ(std::string(mt_examples::UserMapping::key_field), std::string("id"));

    auto fields = mt_examples::UserMapping::fields();
    EXPECT_EQ(fields.size(), std::size_t{10});

    EXPECT_EQ(fields[0].name, std::string("id"));
    EXPECT_EQ(fields[0].type, mt::FieldType::String);
    EXPECT_TRUE(fields[0].required);

    EXPECT_EQ(fields[3].name, std::string("nickname"));
    EXPECT_EQ(fields[3].type, mt::FieldType::Optional);
    EXPECT_EQ(fields[3].value_type, mt::FieldType::String);
    EXPECT_TRUE(fields[3].required);

    EXPECT_EQ(fields[4].name, std::string("tags"));
    EXPECT_EQ(fields[4].type, mt::FieldType::Array);
    EXPECT_EQ(fields[4].value_type, mt::FieldType::String);

    EXPECT_EQ(fields[5].name, std::string("metadata"));
    EXPECT_EQ(fields[5].type, mt::FieldType::Json);
    EXPECT_TRUE(fields[5].has_default);
    EXPECT_EQ(fields[5].default_value, mt::Json::object({}));

    EXPECT_EQ(fields[6].name, std::string("logins"));
    EXPECT_EQ(fields[6].type, mt::FieldType::Array);
    EXPECT_EQ(fields[6].value_type, mt::FieldType::Object);
    EXPECT_EQ(fields[6].fields.size(), std::size_t{2});
    EXPECT_EQ(fields[6].fields[0].name, std::string("provider"));

    EXPECT_EQ(fields[7].name, std::string("address"));
    EXPECT_EQ(fields[7].type, mt::FieldType::Object);
    EXPECT_EQ(fields[7].fields.size(), std::size_t{4});
    EXPECT_EQ(fields[7].fields[2].name, std::string("unit"));
    EXPECT_EQ(fields[7].fields[2].type, mt::FieldType::Optional);
    EXPECT_EQ(fields[7].fields[3].name, std::string("labels"));
    EXPECT_EQ(fields[7].fields[3].type, mt::FieldType::Array);

    EXPECT_EQ(fields[8].name, std::string("active"));
    EXPECT_EQ(fields[8].type, mt::FieldType::Bool);
    EXPECT_FALSE(fields[8].required);
    EXPECT_TRUE(fields[8].has_default);
    EXPECT_EQ(fields[8].default_value, mt::Json(true));

    EXPECT_EQ(fields[9].name, std::string("login_count"));
    EXPECT_EQ(fields[9].type, mt::FieldType::Int64);
    EXPECT_FALSE(fields[9].required);
    EXPECT_TRUE(fields[9].has_default);
    EXPECT_EQ(fields[9].default_value, mt::Json(std::int64_t{0}));
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
                        .metadata = mt::Json::object({{"role", "operator"}}),
                        .logins = {mt_examples::Login{.provider = "password", .successful = true}},
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
    EXPECT_EQ(loaded.metadata["role"].as_string(), std::string("operator"));
    EXPECT_EQ(loaded.logins.size(), std::size_t{1});
    EXPECT_EQ(loaded.logins[0].provider, std::string("password"));
    EXPECT_TRUE(loaded.logins[0].successful);
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

void test_generated_composite_key_table_works_with_memory_backend()
{
    EXPECT_EQ(std::string(mt_examples::OrderMapping::key_field), std::string("tenant_id:order_id"));

    mt_examples::Order order{
        .tenant_id = "tenant:a", .order_id = "order:1", .status = "open", .total_cents = 1299
    };

    EXPECT_EQ(mt_examples::OrderMapping::key(order), std::string("tenant:a:order:1"));

    auto backend = std::make_shared<mt::backends::memory::MemoryBackend>();
    mt::Database db{backend};
    mt::TransactionProvider txs{db};
    mt::TableProvider tables{db};

    auto orders = tables.table<mt_examples::Order, mt_examples::OrderMapping>();

    txs.run([&](mt::Transaction& tx) { orders.put(tx, order); });

    auto loaded = orders.require("tenant:a:order:1");
    EXPECT_EQ(loaded, order);

    auto indexes = mt_examples::OrderMapping::indexes();
    EXPECT_EQ(indexes.size(), std::size_t{1});
    EXPECT_EQ(indexes[0].name, std::string("status"));
}

int main()
{
    test_generated_user_mapping_round_trips();
    test_generated_user_mapping_round_trips_null_optional();
    test_generated_user_mapping_exposes_schema_metadata();
    test_generated_user_table_works_with_memory_backend();
    test_generated_composite_key_table_works_with_memory_backend();

    std::cout << "All mt_codegen tests passed.\n";
    return 0;
}
