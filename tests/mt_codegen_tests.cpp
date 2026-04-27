#include "../build/generated/user.hpp"

#include "mt/backends/memory.hpp"

#include <cassert>
#include <iostream>
#include <memory>
#include <string>

#define EXPECT_TRUE(expr) assert((expr))
#define EXPECT_EQ(a, b) assert((a) == (b))

void test_generated_user_mapping_round_trips()
{
    mt_examples::User user{
        .id = "user:1",
        .email = "alice@example.com",
        .name = "Alice",
        .active = false,
        .login_count = 7
    };

    auto json = mt_examples::UserMapping::to_json(user);
    auto decoded = mt_examples::UserMapping::from_json(json);

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
                        .active = true,
                        .login_count = 3
                    }
            );
        }
    );

    auto loaded = users.require("user:1");
    EXPECT_EQ(loaded.email, std::string("alice@example.com"));
    EXPECT_EQ(loaded.login_count, std::int64_t{3});

    auto indexes = mt_examples::UserMapping::indexes();
    EXPECT_EQ(indexes.size(), std::size_t{2});
    EXPECT_TRUE(indexes[0].unique);
}

int main()
{
    test_generated_user_mapping_round_trips();
    test_generated_user_table_works_with_memory_backend();

    std::cout << "All mt_codegen tests passed.\n";
    return 0;
}
