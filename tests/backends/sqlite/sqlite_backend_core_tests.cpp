#include "sqlite_test_support.hpp"

void test_sqlite_core_transactions_persist_across_backend_instances()
{
    auto path = sqlite_test_path("mt_sqlite_core_persistence_test.sqlite");
    auto saved = sqlite_user("user:1", "one@example.com");
    saved.nickname = std::string("one");
    saved.tags = {"sqlite", "generated"};
    saved.address.unit = std::string("5A");
    saved.address.labels = {"home"};
    saved.login_count = 3;

    {
        auto backend = std::make_shared<mt::backends::sqlite::SqliteBackend>(path.string());
        mt::Database db{backend};
        mt::TransactionProvider txs{db};
        mt::TableProvider tables{db};
        auto users = tables.table<SqliteUser, SqliteUserMapping>();

        txs.run([&](mt::Transaction& tx) { users.put(tx, saved); });
    }

    {
        auto backend = std::make_shared<mt::backends::sqlite::SqliteBackend>(path.string());
        mt::Database db{backend};
        mt::TableProvider tables{db};
        auto users = tables.table<SqliteUser, SqliteUserMapping>();

        auto loaded = users.require("user:1");
        EXPECT_EQ(loaded, saved);
    }

    std::filesystem::remove(path);
}

void test_sqlite_core_table_list_query_and_delete()
{
    auto path = sqlite_test_path("mt_sqlite_core_table_test.sqlite");
    auto backend = std::make_shared<mt::backends::sqlite::SqliteBackend>(path.string());
    mt::Database db{backend};
    mt::TransactionProvider txs{db};
    mt::TableProvider tables{db};
    auto users = tables.table<SqliteUser, SqliteUserMapping>();

    txs.run(
        [&](mt::Transaction& tx)
        {
            users.put(tx, sqlite_user("user:1", "one@example.com"));
            users.put(tx, sqlite_user("user:2", "two@example.com", false));
            users.put(tx, sqlite_user("other:1", "other@example.com"));
        }
    );

    auto listed = users.list(mt::ListOptions{.limit = 2});
    EXPECT_EQ(listed.size(), std::size_t{2});
    EXPECT_EQ(listed[0].id, std::string("other:1"));
    EXPECT_EQ(listed[1].id, std::string("user:1"));

    auto queried = users.query(mt::QuerySpec::key_prefix("user:"));
    EXPECT_EQ(queried.size(), std::size_t{2});
    EXPECT_EQ(queried[0].id, std::string("user:1"));
    EXPECT_EQ(queried[1].id, std::string("user:2"));

    txs.run([&](mt::Transaction& tx) { users.erase(tx, "user:1"); });
    EXPECT_FALSE(users.get("user:1").has_value());

    std::filesystem::remove(path);
}

void test_sqlite_core_transactions_reject_unique_index_conflict()
{
    auto path = sqlite_test_path("mt_sqlite_core_unique_conflict_test.sqlite");
    auto backend = std::make_shared<mt::backends::sqlite::SqliteBackend>(path.string());
    mt::Database db{backend};
    mt::TransactionProvider txs{db};
    mt::TableProvider tables{db};
    auto users = tables.table<SqliteUser, SqliteUserMapping>();

    txs.run([&](mt::Transaction& tx) { users.put(tx, sqlite_user("user:1", "same@example.com")); });

    EXPECT_THROW_AS(
        txs.run([&](mt::Transaction& tx)
                { users.put(tx, sqlite_user("user:2", "same@example.com")); }),
        mt::BackendError
    );

    EXPECT_FALSE(users.get("user:2").has_value());

    std::filesystem::remove(path);
}

void test_sqlite_core_rejects_unsupported_query()
{
    auto path = sqlite_test_path("mt_sqlite_core_unsupported_query_test.sqlite");
    auto backend = std::make_shared<mt::backends::sqlite::SqliteBackend>(path.string());
    mt::Database db{backend};
    mt::TableProvider tables{db};
    auto users = tables.table<SqliteUser, SqliteUserMapping>();

    mt::QuerySpec contains;
    contains.predicates.push_back(
        mt::QueryPredicate{
            .op = mt::QueryOp::JsonContains,
            .path = "$",
            .value = sqlite_user_json("user:1", "one@example.com")
        }
    );

    EXPECT_THROW_AS(users.query(contains), mt::BackendError);

    std::filesystem::remove(path);
}
