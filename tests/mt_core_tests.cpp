#include "mt/backends/memory.hpp"
#include "mt/core.hpp"

#include <cassert>
#include <iostream>

// -----------------------------------------------------------------------------
// Minimal tests for mt/core.hpp
//
// These tests use a fake in-memory backend that implements the backend
// contracts expected by mt/core.hpp. It is intentionally small and
// deterministic.
//
// Build example:
//   c++ -std=c++20 -Iinclude -Wall -Wextra -pedantic tests/mt_core_tests.cpp -o mt_core_tests
//
// Run:
//   ./mt_core_tests
// -----------------------------------------------------------------------------

namespace test_support
{

// -----------------------------------------------------------------------------
// Test row and JSON mapping
// -----------------------------------------------------------------------------

struct User
{
    std::string id;
    std::string email;
    std::string name;
    bool active = true;
    std::int64_t login_count = 0;

    friend bool operator==(
        const User&,
        const User&
    ) = default;
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

} // namespace test_support

// -----------------------------------------------------------------------------
// Test helpers
// -----------------------------------------------------------------------------

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

using test_support::Harness;
using test_support::MigratingUserMapping;
using test_support::User;
using test_support::UserMapping;

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

void test_table_provider_creates_table()
{
    Harness h;
    EXPECT_EQ(h.users.descriptor().logical_name, std::string("users"));
    EXPECT_TRUE(h.users.descriptor().id != 0);
}

void test_memory_backend_reports_capabilities()
{
    Harness h;
    auto capabilities = h.backend->capabilities();

    EXPECT_TRUE(capabilities.query.key_prefix);
    EXPECT_TRUE(capabilities.query.json_equals);
    EXPECT_FALSE(capabilities.query.json_contains);
    EXPECT_TRUE(capabilities.query.order_by_key);
    EXPECT_FALSE(capabilities.query.custom_ordering);

    EXPECT_TRUE(capabilities.schema.json_indexes);
    EXPECT_TRUE(capabilities.schema.unique_indexes);
    EXPECT_FALSE(capabilities.schema.migrations);
}

void test_backend_contract_transaction_ids_are_non_empty_and_unique()
{
    Harness h;
    auto session = h.backend->open_session();
    session->begin_backend_transaction();

    auto first = session->create_transaction_id();
    auto second = session->create_transaction_id();

    EXPECT_FALSE(first.empty());
    EXPECT_FALSE(second.empty());
    EXPECT_FALSE(first == second);

    session->rollback_backend_transaction();
}

void test_backend_contract_commit_versions_strictly_increase()
{
    Harness h;

    auto first_session = h.backend->open_session();
    first_session->begin_backend_transaction();
    first_session->lock_clock_and_read();
    auto first_version = first_session->increment_clock_and_return();
    first_session->commit_backend_transaction();

    auto second_session = h.backend->open_session();
    second_session->begin_backend_transaction();
    second_session->lock_clock_and_read();
    auto second_version = second_session->increment_clock_and_return();
    second_session->commit_backend_transaction();

    EXPECT_TRUE(second_version > first_version);
}

void test_backend_contract_clock_increment_requires_lock_owner()
{
    Harness h;

    auto unlocked_session = h.backend->open_session();
    unlocked_session->begin_backend_transaction();
    EXPECT_THROW_AS(unlocked_session->increment_clock_and_return(), mt::BackendError);
    unlocked_session->rollback_backend_transaction();

    auto locked_session = h.backend->open_session();
    locked_session->begin_backend_transaction();
    locked_session->lock_clock_and_read();

    auto blocked_session = h.backend->open_session();
    blocked_session->begin_backend_transaction();
    EXPECT_THROW_AS(blocked_session->lock_clock_and_read(), mt::BackendError);

    blocked_session->rollback_backend_transaction();
    locked_session->rollback_backend_transaction();
}

void test_backend_contract_cleanup_tolerates_missing_active_transaction()
{
    Harness h;
    auto session = h.backend->open_session();
    session->begin_backend_transaction();

    session->unregister_active_transaction("missing-transaction");
    session->rollback_backend_transaction();
    session->rollback_backend_transaction();
}

void test_backend_contract_snapshot_reads_remain_stable_after_later_commits()
{
    Harness h;

    h.txs.run(
        [&](mt::Transaction& tx)
        { h.users.put(tx, User{.id = "user:1", .email = "old@example.com", .name = "Old"}); }
    );

    auto session = h.backend->open_session();
    session->begin_backend_transaction();
    auto snapshot_version = session->read_clock();
    session->commit_backend_transaction();

    h.txs.run(
        [&](mt::Transaction& tx)
        { h.users.put(tx, User{.id = "user:1", .email = "new@example.com", .name = "New"}); }
    );

    auto read_session = h.backend->open_session();
    read_session->begin_backend_transaction();
    auto doc = read_session->read_snapshot(h.users.descriptor().id, "user:1", snapshot_version);
    read_session->commit_backend_transaction();

    EXPECT_TRUE(doc.has_value());
    EXPECT_EQ(doc->value["email"].as_string(), std::string("old@example.com"));
}

void test_backend_contract_delete_tombstone_metadata_remains_visible()
{
    Harness h;

    h.txs.run(
        [&](mt::Transaction& tx)
        { h.users.put(tx, User{.id = "user:1", .email = "a@example.com", .name = "Alice"}); }
    );
    h.txs.run([&](mt::Transaction& tx) { h.users.erase(tx, "user:1"); });

    auto session = h.backend->open_session();
    session->begin_backend_transaction();
    auto metadata = session->read_current_metadata(h.users.descriptor().id, "user:1");
    session->commit_backend_transaction();

    EXPECT_TRUE(metadata.has_value());
    EXPECT_TRUE(metadata->deleted);
}

void test_commit_semantics_history_and_current_share_commit_version()
{
    Harness h;

    h.txs.run(
        [&](mt::Transaction& tx)
        { h.users.put(tx, User{.id = "user:1", .email = "a@example.com", .name = "Alice"}); }
    );

    auto session = h.backend->open_session();
    session->begin_backend_transaction();
    auto current = session->read_current_metadata(h.users.descriptor().id, "user:1");
    EXPECT_TRUE(current.has_value());
    auto history = session->read_snapshot(h.users.descriptor().id, "user:1", current->version);
    session->commit_backend_transaction();

    EXPECT_TRUE(history.has_value());
    EXPECT_EQ(history->version, current->version);
    EXPECT_EQ(history->value_hash, current->value_hash);
}

void test_commit_semantics_delete_history_and_current_share_commit_version()
{
    Harness h;

    h.txs.run(
        [&](mt::Transaction& tx)
        { h.users.put(tx, User{.id = "user:1", .email = "a@example.com", .name = "Alice"}); }
    );
    h.txs.run([&](mt::Transaction& tx) { h.users.erase(tx, "user:1"); });

    auto session = h.backend->open_session();
    session->begin_backend_transaction();
    auto current = session->read_current_metadata(h.users.descriptor().id, "user:1");
    EXPECT_TRUE(current.has_value());
    auto history = session->read_snapshot(h.users.descriptor().id, "user:1", current->version);
    session->commit_backend_transaction();

    EXPECT_TRUE(history.has_value());
    EXPECT_TRUE(current->deleted);
    EXPECT_TRUE(history->deleted);
    EXPECT_EQ(history->version, current->version);
}

void test_commit_semantics_conflict_does_not_make_write_visible()
{
    Harness h;

    h.txs.run(
        [&](mt::Transaction& tx)
        { h.users.put(tx, User{.id = "user:1", .email = "a@example.com", .name = "Alice"}); }
    );

    auto conflicting = h.txs.begin();
    auto winner = h.txs.begin();

    auto loaded = h.users.require(conflicting, "user:1");
    loaded.email = "conflict@example.com";

    h.users.put(winner, User{.id = "user:1", .email = "winner@example.com", .name = "Winner"});
    winner.commit();

    h.users.put(conflicting, loaded);
    EXPECT_THROW_AS(conflicting.commit(), mt::TransactionConflict);

    auto after = h.users.require("user:1");
    EXPECT_EQ(after.email, std::string("winner@example.com"));
}

void test_json_values_round_trip_and_hash_stably()
{
    User expected{
        .id = "user:json",
        .email = "json@example.com",
        .name = "Json User",
        .active = false,
        .login_count = 42
    };

    auto user_json = UserMapping::to_json(expected);
    auto decoded = UserMapping::from_json(user_json);
    EXPECT_EQ(decoded, expected);

    auto same_json = UserMapping::to_json(decoded);
    EXPECT_EQ(mt::hash_json(user_json), mt::hash_json(same_json));

    auto changed_json = UserMapping::to_json(
        User{
            .id = "user:json",
            .email = "changed@example.com",
            .name = "Json User",
            .active = false,
            .login_count = 42
        }
    );
    EXPECT_FALSE(mt::hash_json(user_json) == mt::hash_json(changed_json));
}

void test_non_transactional_get_missing_returns_nullopt()
{
    Harness h;
    auto user = h.users.get("user:missing");
    EXPECT_FALSE(user.has_value());
}

void test_transactional_put_then_non_transactional_get()
{
    Harness h;

    h.txs.run(
        [&](mt::Transaction& tx)
        {
            h.users.put(
                tx, User{
                        .id = "user:1",
                        .email = "a@example.com",
                        .name = "Alice",
                        .active = true,
                        .login_count = 1
                    }
            );
        }
    );

    auto loaded = h.users.get("user:1");
    EXPECT_TRUE(loaded.has_value());
    User expected{
        .id = "user:1", .email = "a@example.com", .name = "Alice", .active = true, .login_count = 1
    };
    EXPECT_EQ(*loaded, expected);
}

void test_require_missing_throws()
{
    Harness h;
    EXPECT_THROW_AS(h.users.require("missing"), mt::DocumentNotFound);
}

void test_transactional_read_your_own_point_write()
{
    Harness h;

    h.txs.run(
        [&](mt::Transaction& tx)
        {
            h.users.put(
                tx, User{
                        .id = "user:1",
                        .email = "a@example.com",
                        .name = "Alice",
                        .active = true,
                        .login_count = 1
                    }
            );

            auto loaded = h.users.get(tx, "user:1");
            EXPECT_TRUE(loaded.has_value());
            User expected{
                .id = "user:1",
                .email = "a@example.com",
                .name = "Alice",
                .active = true,
                .login_count = 1
            };
            EXPECT_EQ(*loaded, expected);
        }
    );
}

void test_transactional_list_reads_own_pending_put()
{
    Harness h;

    h.txs.run(
        [&](mt::Transaction& tx)
        {
            h.users.put(tx, User{.id = "user:1", .email = "a@example.com", .name = "Alice"});

            auto listed = h.users.list(tx);
            EXPECT_EQ(listed.size(), std::size_t{1});
            EXPECT_EQ(listed[0].id, std::string("user:1"));
            EXPECT_EQ(listed[0].email, std::string("a@example.com"));
        }
    );
}

void test_transactional_query_reads_own_pending_matching_put()
{
    Harness h;

    h.txs.run(
        [&](mt::Transaction& tx)
        {
            h.users.put(tx, User{.id = "user:1", .email = "a@example.com", .name = "Alice"});
            h.users.put(tx, User{.id = "other:1", .email = "o@example.com", .name = "Other"});

            auto rows = h.users.query(tx, mt::QuerySpec::key_prefix("user:"));
            EXPECT_EQ(rows.size(), std::size_t{1});
            EXPECT_EQ(rows[0].id, std::string("user:1"));
        }
    );
}

void test_transactional_json_equals_query_reads_own_pending_matching_put()
{
    Harness h;

    h.txs.run(
        [&](mt::Transaction& tx)
        {
            h.users.put(tx, User{.id = "user:1", .email = "a@example.com", .name = "Alice"});
            h.users.put(tx, User{.id = "user:2", .email = "b@example.com", .name = "Bob"});

            auto rows = h.users.query(tx, mt::QuerySpec::where_json_eq("$.email", "a@example.com"));
            EXPECT_EQ(rows.size(), std::size_t{1});
            EXPECT_EQ(rows[0].id, std::string("user:1"));
        }
    );
}

void test_transactional_json_equals_query_excludes_pending_non_match()
{
    Harness h;

    h.txs.run(
        [&](mt::Transaction& tx)
        {
            h.users.put(tx, User{.id = "user:1", .email = "a@example.com", .name = "Alice"});

            auto rows = h.users.query(tx, mt::QuerySpec::where_json_eq("$.email", "b@example.com"));
            EXPECT_TRUE(rows.empty());
        }
    );
}

void test_transactional_json_equals_query_reflects_pending_replacement()
{
    Harness h;

    h.txs.run(
        [&](mt::Transaction& tx)
        { h.users.put(tx, User{.id = "user:1", .email = "old@example.com", .name = "Old"}); }
    );

    h.txs.run(
        [&](mt::Transaction& tx)
        {
            h.users.put(tx, User{.id = "user:1", .email = "new@example.com", .name = "New"});

            auto old_rows =
                h.users.query(tx, mt::QuerySpec::where_json_eq("$.email", "old@example.com"));
            auto new_rows =
                h.users.query(tx, mt::QuerySpec::where_json_eq("$.email", "new@example.com"));

            EXPECT_TRUE(old_rows.empty());
            EXPECT_EQ(new_rows.size(), std::size_t{1});
            EXPECT_EQ(new_rows[0].name, std::string("New"));
        }
    );
}

void test_transactional_json_equals_query_hides_pending_delete()
{
    Harness h;

    h.txs.run(
        [&](mt::Transaction& tx)
        { h.users.put(tx, User{.id = "user:1", .email = "a@example.com", .name = "Alice"}); }
    );

    h.txs.run(
        [&](mt::Transaction& tx)
        {
            h.users.erase(tx, "user:1");

            auto rows = h.users.query(tx, mt::QuerySpec::where_json_eq("$.email", "a@example.com"));
            EXPECT_TRUE(rows.empty());
        }
    );
}

void test_transactional_query_rejects_json_contains()
{
    Harness h;

    mt::QuerySpec query;
    query.predicates.push_back(
        mt::QueryPredicate{
            .op = mt::QueryOp::JsonContains,
            .path = "$",
            .value = mt::Json::object({{"email", "a@example.com"}})
        }
    );

    h.txs.run([&](mt::Transaction& tx)
              { EXPECT_THROW_AS(h.users.query(tx, query), mt::BackendError); });
}

void test_transactional_list_hides_own_pending_delete()
{
    Harness h;

    h.txs.run(
        [&](mt::Transaction& tx)
        { h.users.put(tx, User{.id = "user:1", .email = "a@example.com", .name = "Alice"}); }
    );

    h.txs.run(
        [&](mt::Transaction& tx)
        {
            h.users.erase(tx, "user:1");

            auto listed = h.users.list(tx);
            EXPECT_TRUE(listed.empty());
        }
    );
}

void test_transactional_list_reflects_own_pending_replacement()
{
    Harness h;

    h.txs.run(
        [&](mt::Transaction& tx)
        { h.users.put(tx, User{.id = "user:1", .email = "old@example.com", .name = "Old"}); }
    );

    h.txs.run(
        [&](mt::Transaction& tx)
        {
            h.users.put(tx, User{.id = "user:1", .email = "new@example.com", .name = "New"});

            auto listed = h.users.list(tx);
            EXPECT_EQ(listed.size(), std::size_t{1});
            EXPECT_EQ(listed[0].email, std::string("new@example.com"));
            EXPECT_EQ(listed[0].name, std::string("New"));
        }
    );
}

void test_transactional_list_paginates_after_pending_write_overlay()
{
    Harness h;

    h.txs.run([&](mt::Transaction& tx)
              { h.users.put(tx, User{.id = "user:2", .email = "b@example.com", .name = "Bob"}); });

    h.txs.run(
        [&](mt::Transaction& tx)
        {
            h.users.put(tx, User{.id = "user:1", .email = "a@example.com", .name = "Alice"});
            h.users.put(tx, User{.id = "user:3", .email = "c@example.com", .name = "Carol"});

            auto first_page = h.users.list(tx, mt::ListOptions{.limit = 2});
            EXPECT_EQ(first_page.size(), std::size_t{2});
            EXPECT_EQ(first_page[0].id, std::string("user:1"));
            EXPECT_EQ(first_page[1].id, std::string("user:2"));

            auto second_page = h.users.list(tx, mt::ListOptions{.after_key = "user:1"});
            EXPECT_EQ(second_page.size(), std::size_t{2});
            EXPECT_EQ(second_page[0].id, std::string("user:2"));
            EXPECT_EQ(second_page[1].id, std::string("user:3"));
        }
    );
}

void test_transactional_query_paginates_after_pending_write_overlay()
{
    Harness h;

    h.txs.run([&](mt::Transaction& tx)
              { h.users.put(tx, User{.id = "user:2", .email = "b@example.com", .name = "Bob"}); });

    h.txs.run(
        [&](mt::Transaction& tx)
        {
            auto query = mt::QuerySpec::key_prefix("user:");
            query.limit = 2;

            h.users.put(tx, User{.id = "user:1", .email = "a@example.com", .name = "Alice"});
            h.users.put(tx, User{.id = "user:3", .email = "c@example.com", .name = "Carol"});

            auto rows = h.users.query(tx, query);
            EXPECT_EQ(rows.size(), std::size_t{2});
            EXPECT_EQ(rows[0].id, std::string("user:1"));
            EXPECT_EQ(rows[1].id, std::string("user:2"));

            query.limit = std::nullopt;
            query.after_key = "user:1";
            auto after_rows = h.users.query(tx, query);
            EXPECT_EQ(after_rows.size(), std::size_t{2});
            EXPECT_EQ(after_rows[0].id, std::string("user:2"));
            EXPECT_EQ(after_rows[1].id, std::string("user:3"));
        }
    );
}

void test_non_transactional_query_filters_before_limit()
{
    Harness h;

    h.txs.run(
        [&](mt::Transaction& tx)
        {
            h.users.put(tx, User{.id = "other:1", .email = "o@example.com", .name = "Other"});
            h.users.put(tx, User{.id = "user:1", .email = "a@example.com", .name = "Alice"});
            h.users.put(tx, User{.id = "user:2", .email = "b@example.com", .name = "Bob"});
        }
    );

    auto query = mt::QuerySpec::key_prefix("user:");
    query.limit = 1;

    auto rows = h.users.query(query);
    EXPECT_EQ(rows.size(), std::size_t{1});
    EXPECT_EQ(rows[0].id, std::string("user:1"));
}

void test_non_transactional_query_filters_before_after_key_and_limit()
{
    Harness h;

    h.txs.run(
        [&](mt::Transaction& tx)
        {
            h.users.put(tx, User{.id = "other:2", .email = "o@example.com", .name = "Other"});
            h.users.put(tx, User{.id = "user:1", .email = "a@example.com", .name = "Alice"});
            h.users.put(tx, User{.id = "user:2", .email = "b@example.com", .name = "Bob"});
            h.users.put(tx, User{.id = "user:3", .email = "c@example.com", .name = "Carol"});
        }
    );

    auto query = mt::QuerySpec::key_prefix("user:");
    query.after_key = "user:1";
    query.limit = 1;

    auto rows = h.users.query(query);
    EXPECT_EQ(rows.size(), std::size_t{1});
    EXPECT_EQ(rows[0].id, std::string("user:2"));
}

void test_memory_backend_query_supports_json_equals()
{
    Harness h;

    h.txs.run(
        [&](mt::Transaction& tx)
        {
            h.users.put(tx, User{.id = "user:1", .email = "a@example.com", .name = "Alice"});
            h.users.put(tx, User{.id = "user:2", .email = "b@example.com", .name = "Bob"});
        }
    );

    auto rows = h.users.query(mt::QuerySpec::where_json_eq("$.email", "b@example.com"));
    EXPECT_EQ(rows.size(), std::size_t{1});
    EXPECT_EQ(rows[0].id, std::string("user:2"));
}

void test_memory_backend_rejects_json_contains_query()
{
    Harness h;

    mt::QuerySpec query;
    query.predicates.push_back(
        mt::QueryPredicate{
            .op = mt::QueryOp::JsonContains,
            .path = "$",
            .value = mt::Json::object({{"email", "a@example.com"}})
        }
    );

    EXPECT_THROW_AS(h.users.query(query), mt::BackendError);
}

void test_memory_backend_rejects_non_key_ordering()
{
    Harness h;

    auto query = mt::QuerySpec::key_prefix("user:");
    query.order_by_key = false;

    EXPECT_THROW_AS(h.users.query(query), mt::BackendError);
}

void test_memory_backend_enforces_unique_indexes()
{
    Harness h;

    h.txs.run(
        [&](mt::Transaction& tx)
        { h.users.put(tx, User{.id = "user:1", .email = "same@example.com", .name = "Alice"}); }
    );

    EXPECT_THROW_AS(
        h.txs.run(
            [&](mt::Transaction& tx)
            { h.users.put(tx, User{.id = "user:2", .email = "same@example.com", .name = "Bob"}); }
        ),
        mt::BackendError
    );
}

void test_memory_backend_rejects_migrations()
{
    auto backend = std::make_shared<mt::backends::memory::MemoryBackend>();
    mt::Database db{backend};
    mt::TableProvider tables{db};

    EXPECT_THROW_AS((tables.table<User, MigratingUserMapping>()), mt::BackendError);
}

void test_delete_hides_document()
{
    Harness h;

    h.txs.run(
        [&](mt::Transaction& tx)
        { h.users.put(tx, User{.id = "user:1", .email = "a@example.com", .name = "Alice"}); }
    );

    h.txs.run([&](mt::Transaction& tx) { h.users.erase(tx, "user:1"); });

    auto loaded = h.users.get("user:1");
    EXPECT_FALSE(loaded.has_value());
}

void test_write_write_conflict_aborts_later_committer()
{
    Harness h;

    auto tx1 = h.txs.begin();
    auto tx2 = h.txs.begin();

    h.users.put(tx1, User{.id = "user:1", .email = "a@example.com", .name = "Alice"});
    h.users.put(tx2, User{.id = "user:1", .email = "b@example.com", .name = "Bob"});

    tx1.commit();
    EXPECT_THROW_AS(tx2.commit(), mt::TransactionConflict);
}

void test_read_write_conflict_aborts_reader()
{
    Harness h;

    h.txs.run(
        [&](mt::Transaction& tx)
        { h.users.put(tx, User{.id = "user:1", .email = "a@example.com", .name = "Alice"}); }
    );

    auto tx1 = h.txs.begin();
    auto tx2 = h.txs.begin();

    auto loaded = h.users.get(tx1, "user:1");
    EXPECT_TRUE(loaded.has_value());

    h.users.put(tx2, User{.id = "user:1", .email = "updated@example.com", .name = "Updated"});
    tx2.commit();

    h.users.put(tx1, User{.id = "user:2", .email = "new@example.com", .name = "New"});
    EXPECT_THROW_AS(tx1.commit(), mt::TransactionConflict);
}

void test_absent_read_conflicts_with_later_insert()
{
    Harness h;

    auto tx1 = h.txs.begin();
    auto tx2 = h.txs.begin();

    auto missing = h.users.get(tx1, "user:missing");
    EXPECT_FALSE(missing.has_value());

    h.users.put(tx2, User{.id = "user:missing", .email = "x@example.com", .name = "Inserted"});
    tx2.commit();

    h.users.put(tx1, User{.id = "user:other", .email = "o@example.com", .name = "Other"});
    EXPECT_THROW_AS(tx1.commit(), mt::TransactionConflict);
}

void test_list_predicate_conflict_on_insert()
{
    Harness h;

    h.txs.run(
        [&](mt::Transaction& tx)
        { h.users.put(tx, User{.id = "user:1", .email = "a@example.com", .name = "Alice"}); }
    );

    auto tx1 = h.txs.begin();
    auto tx2 = h.txs.begin();

    auto listed = h.users.list(tx1);
    EXPECT_TRUE(listed.size() >= 1);

    h.users.put(tx2, User{.id = "user:2", .email = "b@example.com", .name = "Bob"});
    tx2.commit();

    h.users.put(tx1, User{.id = "user:3", .email = "c@example.com", .name = "Carol"});
    EXPECT_THROW_AS(tx1.commit(), mt::TransactionConflict);
}

void test_query_predicate_conflict_on_matching_insert()
{
    Harness h;

    auto tx1 = h.txs.begin();
    auto tx2 = h.txs.begin();

    auto query = mt::QuerySpec::key_prefix("user:");
    auto rows = h.users.query(tx1, query);
    EXPECT_TRUE(rows.empty());

    h.users.put(tx2, User{.id = "user:1", .email = "a@example.com", .name = "Alice"});
    tx2.commit();

    h.users.put(tx1, User{.id = "other:1", .email = "o@example.com", .name = "Other"});
    EXPECT_THROW_AS(tx1.commit(), mt::TransactionConflict);
}

void test_retry_retries_on_conflict()
{
    Harness h;

    h.txs.run(
        [&](mt::Transaction& tx)
        { h.users.put(tx, User{.id = "user:1", .email = "a@example.com", .name = "Alice"}); }
    );

    int attempts = 0;

    auto fn = [&](mt::Transaction& tx)
    {
        ++attempts;

        auto user = h.users.require(tx, "user:1");
        user.login_count += 1;

        if (attempts == 1)
        {
            h.txs.run(
                [&](mt::Transaction& other)
                {
                    h.users.put(
                        other, User{
                                   .id = "user:1",
                                   .email = "external@example.com",
                                   .name = "External",
                                   .active = true,
                                   .login_count = 99
                               }
                    );
                }
            );
        }

        h.users.put(tx, user);
    };

    mt::RetryPolicy policy;
    policy.max_attempts = 2;

    h.txs.retry(policy, fn);
    EXPECT_EQ(attempts, 2);

    auto loaded = h.users.require("user:1");
    EXPECT_EQ(loaded.login_count, 100);
    EXPECT_EQ(loaded.email, std::string("external@example.com"));
}

void test_rollback_discards_writes()
{
    Harness h;

    {
        auto tx = h.txs.begin();
        h.users.put(tx, User{.id = "user:1", .email = "a@example.com", .name = "Alice"});
        tx.rollback();
    }

    auto loaded = h.users.get("user:1");
    EXPECT_FALSE(loaded.has_value());
}

void test_destructor_rolls_back_uncommitted_transaction()
{
    Harness h;

    {
        auto tx = h.txs.begin();
        h.users.put(tx, User{.id = "user:1", .email = "a@example.com", .name = "Alice"});
    }

    auto loaded = h.users.get("user:1");
    EXPECT_FALSE(loaded.has_value());
}

int main()
{
    test_table_provider_creates_table();
    test_memory_backend_reports_capabilities();
    test_backend_contract_transaction_ids_are_non_empty_and_unique();
    test_backend_contract_commit_versions_strictly_increase();
    test_backend_contract_clock_increment_requires_lock_owner();
    test_backend_contract_cleanup_tolerates_missing_active_transaction();
    test_backend_contract_snapshot_reads_remain_stable_after_later_commits();
    test_backend_contract_delete_tombstone_metadata_remains_visible();
    test_commit_semantics_history_and_current_share_commit_version();
    test_commit_semantics_delete_history_and_current_share_commit_version();
    test_commit_semantics_conflict_does_not_make_write_visible();
    test_json_values_round_trip_and_hash_stably();
    test_non_transactional_get_missing_returns_nullopt();
    test_transactional_put_then_non_transactional_get();
    test_require_missing_throws();
    test_transactional_read_your_own_point_write();
    test_transactional_list_reads_own_pending_put();
    test_transactional_query_reads_own_pending_matching_put();
    test_transactional_json_equals_query_reads_own_pending_matching_put();
    test_transactional_json_equals_query_excludes_pending_non_match();
    test_transactional_json_equals_query_reflects_pending_replacement();
    test_transactional_json_equals_query_hides_pending_delete();
    test_transactional_query_rejects_json_contains();
    test_transactional_list_hides_own_pending_delete();
    test_transactional_list_reflects_own_pending_replacement();
    test_transactional_list_paginates_after_pending_write_overlay();
    test_transactional_query_paginates_after_pending_write_overlay();
    test_non_transactional_query_filters_before_limit();
    test_non_transactional_query_filters_before_after_key_and_limit();
    test_memory_backend_query_supports_json_equals();
    test_memory_backend_rejects_json_contains_query();
    test_memory_backend_rejects_non_key_ordering();
    test_memory_backend_enforces_unique_indexes();
    test_memory_backend_rejects_migrations();
    test_delete_hides_document();
    test_write_write_conflict_aborts_later_committer();
    test_read_write_conflict_aborts_reader();
    test_absent_read_conflicts_with_later_insert();
    test_list_predicate_conflict_on_insert();
    test_query_predicate_conflict_on_matching_insert();
    test_retry_retries_on_conflict();
    test_rollback_discards_writes();
    test_destructor_rolls_back_uncommitted_transaction();

    std::cout << "All mt_core tests passed.\n";
    return 0;
}
