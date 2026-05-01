#include "memory_test_support.hpp"

using memory_test_support::delete_write;
using memory_test_support::Harness;
using memory_test_support::User;
using memory_test_support::user_write;

void test_memory_backend_query_current_metadata_filters_and_limits()
{
    Harness h;
    auto collection = h.users.descriptor().id;

    auto user_1 = user_write(collection, "user:1", "one@example.com", true, 0x41);
    auto user_2 = user_write(collection, "user:2", "two@example.com", true, 0x42);
    auto skipped = user_write(collection, "user:3", "three@example.com", false, 0x43);
    auto deleted = delete_write(collection, "user:4", 0x44);

    auto session = h.backend->open_session();
    session->begin_backend_transaction();
    session->insert_history(collection, user_1, 1);
    session->upsert_current(collection, user_1, 1);
    session->insert_history(collection, user_2, 2);
    session->upsert_current(collection, user_2, 2);
    session->insert_history(collection, skipped, 3);
    session->upsert_current(collection, skipped, 3);
    session->insert_history(collection, deleted, 4);
    session->upsert_current(collection, deleted, 4);
    session->commit_backend_transaction();

    auto query = mt::QuerySpec::where_json_eq("$.active", true);
    query.after_key = "user:1";
    query.limit = 1;

    auto rows = session->query_current_metadata(collection, query).rows;

    EXPECT_EQ(rows.size(), std::size_t{1});
    EXPECT_EQ(rows[0].key, std::string("user:2"));
    EXPECT_FALSE(rows[0].deleted);
    EXPECT_EQ(rows[0].value_hash, test_hash(0x42));
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

void test_memory_backend_unique_indexes_allow_same_key_missing_path_and_delete()
{
    Harness h;
    auto collection = h.users.descriptor().id;

    auto first = user_write(collection, "user:1", "same@example.com", true, 0x51);
    auto same_key = user_write(collection, "user:1", "same@example.com", false, 0x52);
    mt::WriteEnvelope missing_path{
        .collection = collection,
        .key = "user:2",
        .kind = mt::WriteKind::Put,
        .value = mt::Json::object({{"active", true}}),
        .value_hash = test_hash(0x53)
    };
    auto delete_missing = delete_write(collection, "user:3", 0x54);
    auto conflict = user_write(collection, "user:4", "same@example.com", true, 0x55);

    auto session = h.backend->open_session();
    session->begin_backend_transaction();
    session->upsert_current(collection, first, 1);
    session->upsert_current(collection, same_key, 2);
    session->upsert_current(collection, missing_path, 3);
    session->upsert_current(collection, delete_missing, 4);
    EXPECT_THROW_AS(session->upsert_current(collection, conflict, 5), mt::BackendError);
    session->abort_backend_transaction();
}
