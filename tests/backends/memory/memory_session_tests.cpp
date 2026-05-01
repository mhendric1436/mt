#include "memory_test_support.hpp"

using memory_test_support::delete_write;
using memory_test_support::Harness;
using memory_test_support::user_write;

void test_memory_backend_active_transaction_lifecycle_allows_register_commit_and_abort()
{
    Harness h;

    {
        auto session = h.backend->open_session();
        session->begin_backend_transaction();
        auto tx_id = session->create_transaction_id();
        session->register_active_transaction(tx_id, session->read_clock());
        session->unregister_active_transaction("missing-transaction");
        session->unregister_active_transaction(tx_id);
        session->commit_backend_transaction();
    }

    {
        auto session = h.backend->open_session();
        session->begin_backend_transaction();
        auto tx_id = session->create_transaction_id();
        session->register_active_transaction(tx_id, session->read_clock());
        session->abort_backend_transaction();
        session->abort_backend_transaction();
    }
}

void test_memory_backend_snapshot_reads_select_best_visible_version()
{
    Harness h;
    auto collection = h.users.descriptor().id;

    auto first = user_write(collection, "user:1", "old@example.com", true, 0x31);
    auto second = user_write(collection, "user:1", "new@example.com", true, 0x32);
    auto deleted = delete_write(collection, "user:1", 0x33);

    auto session = h.backend->open_session();
    session->begin_backend_transaction();
    session->insert_history(collection, first, 1);
    session->upsert_current(collection, first, 1);
    session->insert_history(collection, second, 2);
    session->upsert_current(collection, second, 2);
    session->insert_history(collection, deleted, 3);
    session->upsert_current(collection, deleted, 3);

    auto missing = session->read_snapshot(collection, "user:1", 0);
    auto old_doc = session->read_snapshot(collection, "user:1", 1);
    auto new_doc = session->read_snapshot(collection, "user:1", 2);
    auto tombstone = session->read_snapshot(collection, "user:1", 3);
    session->commit_backend_transaction();

    EXPECT_FALSE(missing.has_value());
    EXPECT_TRUE(old_doc.has_value());
    EXPECT_EQ(old_doc->value["email"].as_string(), std::string("old@example.com"));
    EXPECT_EQ(old_doc->value_hash, test_hash(0x31));
    EXPECT_TRUE(new_doc.has_value());
    EXPECT_EQ(new_doc->value["email"].as_string(), std::string("new@example.com"));
    EXPECT_EQ(new_doc->value_hash, test_hash(0x32));
    EXPECT_TRUE(tombstone.has_value());
    EXPECT_TRUE(tombstone->deleted);
    EXPECT_EQ(tombstone->value_hash, test_hash(0x33));
}
