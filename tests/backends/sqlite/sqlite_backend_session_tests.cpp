#include "sqlite_test_support.hpp"

void test_sqlite_backend_session_commits_and_aborts_transactions()
{
    auto path = sqlite_test_path("mt_sqlite_session_lifecycle_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        session->commit_backend_transaction();
    }

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        session->abort_backend_transaction();
        session->abort_backend_transaction();
    }

    std::filesystem::remove(path);
}

void test_sqlite_backend_session_rejects_invalid_lifecycle()
{
    auto path = sqlite_test_path("mt_sqlite_session_invalid_lifecycle_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};

    {
        auto session = backend.open_session();
        EXPECT_THROW_AS(session->commit_backend_transaction(), mt::BackendError);
    }

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        EXPECT_THROW_AS(session->begin_backend_transaction(), mt::BackendError);
        session->abort_backend_transaction();
    }

    std::filesystem::remove(path);
}

void test_sqlite_backend_session_rejects_unsupported_queries()
{
    auto path = sqlite_test_path("mt_sqlite_session_unimplemented_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};
    auto session = backend.open_session();
    mt::QuerySpec contains;
    contains.predicates.push_back(
        mt::QueryPredicate{
            .op = mt::QueryOp::JsonContains,
            .path = "$",
            .value = sqlite_user_json("user:1", "a@example.com")
        }
    );

    session->begin_backend_transaction();
    EXPECT_THROW_AS(session->query_snapshot(1, contains, 0), mt::BackendError);

    auto unordered = mt::QuerySpec::key_prefix("user:");
    unordered.order_by_key = false;
    EXPECT_THROW_AS(session->query_current_metadata(1, unordered), mt::BackendError);
    session->abort_backend_transaction();

    std::filesystem::remove(path);
}

void test_sqlite_backend_clock_reads_and_increments()
{
    auto path = sqlite_test_path("mt_sqlite_clock_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        EXPECT_EQ(session->read_clock(), mt::Version{0});
        EXPECT_EQ(session->lock_clock_and_read(), mt::Version{0});
        EXPECT_THROW_AS(session->lock_clock_and_read(), mt::BackendError);
        EXPECT_EQ(session->increment_clock_and_return(), mt::Version{1});
        session->commit_backend_transaction();
    }

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        EXPECT_EQ(session->read_clock(), mt::Version{1});
        session->abort_backend_transaction();
    }

    std::filesystem::remove(path);
}

void test_sqlite_backend_clock_increment_requires_lock()
{
    auto path = sqlite_test_path("mt_sqlite_clock_requires_lock_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};

    auto session = backend.open_session();
    session->begin_backend_transaction();
    EXPECT_THROW_AS(session->increment_clock_and_return(), mt::BackendError);
    session->abort_backend_transaction();

    std::filesystem::remove(path);
}

void test_sqlite_backend_transaction_ids_are_unique_and_persisted()
{
    auto path = sqlite_test_path("mt_sqlite_tx_ids_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        EXPECT_EQ(session->create_transaction_id(), std::string("sqlite:1"));
        EXPECT_EQ(session->create_transaction_id(), std::string("sqlite:2"));
        session->commit_backend_transaction();
    }

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        EXPECT_EQ(session->create_transaction_id(), std::string("sqlite:3"));
        session->abort_backend_transaction();
    }

    std::filesystem::remove(path);
}

void test_sqlite_backend_active_transaction_metadata_registers_and_cleans_up()
{
    auto path = sqlite_test_path("mt_sqlite_active_tx_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        session->register_active_transaction("sqlite:test", 42);
        session->unregister_active_transaction("missing");
        session->commit_backend_transaction();
    }

    {
        auto connection = mt::backends::sqlite::detail::Connection::open(path.string());
        mt::backends::sqlite::detail::Statement count{
            connection.get(),
            mt::backends::sqlite::detail::PrivateSchemaSql::count_active_transactions()
        };
        EXPECT_TRUE(count.step());
        EXPECT_EQ(count.column_int64(0), std::int64_t{1});
    }

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        session->unregister_active_transaction("sqlite:test");
        session->commit_backend_transaction();
    }

    {
        auto connection = mt::backends::sqlite::detail::Connection::open(path.string());
        mt::backends::sqlite::detail::Statement count{
            connection.get(),
            mt::backends::sqlite::detail::PrivateSchemaSql::count_active_transactions()
        };
        EXPECT_TRUE(count.step());
        EXPECT_EQ(count.column_int64(0), std::int64_t{0});
    }

    std::filesystem::remove(path);
}
