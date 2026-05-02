#include "../../../src/backends/postgres/postgres_connection.hpp"
#include "../../../src/backends/postgres/postgres_schema.hpp"

#include "mt/backends/postgres.hpp"

#include <cstdlib>
#include <iostream>
#include <string>
#include <string_view>
#include <utility>

namespace
{
mt::CollectionSpec postgres_user_schema(std::string logical_name)
{
    return mt::CollectionSpec{
        .logical_name = std::move(logical_name),
        .indexes = {mt::IndexSpec::json_path_index("email", "$.email").make_unique()},
        .schema_version = 1,
        .key_field = "id",
        .fields = {mt::FieldSpec::string("id"), mt::FieldSpec::string("email")}
    };
}

void reset_postgres_private_tables(std::string_view dsn)
{
    auto connection = mt::backends::postgres::detail::Connection::open(dsn);
    connection.exec_command(
        "DROP TABLE IF EXISTS "
        "mt_current, mt_history, mt_active_transactions, mt_schema_snapshots, "
        "mt_collections, mt_clock, mt_metadata "
        "CASCADE"
    );
}

void test_postgres_connection_executes_query(std::string_view dsn)
{
    auto connection = mt::backends::postgres::detail::Connection::open(dsn);
    auto result = connection.exec_params("SELECT $1::text", {"mt"}, {PGRES_TUPLES_OK});

    if (result.rows() != 1 || result.columns() != 1 || result.value(0, 0) != "mt")
    {
        throw mt::BackendError("postgres connection wrapper returned unexpected query result");
    }
}

void require_postgres_backend_capabilities_are_pending(const mt::BackendCapabilities& capabilities)
{
    if (capabilities.query.key_prefix || capabilities.query.json_equals ||
        capabilities.query.json_contains || capabilities.query.order_by_key ||
        capabilities.query.custom_ordering || capabilities.schema.json_indexes ||
        capabilities.schema.unique_indexes || capabilities.schema.migrations)
    {
        throw mt::BackendError("postgres backend reports capabilities before implementation");
    }
}

void test_postgres_backend_entry_points(std::string_view dsn)
{
    auto backend = mt::backends::postgres::PostgresBackend(std::string(dsn));
    require_postgres_backend_capabilities_are_pending(backend.capabilities());
    backend.bootstrap(mt::BootstrapSpec{});

    auto session = backend.open_session();
    if (!session)
    {
        throw mt::BackendError("postgres backend returned null session");
    }

    session->begin_backend_transaction();
    session->abort_backend_transaction();
}

void test_postgres_backend_session_rejects_invalid_lifecycle(std::string_view dsn)
{
    auto backend = mt::backends::postgres::PostgresBackend(std::string(dsn));
    backend.bootstrap(mt::BootstrapSpec{});

    {
        auto session = backend.open_session();
        auto threw = false;
        try
        {
            session->commit_backend_transaction();
        }
        catch (const mt::BackendError&)
        {
            threw = true;
        }

        if (!threw)
        {
            throw mt::BackendError("postgres backend accepted commit without open transaction");
        }
    }

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        auto threw = false;
        try
        {
            session->begin_backend_transaction();
        }
        catch (const mt::BackendError&)
        {
            threw = true;
        }
        session->abort_backend_transaction();

        if (!threw)
        {
            throw mt::BackendError("postgres backend accepted nested session lifecycle");
        }
    }
}

void test_postgres_backend_clock_reads_and_increments(std::string_view dsn)
{
    auto backend = mt::backends::postgres::PostgresBackend(std::string(dsn));
    backend.bootstrap(mt::BootstrapSpec{});

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        if (session->read_clock() != mt::Version{0} ||
            session->lock_clock_and_read() != mt::Version{0})
        {
            throw mt::BackendError("postgres clock did not start at zero");
        }

        auto threw = false;
        try
        {
            session->lock_clock_and_read();
        }
        catch (const mt::BackendError&)
        {
            threw = true;
        }

        if (!threw)
        {
            throw mt::BackendError("postgres clock accepted second lock in one session");
        }

        if (session->increment_clock_and_return() != mt::Version{1})
        {
            throw mt::BackendError("postgres clock increment returned unexpected version");
        }
        session->commit_backend_transaction();
    }

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        if (session->read_clock() != mt::Version{1})
        {
            throw mt::BackendError("postgres clock increment was not persisted");
        }
        session->abort_backend_transaction();
    }
}

void test_postgres_backend_clock_increment_requires_lock(std::string_view dsn)
{
    auto backend = mt::backends::postgres::PostgresBackend(std::string(dsn));
    backend.bootstrap(mt::BootstrapSpec{});

    auto session = backend.open_session();
    session->begin_backend_transaction();
    try
    {
        session->increment_clock_and_return();
    }
    catch (const mt::BackendError&)
    {
        session->abort_backend_transaction();
        return;
    }

    session->abort_backend_transaction();
    throw mt::BackendError("postgres clock increment succeeded without clock lock");
}

void test_postgres_backend_transaction_ids_are_unique_and_persisted(std::string_view dsn)
{
    auto backend = mt::backends::postgres::PostgresBackend(std::string(dsn));
    backend.bootstrap(mt::BootstrapSpec{});

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        if (session->create_transaction_id() != "postgres:1" ||
            session->create_transaction_id() != "postgres:2")
        {
            throw mt::BackendError("postgres transaction ids did not start at expected values");
        }
        session->commit_backend_transaction();
    }

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        if (session->create_transaction_id() != "postgres:3")
        {
            throw mt::BackendError("postgres transaction ids were not persisted");
        }
        session->abort_backend_transaction();
    }
}

void test_postgres_backend_active_transaction_metadata_registers_and_cleans_up(std::string_view dsn)
{
    auto backend = mt::backends::postgres::PostgresBackend(std::string(dsn));
    backend.bootstrap(mt::BootstrapSpec{});

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        session->register_active_transaction("postgres:test", 42);
        session->unregister_active_transaction("missing");
        session->commit_backend_transaction();
    }

    {
        auto connection = mt::backends::postgres::detail::Connection::open(dsn);
        auto count = connection.exec_query(
            mt::backends::postgres::detail::PrivateSchemaSql::count_active_transactions()
        );
        if (count.rows() != 1 || count.value(0, 0) != "1")
        {
            throw mt::BackendError("postgres active transaction was not registered");
        }
    }

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        session->unregister_active_transaction("postgres:test");
        session->commit_backend_transaction();
    }

    {
        auto connection = mt::backends::postgres::detail::Connection::open(dsn);
        auto count = connection.exec_query(
            mt::backends::postgres::detail::PrivateSchemaSql::count_active_transactions()
        );
        if (count.rows() != 1 || count.value(0, 0) != "0")
        {
            throw mt::BackendError("postgres active transaction was not removed");
        }
    }
}

void test_postgres_bootstrap_creates_private_tables(std::string_view dsn)
{
    auto backend = mt::backends::postgres::PostgresBackend(std::string(dsn));
    backend.bootstrap(mt::BootstrapSpec{.metadata_schema_version = 3});

    auto connection = mt::backends::postgres::detail::Connection::open(dsn);
    auto tables = connection.exec_query(
        mt::backends::postgres::detail::PrivateSchemaSql::count_private_tables()
    );
    if (tables.rows() != 1 || tables.value(0, 0) != "7")
    {
        throw mt::BackendError("postgres bootstrap did not create all private tables");
    }

    auto metadata = connection.exec_query(
        mt::backends::postgres::detail::PrivateSchemaSql::select_metadata_schema_version()
    );
    if (metadata.rows() != 1 || metadata.value(0, 0) != "3")
    {
        throw mt::BackendError("postgres bootstrap did not seed metadata schema version");
    }

    auto clock =
        connection.exec_query(mt::backends::postgres::detail::PrivateSchemaSql::select_clock_row());
    if (clock.rows() != 1 || clock.value(0, 0) != "0" || clock.value(0, 1) != "1")
    {
        throw mt::BackendError("postgres bootstrap did not seed clock row");
    }
}

void test_postgres_collection_metadata_round_trips(std::string_view dsn)
{
    auto backend = mt::backends::postgres::PostgresBackend(std::string(dsn));
    auto initial = postgres_user_schema("postgres_schema_users");
    auto requested = postgres_user_schema("postgres_schema_users");
    requested.schema_version = 2;
    requested.fields.push_back(mt::FieldSpec::string("nickname").mark_required(false));

    auto first = backend.ensure_collection(initial);
    auto second = backend.ensure_collection(requested);
    auto descriptor = backend.get_collection("postgres_schema_users");

    if (first.id != second.id || descriptor.id != first.id || descriptor.schema_version != 2)
    {
        throw mt::BackendError("postgres collection descriptors are inconsistent");
    }

    auto connection = mt::backends::postgres::detail::Connection::open(dsn);
    auto snapshot =
        mt::backends::postgres::detail::load_collection_spec(connection, "postgres_schema_users");
    if (!snapshot || snapshot->schema_version != 2 || snapshot->fields.size() != 3 ||
        snapshot->indexes.size() != 1 || !snapshot->indexes[0].unique)
    {
        throw mt::BackendError("postgres collection schema snapshot did not round-trip");
    }
}

void test_postgres_rejects_incompatible_schema_change(std::string_view dsn)
{
    auto backend = mt::backends::postgres::PostgresBackend(std::string(dsn));
    auto initial = postgres_user_schema("postgres_incompatible_schema_users");
    auto requested = postgres_user_schema("postgres_incompatible_schema_users");
    requested.schema_version = 2;
    requested.fields = {mt::FieldSpec::string("id")};

    auto first = backend.ensure_collection(initial);
    try
    {
        backend.ensure_collection(requested);
    }
    catch (const mt::BackendError&)
    {
        auto descriptor = backend.get_collection("postgres_incompatible_schema_users");
        if (descriptor.schema_version != first.schema_version)
        {
            throw mt::BackendError("postgres incompatible schema update changed descriptor");
        }
        return;
    }

    throw mt::BackendError("postgres accepted incompatible schema change");
}

} // namespace

int main()
{
    const auto* dsn = std::getenv("MT_POSTGRES_TEST_DSN");
    if (!dsn || *dsn == '\0')
    {
        std::cout << "Skipping postgres backend tests: MT_POSTGRES_TEST_DSN is not set.\n";
        return 0;
    }

    try
    {
        test_postgres_connection_executes_query(dsn);
        reset_postgres_private_tables(dsn);
        test_postgres_backend_entry_points(dsn);
        test_postgres_bootstrap_creates_private_tables(dsn);
        test_postgres_backend_session_rejects_invalid_lifecycle(dsn);
        test_postgres_backend_clock_reads_and_increments(dsn);
        test_postgres_backend_clock_increment_requires_lock(dsn);
        test_postgres_backend_transaction_ids_are_unique_and_persisted(dsn);
        test_postgres_backend_active_transaction_metadata_registers_and_cleans_up(dsn);
        test_postgres_collection_metadata_round_trips(dsn);
        test_postgres_rejects_incompatible_schema_change(dsn);
    }
    catch (const mt::Error& error)
    {
        std::cerr << error.what() << "\n";
        return 1;
    }

    std::cout << "Postgres backend connection tests passed.\n";
    return 0;
}
