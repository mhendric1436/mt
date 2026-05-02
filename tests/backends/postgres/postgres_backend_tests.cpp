#include "../../../src/backends/postgres/postgres_connection.hpp"
#include "../../../src/backends/postgres/postgres_schema.hpp"

#include "mt/backends/postgres.hpp"

#include <cstdlib>
#include <iostream>
#include <string_view>

namespace
{
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

    try
    {
        session->begin_backend_transaction();
    }
    catch (const mt::BackendError&)
    {
        return;
    }

    throw mt::BackendError("postgres session lifecycle unexpectedly succeeded");
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
        test_postgres_backend_entry_points(dsn);
        test_postgres_bootstrap_creates_private_tables(dsn);
    }
    catch (const mt::Error& error)
    {
        std::cerr << error.what() << "\n";
        return 1;
    }

    std::cout << "Postgres backend connection tests passed.\n";
    return 0;
}
