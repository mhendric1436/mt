#include "../../../src/backends/postgres/postgres_connection.hpp"

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
    }
    catch (const mt::Error& error)
    {
        std::cerr << error.what() << "\n";
        return 1;
    }

    std::cout << "Postgres backend connection tests passed.\n";
    return 0;
}
