#include <libpq-fe.h>

#include <cstdlib>
#include <iostream>

int main()
{
    const auto* dsn = std::getenv("MT_POSTGRES_TEST_DSN");
    if (!dsn || *dsn == '\0')
    {
        std::cout << "Skipping postgres backend tests: MT_POSTGRES_TEST_DSN is not set.\n";
        return 0;
    }

    auto* connection = PQconnectdb(dsn);
    if (PQstatus(connection) != CONNECTION_OK)
    {
        std::cerr << "PostgreSQL connection failed: " << PQerrorMessage(connection);
        PQfinish(connection);
        return 1;
    }

    PQfinish(connection);
    std::cout << "Postgres backend test harness connected; backend tests pending.\n";
    return 0;
}
