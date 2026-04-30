#include "mt/backends/sqlite.hpp"

#include "../../../src/backends/sqlite/sqlite_detail.hpp"

#include <cassert>
#include <cstdint>
#include <iostream>
#include <string>

#define EXPECT_FALSE(expr) assert(!(expr))
#define EXPECT_TRUE(expr) assert((expr))
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

void test_sqlite_backend_skeleton_reports_no_capabilities()
{
    mt::backends::sqlite::SqliteBackend backend;
    auto capabilities = backend.capabilities();

    EXPECT_FALSE(capabilities.query.key_prefix);
    EXPECT_FALSE(capabilities.query.json_equals);
    EXPECT_FALSE(capabilities.query.json_contains);
    EXPECT_FALSE(capabilities.query.order_by_key);
    EXPECT_FALSE(capabilities.query.custom_ordering);

    EXPECT_FALSE(capabilities.schema.json_indexes);
    EXPECT_FALSE(capabilities.schema.unique_indexes);
    EXPECT_FALSE(capabilities.schema.migrations);
}

void test_sqlite_backend_skeleton_rejects_operations()
{
    mt::backends::sqlite::SqliteBackend backend;

    EXPECT_THROW_AS(backend.open_session(), mt::BackendError);
    EXPECT_THROW_AS(backend.bootstrap(mt::BootstrapSpec{}), mt::BackendError);
    EXPECT_THROW_AS(
        backend.ensure_collection(mt::CollectionSpec{.logical_name = "users"}), mt::BackendError
    );
    EXPECT_THROW_AS(backend.get_collection("users"), mt::BackendError);
}

void test_sqlite_detail_connection_executes_sql()
{
    auto connection = mt::backends::sqlite::detail::Connection::open_memory();

    connection.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)");
    connection.execute("INSERT INTO items (id, name) VALUES (1, 'one')");

    mt::backends::sqlite::detail::Statement statement{
        connection.get(), "SELECT name FROM items WHERE id = ?"
    };
    statement.bind_int64(1, 1);

    EXPECT_TRUE(statement.step());
    EXPECT_EQ(statement.column_text(0), std::string("one"));
    EXPECT_FALSE(statement.step());
}

void test_sqlite_detail_statement_reuses_bindings_after_reset()
{
    auto connection = mt::backends::sqlite::detail::Connection::open_memory();

    connection.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)");
    connection.execute("INSERT INTO items (id, name) VALUES (1, 'one'), (2, NULL)");

    mt::backends::sqlite::detail::Statement statement{
        connection.get(), "SELECT name FROM items WHERE id = ?"
    };

    statement.bind_int64(1, 1);
    EXPECT_TRUE(statement.step());
    EXPECT_EQ(statement.column_text(0), std::string("one"));
    EXPECT_FALSE(statement.step());

    statement.reset();
    statement.bind_int64(1, 2);
    EXPECT_TRUE(statement.step());
    EXPECT_TRUE(statement.column_is_null(0));
    EXPECT_FALSE(statement.step());
}

void test_sqlite_detail_statement_binds_text_and_null()
{
    auto connection = mt::backends::sqlite::detail::Connection::open_memory();

    connection.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)");

    mt::backends::sqlite::detail::Statement insert{
        connection.get(), "INSERT INTO items (id, name) VALUES (?, ?)"
    };
    insert.bind_int64(1, 1);
    insert.bind_text(2, "one");
    EXPECT_FALSE(insert.step());

    insert.reset();
    insert.bind_int64(1, 2);
    insert.bind_null(2);
    EXPECT_FALSE(insert.step());

    mt::backends::sqlite::detail::Statement count{
        connection.get(), "SELECT COUNT(*) FROM items WHERE name IS NULL"
    };
    EXPECT_TRUE(count.step());
    EXPECT_EQ(count.column_int64(0), std::int64_t{1});
}

int main()
{
    test_sqlite_backend_skeleton_reports_no_capabilities();
    test_sqlite_backend_skeleton_rejects_operations();
    test_sqlite_detail_connection_executes_sql();
    test_sqlite_detail_statement_reuses_bindings_after_reset();
    test_sqlite_detail_statement_binds_text_and_null();

    std::cout << "All sqlite backend tests passed.\n";
    return 0;
}
