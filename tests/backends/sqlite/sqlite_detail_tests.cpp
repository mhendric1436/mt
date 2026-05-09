#include "sqlite_test_support.hpp"

void test_sqlite_detail_physical_names_are_derived_from_logical_table()
{
    using mt::backends::sqlite::detail::physical_current_key_index_name;
    using mt::backends::sqlite::detail::physical_json_index_name;
    using mt::backends::sqlite::detail::physical_user_table_name;

    auto unique = mt::IndexSpec::json_path_index("email", "$.email").make_unique();
    auto non_unique = mt::IndexSpec::json_path_index("active", "$.active");

    EXPECT_EQ(physical_user_table_name("users"), std::string("mt_user_users"));
    EXPECT_EQ(
        physical_current_key_index_name("users"), std::string("mt_user_users_current_key_idx")
    );
    EXPECT_EQ(physical_json_index_name("users", unique), std::string("mt_user_users_email_uidx"));
    EXPECT_EQ(
        physical_json_index_name("users", non_unique), std::string("mt_user_users_active_idx")
    );
}

void test_sqlite_detail_quote_identifier_escapes_embedded_quotes()
{
    EXPECT_EQ(
        mt::backends::sqlite::detail::quote_identifier("mt_user_users"),
        std::string("\"mt_user_users\"")
    );
    EXPECT_EQ(mt::backends::sqlite::detail::quote_identifier("a\"b"), std::string("\"a\"\"b\""));
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
