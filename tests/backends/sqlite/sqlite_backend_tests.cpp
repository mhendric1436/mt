#include "mt/backends/sqlite.hpp"

#include "../../../src/backends/sqlite/sqlite_detail.hpp"

#include <cassert>
#include <cstdint>
#include <filesystem>
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
    EXPECT_THROW_AS(backend.get_collection("users"), mt::BackendError);
}

std::filesystem::path sqlite_test_path(std::string_view name)
{
    auto path = std::filesystem::temp_directory_path() / std::string(name);
    std::filesystem::remove(path);
    return path;
}

void test_sqlite_backend_bootstrap_creates_private_metadata()
{
    auto path = sqlite_test_path("mt_sqlite_bootstrap_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};

    backend.bootstrap(mt::BootstrapSpec{.metadata_schema_version = 7});
    backend.bootstrap(mt::BootstrapSpec{.metadata_schema_version = 7});

    auto connection = mt::backends::sqlite::detail::Connection::open(path.string());
    mt::backends::sqlite::detail::Statement tables{
        connection.get(), "SELECT COUNT(*) FROM sqlite_master "
                          "WHERE type = 'table' "
                          "AND name IN ("
                          "'mt_meta', 'mt_clock', 'mt_collections', "
                          "'mt_active_transactions', 'mt_history', 'mt_current')"
    };
    EXPECT_TRUE(tables.step());
    EXPECT_EQ(tables.column_int64(0), std::int64_t{6});

    mt::backends::sqlite::detail::Statement metadata{
        connection.get(), "SELECT value FROM mt_meta WHERE key = 'metadata_schema_version'"
    };
    EXPECT_TRUE(metadata.step());
    EXPECT_EQ(metadata.column_int64(0), std::int64_t{7});

    mt::backends::sqlite::detail::Statement clock{
        connection.get(), "SELECT version, next_tx_id FROM mt_clock WHERE id = 1"
    };
    EXPECT_TRUE(clock.step());
    EXPECT_EQ(clock.column_int64(0), std::int64_t{0});
    EXPECT_EQ(clock.column_int64(1), std::int64_t{1});

    std::filesystem::remove(path);
}

mt::CollectionSpec sqlite_user_schema(int schema_version = 1)
{
    return mt::CollectionSpec{
        .logical_name = "users",
        .indexes = {mt::IndexSpec::json_path_index("email", "$.email").make_unique()},
        .schema_version = schema_version,
        .key_field = "id",
        .fields = {
            mt::FieldSpec::string("id"), mt::FieldSpec::string("email"),
            mt::FieldSpec::boolean("active").mark_required(false).with_default(mt::Json(true))
        }
    };
}

void test_sqlite_backend_ensure_collection_creates_and_gets_descriptor()
{
    auto path = sqlite_test_path("mt_sqlite_collection_create_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};

    auto descriptor = backend.ensure_collection(sqlite_user_schema(3));
    auto loaded = backend.get_collection("users");

    EXPECT_TRUE(descriptor.id != 0);
    EXPECT_EQ(descriptor.id, loaded.id);
    EXPECT_EQ(loaded.logical_name, std::string("users"));
    EXPECT_EQ(loaded.schema_version, 3);

    std::filesystem::remove(path);
}

void test_sqlite_backend_ensure_collection_persists_across_instances()
{
    auto path = sqlite_test_path("mt_sqlite_collection_persistence_test.sqlite");
    {
        mt::backends::sqlite::SqliteBackend backend{path.string()};
        backend.ensure_collection(sqlite_user_schema(4));
    }
    {
        mt::backends::sqlite::SqliteBackend backend{path.string()};
        auto loaded = backend.get_collection("users");

        EXPECT_EQ(loaded.logical_name, std::string("users"));
        EXPECT_EQ(loaded.schema_version, 4);
    }

    std::filesystem::remove(path);
}

void test_sqlite_backend_accepts_compatible_schema_change()
{
    auto path = sqlite_test_path("mt_sqlite_collection_compatible_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};
    auto updated = sqlite_user_schema(2);
    updated.fields.push_back(mt::FieldSpec::optional("nickname", mt::FieldType::String));

    auto first = backend.ensure_collection(sqlite_user_schema(1));
    auto second = backend.ensure_collection(updated);
    auto loaded = backend.get_collection("users");

    EXPECT_EQ(second.id, first.id);
    EXPECT_EQ(second.schema_version, 2);
    EXPECT_EQ(loaded.schema_version, 2);

    std::filesystem::remove(path);
}

void test_sqlite_backend_rejects_incompatible_schema_change()
{
    auto path = sqlite_test_path("mt_sqlite_collection_incompatible_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};
    auto incompatible = sqlite_user_schema(2);
    incompatible.fields.erase(incompatible.fields.begin() + 1);

    backend.ensure_collection(sqlite_user_schema(1));
    EXPECT_THROW_AS(backend.ensure_collection(incompatible), mt::BackendError);

    auto loaded = backend.get_collection("users");
    EXPECT_EQ(loaded.schema_version, 1);

    std::filesystem::remove(path);
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
    test_sqlite_backend_bootstrap_creates_private_metadata();
    test_sqlite_backend_ensure_collection_creates_and_gets_descriptor();
    test_sqlite_backend_ensure_collection_persists_across_instances();
    test_sqlite_backend_accepts_compatible_schema_change();
    test_sqlite_backend_rejects_incompatible_schema_change();
    test_sqlite_detail_connection_executes_sql();
    test_sqlite_detail_statement_reuses_bindings_after_reset();
    test_sqlite_detail_statement_binds_text_and_null();

    std::cout << "All sqlite backend tests passed.\n";
    return 0;
}
