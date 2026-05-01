#include "sqlite_test_support.hpp"

void test_sqlite_backend_reports_capabilities()
{
    mt::backends::sqlite::SqliteBackend backend;
    auto capabilities = backend.capabilities();

    EXPECT_TRUE(capabilities.query.key_prefix);
    EXPECT_TRUE(capabilities.query.json_equals);
    EXPECT_FALSE(capabilities.query.json_contains);
    EXPECT_TRUE(capabilities.query.order_by_key);
    EXPECT_FALSE(capabilities.query.custom_ordering);

    EXPECT_TRUE(capabilities.schema.json_indexes);
    EXPECT_TRUE(capabilities.schema.unique_indexes);
    EXPECT_FALSE(capabilities.schema.migrations);
}

void test_sqlite_backend_rejects_unimplemented_operations()
{
    mt::backends::sqlite::SqliteBackend backend;

    EXPECT_THROW_AS(backend.get_collection("users"), mt::BackendError);
}

void test_sqlite_backend_bootstrap_creates_private_metadata()
{
    auto path = sqlite_test_path("mt_sqlite_bootstrap_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};

    backend.bootstrap(mt::BootstrapSpec{.metadata_schema_version = 7});
    backend.bootstrap(mt::BootstrapSpec{.metadata_schema_version = 7});

    auto connection = mt::backends::sqlite::detail::Connection::open(path.string());
    mt::backends::sqlite::detail::Statement tables{
        connection.get(), mt::backends::sqlite::detail::PrivateSchemaSql::count_private_tables()
    };
    EXPECT_TRUE(tables.step());
    EXPECT_EQ(tables.column_int64(0), std::int64_t{6});

    mt::backends::sqlite::detail::Statement metadata{
        connection.get(),
        mt::backends::sqlite::detail::PrivateSchemaSql::select_metadata_schema_version()
    };
    EXPECT_TRUE(metadata.step());
    EXPECT_EQ(metadata.column_int64(0), std::int64_t{7});

    mt::backends::sqlite::detail::Statement clock{
        connection.get(), mt::backends::sqlite::detail::PrivateSchemaSql::select_clock_row()
    };
    EXPECT_TRUE(clock.step());
    EXPECT_EQ(clock.column_int64(0), std::int64_t{0});
    EXPECT_EQ(clock.column_int64(1), std::int64_t{1});

    std::filesystem::remove(path);
}

void test_sqlite_backend_bootstrap_runs_once_per_backend_instance()
{
    auto path = sqlite_test_path("mt_sqlite_bootstrap_once_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};

    backend.bootstrap(mt::BootstrapSpec{.metadata_schema_version = 7});
    backend.bootstrap(mt::BootstrapSpec{.metadata_schema_version = 9});

    auto connection = mt::backends::sqlite::detail::Connection::open(path.string());
    mt::backends::sqlite::detail::Statement metadata{
        connection.get(),
        mt::backends::sqlite::detail::PrivateSchemaSql::select_metadata_schema_version()
    };
    EXPECT_TRUE(metadata.step());
    EXPECT_EQ(metadata.column_int64(0), std::int64_t{7});

    std::filesystem::remove(path);
}
