#include "sqlite_test_support.hpp"

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
