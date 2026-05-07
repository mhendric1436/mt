#include "sqlite_test_support.hpp"

namespace
{
std::int64_t count_collection_unique_index_entries(
    const std::filesystem::path& path,
    mt::CollectionId collection
)
{
    auto connection = mt::backends::sqlite::detail::Connection::open(path.string());
    mt::backends::sqlite::detail::Statement count{
        connection.get(), "SELECT COUNT(*) FROM mt_unique_index_values WHERE collection_id = ?"
    };
    count.bind_int64(1, collection);
    EXPECT_TRUE(count.step());
    return count.column_int64(0);
}

mt::CollectionSpec sqlite_user_schema_without_indexes(int schema_version = 1)
{
    auto spec = sqlite_user_schema(schema_version);
    spec.indexes = {};
    return spec;
}
} // namespace

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

void test_sqlite_backend_rebuilds_added_unique_index()
{
    auto path = sqlite_test_path("mt_sqlite_collection_unique_rebuild_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};

    auto initial = sqlite_user_schema_without_indexes(1);
    auto descriptor = backend.ensure_collection(initial);

    auto first = mt::WriteEnvelope{
        .collection = descriptor.id,
        .key = "user:1",
        .kind = mt::WriteKind::Put,
        .value = sqlite_user_json("user:1", "one@example.com"),
        .value_hash = test_hash(0xE1)
    };
    auto second = mt::WriteEnvelope{
        .collection = descriptor.id,
        .key = "user:2",
        .kind = mt::WriteKind::Put,
        .value = sqlite_user_json("user:2", "two@example.com"),
        .value_hash = test_hash(0xE2)
    };
    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        session->upsert_current(descriptor.id, first, 1);
        session->upsert_current(descriptor.id, second, 2);
        session->commit_backend_transaction();
    }
    EXPECT_EQ(count_collection_unique_index_entries(path, descriptor.id), std::int64_t{0});

    auto updated = sqlite_user_schema(2);
    auto rebuilt = backend.ensure_collection(updated);

    EXPECT_EQ(rebuilt.id, descriptor.id);
    EXPECT_EQ(count_collection_unique_index_entries(path, descriptor.id), std::int64_t{2});

    std::filesystem::remove(path);
}

void test_sqlite_backend_rejects_added_unique_index_with_existing_duplicates()
{
    auto path = sqlite_test_path("mt_sqlite_collection_unique_rebuild_duplicate_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};

    auto initial = sqlite_user_schema_without_indexes(1);
    auto descriptor = backend.ensure_collection(initial);

    auto first = mt::WriteEnvelope{
        .collection = descriptor.id,
        .key = "user:1",
        .kind = mt::WriteKind::Put,
        .value = sqlite_user_json("user:1", "same@example.com"),
        .value_hash = test_hash(0xE3)
    };
    auto second = mt::WriteEnvelope{
        .collection = descriptor.id,
        .key = "user:2",
        .kind = mt::WriteKind::Put,
        .value = sqlite_user_json("user:2", "same@example.com"),
        .value_hash = test_hash(0xE4)
    };
    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        session->upsert_current(descriptor.id, first, 1);
        session->upsert_current(descriptor.id, second, 2);
        session->commit_backend_transaction();
    }

    auto updated = sqlite_user_schema(2);

    EXPECT_THROW_AS(backend.ensure_collection(updated), mt::BackendError);
    EXPECT_EQ(count_collection_unique_index_entries(path, descriptor.id), std::int64_t{0});

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

void test_sqlite_backend_rejects_nullable_unique_index_schema()
{
    auto path = sqlite_test_path("mt_sqlite_collection_nullable_unique_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};
    auto spec = sqlite_user_schema(1);
    spec.fields.push_back(mt::FieldSpec::optional("nickname", mt::FieldType::String));
    spec.indexes.push_back(mt::IndexSpec::json_path_index("nickname", "$.nickname").make_unique());

    EXPECT_THROW_AS(backend.ensure_collection(spec), mt::BackendError);

    std::filesystem::remove(path);
}

void test_sqlite_backend_rejects_nested_index_schema()
{
    auto path = sqlite_test_path("mt_sqlite_collection_nested_index_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};
    auto spec = sqlite_user_schema(1);
    spec.fields.push_back(mt::FieldSpec::object("profile", {mt::FieldSpec::string("handle")}));
    spec.indexes.push_back(mt::IndexSpec::json_path_index("handle", "$.profile.handle"));

    EXPECT_THROW_AS(backend.ensure_collection(spec), mt::BackendError);

    std::filesystem::remove(path);
}
