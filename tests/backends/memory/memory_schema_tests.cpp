#include "memory_test_support.hpp"

using memory_test_support::Harness;
using memory_test_support::MigratingUserMapping;
using memory_test_support::User;
using memory_test_support::user_schema_spec;

void test_memory_backend_reports_capabilities()
{
    Harness h;
    auto capabilities = h.backend->capabilities();

    EXPECT_TRUE(capabilities.query.key_prefix);
    EXPECT_TRUE(capabilities.query.json_equals);
    EXPECT_FALSE(capabilities.query.json_contains);
    EXPECT_TRUE(capabilities.query.order_by_key);
    EXPECT_FALSE(capabilities.query.custom_ordering);

    EXPECT_TRUE(capabilities.schema.json_indexes);
    EXPECT_TRUE(capabilities.schema.unique_indexes);
    EXPECT_FALSE(capabilities.schema.migrations);
}

void test_memory_backend_stores_schema_snapshot_on_create()
{
    mt::backends::memory::MemoryBackend backend;
    mt::CollectionSpec spec{
        .logical_name = "schema_users",
        .indexes = {mt::IndexSpec::json_path_index("email", "$.email").make_unique()},
        .schema_version = 3,
        .key_field = "id",
        .fields = {
            mt::FieldSpec::string("id"), mt::FieldSpec::string("email"),
            mt::FieldSpec::boolean("active").mark_required(false).with_default(mt::Json(true))
        }
    };

    auto descriptor = backend.ensure_collection(spec);
    auto snapshot = backend.schema_snapshot("schema_users");

    EXPECT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot->logical_name, std::string("schema_users"));
    EXPECT_EQ(snapshot->schema_version, 3);
    EXPECT_EQ(snapshot->key_field, std::string("id"));
    EXPECT_EQ(snapshot->fields.size(), std::size_t{3});
    EXPECT_EQ(snapshot->fields[0].name, std::string("id"));
    EXPECT_EQ(snapshot->fields[0].type, mt::FieldType::String);
    EXPECT_EQ(snapshot->fields[2].name, std::string("active"));
    EXPECT_EQ(snapshot->fields[2].type, mt::FieldType::Bool);
    EXPECT_FALSE(snapshot->fields[2].required);
    EXPECT_TRUE(snapshot->fields[2].has_default);
    EXPECT_EQ(snapshot->fields[2].default_value, mt::Json(true));
    EXPECT_EQ(snapshot->indexes.size(), std::size_t{1});
    EXPECT_TRUE(snapshot->indexes[0].unique);
    EXPECT_EQ(descriptor.logical_name, snapshot->logical_name);
}

void test_memory_backend_updates_schema_snapshot_for_compatible_repeat_ensure()
{
    mt::backends::memory::MemoryBackend backend;
    mt::CollectionSpec initial{
        .logical_name = "schema_users",
        .schema_version = 1,
        .key_field = "id",
        .fields = {mt::FieldSpec::string("id"), mt::FieldSpec::string("email")}
    };
    mt::CollectionSpec requested{
        .logical_name = "schema_users",
        .schema_version = 2,
        .key_field = "id",
        .fields = {
            mt::FieldSpec::string("id"), mt::FieldSpec::string("email"),
            mt::FieldSpec::string("nickname").mark_required(false)
        }
    };

    auto first = backend.ensure_collection(initial);
    auto second = backend.ensure_collection(requested);
    auto snapshot = backend.schema_snapshot("schema_users");
    auto descriptor = backend.get_collection("schema_users");

    EXPECT_EQ(second.id, first.id);
    EXPECT_TRUE(snapshot.has_value());
    EXPECT_EQ(second.schema_version, 2);
    EXPECT_EQ(descriptor.id, first.id);
    EXPECT_EQ(descriptor.schema_version, second.schema_version);
    EXPECT_EQ(snapshot->schema_version, 2);
    EXPECT_EQ(snapshot->schema_version, descriptor.schema_version);
    EXPECT_EQ(snapshot->fields.size(), std::size_t{3});
    EXPECT_EQ(snapshot->fields[2].name, std::string("nickname"));
}

void test_memory_backend_rejects_incompatible_schema_change()
{
    mt::backends::memory::MemoryBackend backend;
    mt::CollectionSpec initial{
        .logical_name = "schema_users",
        .schema_version = 1,
        .key_field = "id",
        .fields = {mt::FieldSpec::string("id"), mt::FieldSpec::string("email")}
    };
    mt::CollectionSpec requested{
        .logical_name = "schema_users",
        .schema_version = 2,
        .key_field = "id",
        .fields = {mt::FieldSpec::string("id")}
    };

    auto first = backend.ensure_collection(initial);
    EXPECT_THROW_AS(backend.ensure_collection(requested), mt::BackendError);

    auto snapshot = backend.schema_snapshot("schema_users");
    EXPECT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot->schema_version, 1);
    EXPECT_EQ(snapshot->fields.size(), std::size_t{2});
    EXPECT_EQ(backend.get_collection("schema_users").schema_version, first.schema_version);
}

void test_memory_backend_accepts_optional_field_schema_change()
{
    mt::backends::memory::MemoryBackend backend;
    auto initial = user_schema_spec();
    auto requested = user_schema_spec();
    requested.schema_version = 2;
    requested.fields.push_back(mt::FieldSpec::optional("nickname_extra", mt::FieldType::String));

    backend.ensure_collection(initial);
    auto descriptor = backend.ensure_collection(requested);
    auto snapshot = backend.schema_snapshot("schema_users");

    EXPECT_EQ(descriptor.schema_version, 2);
    EXPECT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot->fields.size(), std::size_t{11});
    EXPECT_EQ(snapshot->fields[10].name, std::string("nickname_extra"));
    EXPECT_EQ(snapshot->fields[10].type, mt::FieldType::Optional);
}

void test_memory_backend_accepts_defaulted_field_schema_change()
{
    mt::backends::memory::MemoryBackend backend;
    auto initial = user_schema_spec();
    auto requested = user_schema_spec();
    requested.schema_version = 2;
    requested.fields.push_back(
        mt::FieldSpec::int64("extra_login_count").with_default(mt::Json(std::int64_t{0}))
    );

    backend.ensure_collection(initial);
    auto descriptor = backend.ensure_collection(requested);
    auto snapshot = backend.schema_snapshot("schema_users");

    EXPECT_EQ(descriptor.schema_version, 2);
    EXPECT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot->fields.size(), std::size_t{11});
    EXPECT_EQ(snapshot->fields[10].name, std::string("extra_login_count"));
    EXPECT_TRUE(snapshot->fields[10].has_default);
    EXPECT_EQ(snapshot->fields[10].default_value, mt::Json(std::int64_t{0}));
}

void test_memory_backend_accepts_nested_object_schema_change()
{
    mt::backends::memory::MemoryBackend backend;
    auto initial = user_schema_spec();
    initial.fields.push_back(mt::FieldSpec::object("profile", {mt::FieldSpec::string("title")}));
    auto requested = initial;
    requested.schema_version = 2;
    requested.fields.back() = mt::FieldSpec::object(
        "profile",
        {mt::FieldSpec::string("title"), mt::FieldSpec::string("unit").mark_required(false)}
    );

    backend.ensure_collection(initial);
    auto descriptor = backend.ensure_collection(requested);
    auto snapshot = backend.schema_snapshot("schema_users");

    EXPECT_EQ(descriptor.schema_version, 2);
    EXPECT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot->fields[10].fields.size(), std::size_t{2});
    EXPECT_EQ(snapshot->fields[10].fields[1].name, std::string("unit"));
}

void test_memory_backend_rejects_key_field_schema_change()
{
    mt::backends::memory::MemoryBackend backend;
    auto initial = user_schema_spec();
    auto requested = user_schema_spec();
    requested.schema_version = 2;
    requested.key_field = "email";

    backend.ensure_collection(initial);
    EXPECT_THROW_AS(backend.ensure_collection(requested), mt::BackendError);

    auto snapshot = backend.schema_snapshot("schema_users");
    EXPECT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot->key_field, std::string("id"));
    EXPECT_EQ(snapshot->schema_version, 1);
}

void test_memory_backend_rejects_field_type_schema_change()
{
    mt::backends::memory::MemoryBackend backend;
    auto initial = user_schema_spec();
    auto requested = user_schema_spec();
    requested.schema_version = 2;
    requested.fields[1] = mt::FieldSpec::int64("email");

    backend.ensure_collection(initial);
    EXPECT_THROW_AS(backend.ensure_collection(requested), mt::BackendError);

    auto snapshot = backend.schema_snapshot("schema_users");
    EXPECT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot->fields[1].type, mt::FieldType::String);
    EXPECT_EQ(snapshot->schema_version, 1);
}

void test_memory_backend_rejects_required_added_field_schema_change()
{
    mt::backends::memory::MemoryBackend backend;
    auto initial = user_schema_spec();
    auto requested = user_schema_spec();
    requested.schema_version = 2;
    requested.fields.push_back(mt::FieldSpec::string("display_name"));

    backend.ensure_collection(initial);
    EXPECT_THROW_AS(backend.ensure_collection(requested), mt::BackendError);

    auto snapshot = backend.schema_snapshot("schema_users");
    EXPECT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot->fields.size(), std::size_t{10});
    EXPECT_EQ(snapshot->schema_version, 1);
}

void test_memory_backend_rejects_migrations()
{
    auto backend = std::make_shared<mt::backends::memory::MemoryBackend>();
    mt::Database db{backend};
    mt::TableProvider tables{db};

    EXPECT_THROW_AS((tables.table<User, MigratingUserMapping>()), mt::BackendError);
}
