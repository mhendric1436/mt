#include "memory_test_support.hpp"

#include <iostream>

using memory_test_support::delete_write;
using memory_test_support::Harness;
using memory_test_support::MigratingUserMapping;
using memory_test_support::test_hash;
using memory_test_support::User;
using memory_test_support::user_schema_spec;
using memory_test_support::user_write;
using memory_test_support::UserMapping;

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

    EXPECT_EQ(second.id, first.id);
    EXPECT_TRUE(snapshot.has_value());
    EXPECT_EQ(second.schema_version, 2);
    EXPECT_EQ(snapshot->schema_version, 2);
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
    requested.fields.push_back(mt::FieldSpec::optional("nickname", mt::FieldType::String));

    backend.ensure_collection(initial);
    auto descriptor = backend.ensure_collection(requested);
    auto snapshot = backend.schema_snapshot("schema_users");

    EXPECT_EQ(descriptor.schema_version, 2);
    EXPECT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot->fields.size(), std::size_t{4});
    EXPECT_EQ(snapshot->fields[3].name, std::string("nickname"));
    EXPECT_EQ(snapshot->fields[3].type, mt::FieldType::Optional);
}

void test_memory_backend_accepts_defaulted_field_schema_change()
{
    mt::backends::memory::MemoryBackend backend;
    auto initial = user_schema_spec();
    auto requested = user_schema_spec();
    requested.schema_version = 2;
    requested.fields.push_back(
        mt::FieldSpec::int64("login_count").with_default(mt::Json(std::int64_t{0}))
    );

    backend.ensure_collection(initial);
    auto descriptor = backend.ensure_collection(requested);
    auto snapshot = backend.schema_snapshot("schema_users");

    EXPECT_EQ(descriptor.schema_version, 2);
    EXPECT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot->fields.size(), std::size_t{4});
    EXPECT_EQ(snapshot->fields[3].name, std::string("login_count"));
    EXPECT_TRUE(snapshot->fields[3].has_default);
    EXPECT_EQ(snapshot->fields[3].default_value, mt::Json(std::int64_t{0}));
}

void test_memory_backend_accepts_nested_object_schema_change()
{
    mt::backends::memory::MemoryBackend backend;
    auto initial = user_schema_spec();
    initial.fields.push_back(mt::FieldSpec::object("address", {mt::FieldSpec::string("city")}));
    auto requested = initial;
    requested.schema_version = 2;
    requested.fields.back() = mt::FieldSpec::object(
        "address",
        {mt::FieldSpec::string("city"), mt::FieldSpec::string("unit").mark_required(false)}
    );

    backend.ensure_collection(initial);
    auto descriptor = backend.ensure_collection(requested);
    auto snapshot = backend.schema_snapshot("schema_users");

    EXPECT_EQ(descriptor.schema_version, 2);
    EXPECT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot->fields[3].fields.size(), std::size_t{2});
    EXPECT_EQ(snapshot->fields[3].fields[1].name, std::string("unit"));
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
    requested.fields.push_back(mt::FieldSpec::string("name"));

    backend.ensure_collection(initial);
    EXPECT_THROW_AS(backend.ensure_collection(requested), mt::BackendError);

    auto snapshot = backend.schema_snapshot("schema_users");
    EXPECT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot->fields.size(), std::size_t{3});
    EXPECT_EQ(snapshot->schema_version, 1);
}

void test_memory_backend_active_transaction_lifecycle_allows_register_commit_and_abort()
{
    Harness h;

    {
        auto session = h.backend->open_session();
        session->begin_backend_transaction();
        auto tx_id = session->create_transaction_id();
        session->register_active_transaction(tx_id, session->read_clock());
        session->unregister_active_transaction("missing-transaction");
        session->unregister_active_transaction(tx_id);
        session->commit_backend_transaction();
    }

    {
        auto session = h.backend->open_session();
        session->begin_backend_transaction();
        auto tx_id = session->create_transaction_id();
        session->register_active_transaction(tx_id, session->read_clock());
        session->abort_backend_transaction();
        session->abort_backend_transaction();
    }
}

void test_memory_backend_snapshot_reads_select_best_visible_version()
{
    Harness h;
    auto collection = h.users.descriptor().id;

    auto first = user_write(collection, "user:1", "old@example.com", true, 0x31);
    auto second = user_write(collection, "user:1", "new@example.com", true, 0x32);
    auto deleted = delete_write(collection, "user:1", 0x33);

    auto session = h.backend->open_session();
    session->begin_backend_transaction();
    session->insert_history(collection, first, 1);
    session->upsert_current(collection, first, 1);
    session->insert_history(collection, second, 2);
    session->upsert_current(collection, second, 2);
    session->insert_history(collection, deleted, 3);
    session->upsert_current(collection, deleted, 3);

    auto missing = session->read_snapshot(collection, "user:1", 0);
    auto old_doc = session->read_snapshot(collection, "user:1", 1);
    auto new_doc = session->read_snapshot(collection, "user:1", 2);
    auto tombstone = session->read_snapshot(collection, "user:1", 3);
    session->commit_backend_transaction();

    EXPECT_FALSE(missing.has_value());
    EXPECT_TRUE(old_doc.has_value());
    EXPECT_EQ(old_doc->value["email"].as_string(), std::string("old@example.com"));
    EXPECT_EQ(old_doc->value_hash, test_hash(0x31));
    EXPECT_TRUE(new_doc.has_value());
    EXPECT_EQ(new_doc->value["email"].as_string(), std::string("new@example.com"));
    EXPECT_EQ(new_doc->value_hash, test_hash(0x32));
    EXPECT_TRUE(tombstone.has_value());
    EXPECT_TRUE(tombstone->deleted);
    EXPECT_EQ(tombstone->value_hash, test_hash(0x33));
}

void test_memory_backend_query_current_metadata_filters_and_limits()
{
    Harness h;
    auto collection = h.users.descriptor().id;

    auto user_1 = user_write(collection, "user:1", "one@example.com", true, 0x41);
    auto user_2 = user_write(collection, "user:2", "two@example.com", true, 0x42);
    auto skipped = user_write(collection, "user:3", "three@example.com", false, 0x43);
    auto deleted = delete_write(collection, "user:4", 0x44);

    auto session = h.backend->open_session();
    session->begin_backend_transaction();
    session->insert_history(collection, user_1, 1);
    session->upsert_current(collection, user_1, 1);
    session->insert_history(collection, user_2, 2);
    session->upsert_current(collection, user_2, 2);
    session->insert_history(collection, skipped, 3);
    session->upsert_current(collection, skipped, 3);
    session->insert_history(collection, deleted, 4);
    session->upsert_current(collection, deleted, 4);

    auto query = mt::QuerySpec::where_json_eq("$.active", true);
    query.after_key = "user:1";
    query.limit = 1;

    auto rows = session->query_current_metadata(collection, query).rows;
    session->commit_backend_transaction();

    EXPECT_EQ(rows.size(), std::size_t{1});
    EXPECT_EQ(rows[0].key, std::string("user:2"));
    EXPECT_FALSE(rows[0].deleted);
    EXPECT_EQ(rows[0].value_hash, test_hash(0x42));
}

void test_memory_backend_query_supports_json_equals()
{
    Harness h;

    h.txs.run(
        [&](mt::Transaction& tx)
        {
            h.users.put(tx, User{.id = "user:1", .email = "a@example.com", .name = "Alice"});
            h.users.put(tx, User{.id = "user:2", .email = "b@example.com", .name = "Bob"});
        }
    );

    auto rows = h.users.query(mt::QuerySpec::where_json_eq("$.email", "b@example.com"));
    EXPECT_EQ(rows.size(), std::size_t{1});
    EXPECT_EQ(rows[0].id, std::string("user:2"));
}

void test_memory_backend_rejects_json_contains_query()
{
    Harness h;

    mt::QuerySpec query;
    query.predicates.push_back(
        mt::QueryPredicate{
            .op = mt::QueryOp::JsonContains,
            .path = "$",
            .value = mt::Json::object({{"email", "a@example.com"}})
        }
    );

    EXPECT_THROW_AS(h.users.query(query), mt::BackendError);
}

void test_memory_backend_rejects_non_key_ordering()
{
    Harness h;

    auto query = mt::QuerySpec::key_prefix("user:");
    query.order_by_key = false;

    EXPECT_THROW_AS(h.users.query(query), mt::BackendError);
}

void test_memory_backend_enforces_unique_indexes()
{
    Harness h;

    h.txs.run(
        [&](mt::Transaction& tx)
        { h.users.put(tx, User{.id = "user:1", .email = "same@example.com", .name = "Alice"}); }
    );

    EXPECT_THROW_AS(
        h.txs.run(
            [&](mt::Transaction& tx)
            { h.users.put(tx, User{.id = "user:2", .email = "same@example.com", .name = "Bob"}); }
        ),
        mt::BackendError
    );
}

void test_memory_backend_unique_indexes_allow_same_key_missing_path_and_delete()
{
    Harness h;
    auto collection = h.users.descriptor().id;

    auto first = user_write(collection, "user:1", "same@example.com", true, 0x51);
    auto same_key = user_write(collection, "user:1", "same@example.com", false, 0x52);
    mt::WriteEnvelope missing_path{
        .collection = collection,
        .key = "user:2",
        .kind = mt::WriteKind::Put,
        .value = mt::Json::object({{"active", true}}),
        .value_hash = test_hash(0x53)
    };
    auto delete_missing = delete_write(collection, "user:3", 0x54);
    auto conflict = user_write(collection, "user:4", "same@example.com", true, 0x55);

    auto session = h.backend->open_session();
    session->begin_backend_transaction();
    session->upsert_current(collection, first, 1);
    session->upsert_current(collection, same_key, 2);
    session->upsert_current(collection, missing_path, 3);
    session->upsert_current(collection, delete_missing, 4);
    EXPECT_THROW_AS(session->upsert_current(collection, conflict, 5), mt::BackendError);
    session->abort_backend_transaction();
}

void test_memory_backend_rejects_migrations()
{
    auto backend = std::make_shared<mt::backends::memory::MemoryBackend>();
    mt::Database db{backend};
    mt::TableProvider tables{db};

    EXPECT_THROW_AS((tables.table<User, MigratingUserMapping>()), mt::BackendError);
}

int main()
{
    test_memory_backend_reports_capabilities();
    test_memory_backend_stores_schema_snapshot_on_create();
    test_memory_backend_updates_schema_snapshot_for_compatible_repeat_ensure();
    test_memory_backend_rejects_incompatible_schema_change();
    test_memory_backend_accepts_optional_field_schema_change();
    test_memory_backend_accepts_defaulted_field_schema_change();
    test_memory_backend_accepts_nested_object_schema_change();
    test_memory_backend_rejects_key_field_schema_change();
    test_memory_backend_rejects_field_type_schema_change();
    test_memory_backend_rejects_required_added_field_schema_change();
    test_memory_backend_active_transaction_lifecycle_allows_register_commit_and_abort();
    test_memory_backend_snapshot_reads_select_best_visible_version();
    test_memory_backend_query_current_metadata_filters_and_limits();
    test_memory_backend_query_supports_json_equals();
    test_memory_backend_rejects_json_contains_query();
    test_memory_backend_rejects_non_key_ordering();
    test_memory_backend_enforces_unique_indexes();
    test_memory_backend_unique_indexes_allow_same_key_missing_path_and_delete();
    test_memory_backend_rejects_migrations();

    std::cout << "All memory backend tests passed.\n";
    return 0;
}
