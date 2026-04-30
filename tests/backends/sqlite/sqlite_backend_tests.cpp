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

void test_sqlite_backend_session_commits_and_aborts_transactions()
{
    auto path = sqlite_test_path("mt_sqlite_session_lifecycle_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        session->commit_backend_transaction();
    }

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        session->abort_backend_transaction();
        session->abort_backend_transaction();
    }

    std::filesystem::remove(path);
}

void test_sqlite_backend_session_rejects_invalid_lifecycle()
{
    auto path = sqlite_test_path("mt_sqlite_session_invalid_lifecycle_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};

    {
        auto session = backend.open_session();
        EXPECT_THROW_AS(session->commit_backend_transaction(), mt::BackendError);
    }

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        EXPECT_THROW_AS(session->begin_backend_transaction(), mt::BackendError);
        session->abort_backend_transaction();
    }

    std::filesystem::remove(path);
}

void test_sqlite_backend_session_rejects_unsupported_queries()
{
    auto path = sqlite_test_path("mt_sqlite_session_unimplemented_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};
    auto session = backend.open_session();
    mt::QuerySpec contains;
    contains.predicates.push_back(
        mt::QueryPredicate{
            .op = mt::QueryOp::JsonContains,
            .path = "$",
            .value = mt::Json::object({{"email", "a@example.com"}})
        }
    );

    session->begin_backend_transaction();
    EXPECT_THROW_AS(session->query_snapshot(1, contains, 0), mt::BackendError);

    auto unordered = mt::QuerySpec::key_prefix("user:");
    unordered.order_by_key = false;
    EXPECT_THROW_AS(session->query_current_metadata(1, unordered), mt::BackendError);
    session->abort_backend_transaction();

    std::filesystem::remove(path);
}

void test_sqlite_backend_clock_reads_and_increments()
{
    auto path = sqlite_test_path("mt_sqlite_clock_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        EXPECT_EQ(session->read_clock(), mt::Version{0});
        EXPECT_EQ(session->lock_clock_and_read(), mt::Version{0});
        EXPECT_THROW_AS(session->lock_clock_and_read(), mt::BackendError);
        EXPECT_EQ(session->increment_clock_and_return(), mt::Version{1});
        session->commit_backend_transaction();
    }

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        EXPECT_EQ(session->read_clock(), mt::Version{1});
        session->abort_backend_transaction();
    }

    std::filesystem::remove(path);
}

void test_sqlite_backend_clock_increment_requires_lock()
{
    auto path = sqlite_test_path("mt_sqlite_clock_requires_lock_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};

    auto session = backend.open_session();
    session->begin_backend_transaction();
    EXPECT_THROW_AS(session->increment_clock_and_return(), mt::BackendError);
    session->abort_backend_transaction();

    std::filesystem::remove(path);
}

void test_sqlite_backend_transaction_ids_are_unique_and_persisted()
{
    auto path = sqlite_test_path("mt_sqlite_tx_ids_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        EXPECT_EQ(session->create_transaction_id(), std::string("sqlite:1"));
        EXPECT_EQ(session->create_transaction_id(), std::string("sqlite:2"));
        session->commit_backend_transaction();
    }

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        EXPECT_EQ(session->create_transaction_id(), std::string("sqlite:3"));
        session->abort_backend_transaction();
    }

    std::filesystem::remove(path);
}

void test_sqlite_backend_active_transaction_metadata_registers_and_cleans_up()
{
    auto path = sqlite_test_path("mt_sqlite_active_tx_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        session->register_active_transaction("sqlite:test", 42);
        session->unregister_active_transaction("missing");
        session->commit_backend_transaction();
    }

    {
        auto connection = mt::backends::sqlite::detail::Connection::open(path.string());
        mt::backends::sqlite::detail::Statement count{
            connection.get(), "SELECT COUNT(*) FROM mt_active_transactions"
        };
        EXPECT_TRUE(count.step());
        EXPECT_EQ(count.column_int64(0), std::int64_t{1});
    }

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        session->unregister_active_transaction("sqlite:test");
        session->commit_backend_transaction();
    }

    {
        auto connection = mt::backends::sqlite::detail::Connection::open(path.string());
        mt::backends::sqlite::detail::Statement count{
            connection.get(), "SELECT COUNT(*) FROM mt_active_transactions"
        };
        EXPECT_TRUE(count.step());
        EXPECT_EQ(count.column_int64(0), std::int64_t{0});
    }

    std::filesystem::remove(path);
}

mt::Hash test_hash(std::uint8_t value)
{
    return mt::Hash{.bytes = {value, static_cast<std::uint8_t>(value + 1)}};
}

void test_sqlite_backend_document_writes_insert_history_and_current()
{
    auto path = sqlite_test_path("mt_sqlite_document_write_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};
    auto descriptor = backend.ensure_collection(sqlite_user_schema(1));

    mt::WriteEnvelope write{
        .collection = descriptor.id,
        .key = "user:1",
        .kind = mt::WriteKind::Put,
        .value = mt::Json::object({{"email", "a@example.com"}}),
        .value_hash = test_hash(0x12)
    };

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        session->insert_history(descriptor.id, write, 7);
        session->upsert_current(descriptor.id, write, 7);
        session->commit_backend_transaction();
    }

    auto connection = mt::backends::sqlite::detail::Connection::open(path.string());
    mt::backends::sqlite::detail::Statement history{
        connection.get(), "SELECT version, deleted, value_hash, value_json FROM mt_history "
                          "WHERE collection_id = ? AND document_key = ?"
    };
    history.bind_int64(1, descriptor.id);
    history.bind_text(2, "user:1");
    EXPECT_TRUE(history.step());
    EXPECT_EQ(history.column_int64(0), std::int64_t{7});
    EXPECT_EQ(history.column_int64(1), std::int64_t{0});
    EXPECT_EQ(history.column_text(2), std::string("1213"));
    EXPECT_EQ(history.column_text(3), std::string("{\"email\":\"a@example.com\"}"));

    mt::backends::sqlite::detail::Statement current{
        connection.get(), "SELECT version, deleted, value_hash, value_json FROM mt_current "
                          "WHERE collection_id = ? AND document_key = ?"
    };
    current.bind_int64(1, descriptor.id);
    current.bind_text(2, "user:1");
    EXPECT_TRUE(current.step());
    EXPECT_EQ(current.column_int64(0), std::int64_t{7});
    EXPECT_EQ(current.column_int64(1), std::int64_t{0});
    EXPECT_EQ(current.column_text(2), std::string("1213"));
    EXPECT_EQ(current.column_text(3), std::string("{\"email\":\"a@example.com\"}"));

    std::filesystem::remove(path);
}

void test_sqlite_backend_document_writes_store_delete_tombstone()
{
    auto path = sqlite_test_path("mt_sqlite_document_delete_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};
    auto descriptor = backend.ensure_collection(sqlite_user_schema(1));

    mt::WriteEnvelope write{
        .collection = descriptor.id,
        .key = "user:1",
        .kind = mt::WriteKind::Delete,
        .value_hash = test_hash(0x21)
    };

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        session->insert_history(descriptor.id, write, 8);
        session->upsert_current(descriptor.id, write, 8);
        session->commit_backend_transaction();
    }

    auto connection = mt::backends::sqlite::detail::Connection::open(path.string());
    mt::backends::sqlite::detail::Statement current{
        connection.get(), "SELECT version, deleted, value_hash, value_json FROM mt_current "
                          "WHERE collection_id = ? AND document_key = ?"
    };
    current.bind_int64(1, descriptor.id);
    current.bind_text(2, "user:1");
    EXPECT_TRUE(current.step());
    EXPECT_EQ(current.column_int64(0), std::int64_t{8});
    EXPECT_EQ(current.column_int64(1), std::int64_t{1});
    EXPECT_EQ(current.column_text(2), std::string("2122"));
    EXPECT_TRUE(current.column_is_null(3));

    std::filesystem::remove(path);
}

void test_sqlite_backend_document_writes_rollback_on_abort()
{
    auto path = sqlite_test_path("mt_sqlite_document_abort_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};
    auto descriptor = backend.ensure_collection(sqlite_user_schema(1));

    mt::WriteEnvelope write{
        .collection = descriptor.id,
        .key = "user:1",
        .kind = mt::WriteKind::Put,
        .value = mt::Json::object({{"email", "a@example.com"}}),
        .value_hash = test_hash(0x31)
    };

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        session->insert_history(descriptor.id, write, 9);
        session->upsert_current(descriptor.id, write, 9);
        session->abort_backend_transaction();
    }

    auto connection = mt::backends::sqlite::detail::Connection::open(path.string());
    mt::backends::sqlite::detail::Statement count{
        connection.get(), "SELECT COUNT(*) FROM mt_history"
    };
    EXPECT_TRUE(count.step());
    EXPECT_EQ(count.column_int64(0), std::int64_t{0});

    std::filesystem::remove(path);
}

void test_sqlite_backend_point_reads_return_snapshot_versions()
{
    auto path = sqlite_test_path("mt_sqlite_point_read_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};
    auto descriptor = backend.ensure_collection(sqlite_user_schema(1));

    mt::WriteEnvelope first{
        .collection = descriptor.id,
        .key = "user:1",
        .kind = mt::WriteKind::Put,
        .value = mt::Json::object({{"email", "old@example.com"}}),
        .value_hash = test_hash(0x41)
    };
    mt::WriteEnvelope second{
        .collection = descriptor.id,
        .key = "user:1",
        .kind = mt::WriteKind::Put,
        .value = mt::Json::object({{"email", "new@example.com"}}),
        .value_hash = test_hash(0x51)
    };

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        session->insert_history(descriptor.id, first, 1);
        session->upsert_current(descriptor.id, first, 1);
        session->insert_history(descriptor.id, second, 2);
        session->upsert_current(descriptor.id, second, 2);
        session->commit_backend_transaction();
    }

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        auto missing = session->read_snapshot(descriptor.id, "missing", 2);
        auto old_doc = session->read_snapshot(descriptor.id, "user:1", 1);
        auto new_doc = session->read_snapshot(descriptor.id, "user:1", 2);
        auto current = session->read_current_metadata(descriptor.id, "user:1");
        session->commit_backend_transaction();

        EXPECT_FALSE(missing.has_value());
        EXPECT_TRUE(old_doc.has_value());
        EXPECT_EQ(old_doc->version, mt::Version{1});
        EXPECT_EQ(old_doc->value["email"].as_string(), std::string("old@example.com"));
        EXPECT_EQ(old_doc->value_hash, test_hash(0x41));

        EXPECT_TRUE(new_doc.has_value());
        EXPECT_EQ(new_doc->version, mt::Version{2});
        EXPECT_EQ(new_doc->value["email"].as_string(), std::string("new@example.com"));
        EXPECT_EQ(new_doc->value_hash, test_hash(0x51));

        EXPECT_TRUE(current.has_value());
        EXPECT_EQ(current->version, mt::Version{2});
        EXPECT_FALSE(current->deleted);
        EXPECT_EQ(current->value_hash, test_hash(0x51));
    }

    std::filesystem::remove(path);
}

void test_sqlite_backend_point_reads_return_tombstones()
{
    auto path = sqlite_test_path("mt_sqlite_point_read_tombstone_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};
    auto descriptor = backend.ensure_collection(sqlite_user_schema(1));

    mt::WriteEnvelope write{
        .collection = descriptor.id,
        .key = "user:1",
        .kind = mt::WriteKind::Delete,
        .value_hash = test_hash(0x61)
    };

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        session->insert_history(descriptor.id, write, 3);
        session->upsert_current(descriptor.id, write, 3);
        session->commit_backend_transaction();
    }

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        auto doc = session->read_snapshot(descriptor.id, "user:1", 3);
        auto current = session->read_current_metadata(descriptor.id, "user:1");
        session->commit_backend_transaction();

        EXPECT_TRUE(doc.has_value());
        EXPECT_TRUE(doc->deleted);
        EXPECT_TRUE(doc->value.is_null());
        EXPECT_EQ(doc->value_hash, test_hash(0x61));

        EXPECT_TRUE(current.has_value());
        EXPECT_TRUE(current->deleted);
        EXPECT_EQ(current->value_hash, test_hash(0x61));
    }

    std::filesystem::remove(path);
}

void test_sqlite_backend_list_snapshot_orders_and_paginates()
{
    auto path = sqlite_test_path("mt_sqlite_list_snapshot_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};
    auto descriptor = backend.ensure_collection(sqlite_user_schema(1));

    mt::WriteEnvelope user_1{
        .collection = descriptor.id,
        .key = "user:1",
        .kind = mt::WriteKind::Put,
        .value = mt::Json::object({{"email", "one@example.com"}}),
        .value_hash = test_hash(0x71)
    };
    mt::WriteEnvelope user_2_old{
        .collection = descriptor.id,
        .key = "user:2",
        .kind = mt::WriteKind::Put,
        .value = mt::Json::object({{"email", "old@example.com"}}),
        .value_hash = test_hash(0x72)
    };
    mt::WriteEnvelope user_2_new{
        .collection = descriptor.id,
        .key = "user:2",
        .kind = mt::WriteKind::Put,
        .value = mt::Json::object({{"email", "new@example.com"}}),
        .value_hash = test_hash(0x73)
    };
    mt::WriteEnvelope user_3_delete{
        .collection = descriptor.id,
        .key = "user:3",
        .kind = mt::WriteKind::Delete,
        .value_hash = test_hash(0x74)
    };

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        session->insert_history(descriptor.id, user_2_old, 1);
        session->upsert_current(descriptor.id, user_2_old, 1);
        session->insert_history(descriptor.id, user_1, 2);
        session->upsert_current(descriptor.id, user_1, 2);
        session->insert_history(descriptor.id, user_2_new, 3);
        session->upsert_current(descriptor.id, user_2_new, 3);
        session->insert_history(descriptor.id, user_3_delete, 4);
        session->upsert_current(descriptor.id, user_3_delete, 4);
        session->commit_backend_transaction();
    }

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        auto rows = session->list_snapshot(descriptor.id, mt::ListOptions{.limit = 2}, 4).rows;
        auto after_rows =
            session
                ->list_snapshot(
                    descriptor.id, mt::ListOptions{.limit = 2, .after_key = "user:1"}, 4
                )
                .rows;
        auto early_rows = session->list_snapshot(descriptor.id, mt::ListOptions{}, 2).rows;
        session->commit_backend_transaction();

        EXPECT_EQ(rows.size(), std::size_t{2});
        EXPECT_EQ(rows[0].key, std::string("user:1"));
        EXPECT_EQ(rows[1].key, std::string("user:2"));
        EXPECT_EQ(rows[1].value["email"].as_string(), std::string("new@example.com"));

        EXPECT_EQ(after_rows.size(), std::size_t{2});
        EXPECT_EQ(after_rows[0].key, std::string("user:2"));
        EXPECT_EQ(after_rows[1].key, std::string("user:3"));
        EXPECT_TRUE(after_rows[1].deleted);

        EXPECT_EQ(early_rows.size(), std::size_t{2});
        EXPECT_EQ(early_rows[0].key, std::string("user:1"));
        EXPECT_EQ(early_rows[1].key, std::string("user:2"));
        EXPECT_EQ(early_rows[1].value["email"].as_string(), std::string("old@example.com"));
    }

    std::filesystem::remove(path);
}

void test_sqlite_backend_list_current_metadata_orders_and_paginates()
{
    auto path = sqlite_test_path("mt_sqlite_list_current_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};
    auto descriptor = backend.ensure_collection(sqlite_user_schema(1));

    mt::WriteEnvelope user_1{
        .collection = descriptor.id,
        .key = "user:1",
        .kind = mt::WriteKind::Put,
        .value = mt::Json::object({{"email", "one@example.com"}}),
        .value_hash = test_hash(0x81)
    };
    mt::WriteEnvelope user_2{
        .collection = descriptor.id,
        .key = "user:2",
        .kind = mt::WriteKind::Delete,
        .value_hash = test_hash(0x82)
    };

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        session->insert_history(descriptor.id, user_2, 1);
        session->upsert_current(descriptor.id, user_2, 1);
        session->insert_history(descriptor.id, user_1, 2);
        session->upsert_current(descriptor.id, user_1, 2);
        session->commit_backend_transaction();
    }

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        auto rows = session
                        ->list_current_metadata(
                            descriptor.id, mt::ListOptions{.limit = 1, .after_key = "user:1"}
                        )
                        .rows;
        session->commit_backend_transaction();

        EXPECT_EQ(rows.size(), std::size_t{1});
        EXPECT_EQ(rows[0].key, std::string("user:2"));
        EXPECT_EQ(rows[0].version, mt::Version{1});
        EXPECT_TRUE(rows[0].deleted);
        EXPECT_EQ(rows[0].value_hash, test_hash(0x82));
    }

    std::filesystem::remove(path);
}

void test_sqlite_backend_query_snapshot_supports_key_prefix_and_json_equals()
{
    auto path = sqlite_test_path("mt_sqlite_query_snapshot_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};
    auto descriptor = backend.ensure_collection(sqlite_user_schema(1));

    mt::WriteEnvelope user_1{
        .collection = descriptor.id,
        .key = "user:1",
        .kind = mt::WriteKind::Put,
        .value = mt::Json::object({{"email", "match@example.com"}, {"active", true}}),
        .value_hash = test_hash(0x91)
    };
    mt::WriteEnvelope user_2{
        .collection = descriptor.id,
        .key = "user:2",
        .kind = mt::WriteKind::Put,
        .value = mt::Json::object({{"email", "skip@example.com"}, {"active", false}}),
        .value_hash = test_hash(0x92)
    };
    mt::WriteEnvelope other{
        .collection = descriptor.id,
        .key = "other:1",
        .kind = mt::WriteKind::Put,
        .value = mt::Json::object({{"email", "other@example.com"}, {"active", true}}),
        .value_hash = test_hash(0x93)
    };

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        session->insert_history(descriptor.id, user_1, 1);
        session->upsert_current(descriptor.id, user_1, 1);
        session->insert_history(descriptor.id, user_2, 2);
        session->upsert_current(descriptor.id, user_2, 2);
        session->insert_history(descriptor.id, other, 3);
        session->upsert_current(descriptor.id, other, 3);
        session->commit_backend_transaction();
    }

    auto query = mt::QuerySpec::key_prefix("user:");
    query.predicates.push_back(
        mt::QueryPredicate{
            .op = mt::QueryOp::JsonEquals, .path = "$.active", .value = mt::Json(true)
        }
    );

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        auto rows = session->query_snapshot(descriptor.id, query, 3).rows;
        session->commit_backend_transaction();

        EXPECT_EQ(rows.size(), std::size_t{1});
        EXPECT_EQ(rows[0].key, std::string("user:1"));
        EXPECT_TRUE(rows[0].value["active"].as_bool());
    }

    std::filesystem::remove(path);
}

void test_sqlite_backend_query_current_metadata_filters_and_limits()
{
    auto path = sqlite_test_path("mt_sqlite_query_current_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};
    auto descriptor = backend.ensure_collection(sqlite_user_schema(1));

    mt::WriteEnvelope user_1{
        .collection = descriptor.id,
        .key = "user:1",
        .kind = mt::WriteKind::Put,
        .value = mt::Json::object({{"email", "one@example.com"}, {"active", true}}),
        .value_hash = test_hash(0xA1)
    };
    mt::WriteEnvelope user_2{
        .collection = descriptor.id,
        .key = "user:2",
        .kind = mt::WriteKind::Put,
        .value = mt::Json::object({{"email", "two@example.com"}, {"active", true}}),
        .value_hash = test_hash(0xA2)
    };
    mt::WriteEnvelope deleted{
        .collection = descriptor.id,
        .key = "user:3",
        .kind = mt::WriteKind::Delete,
        .value_hash = test_hash(0xA3)
    };

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        session->insert_history(descriptor.id, user_1, 1);
        session->upsert_current(descriptor.id, user_1, 1);
        session->insert_history(descriptor.id, user_2, 2);
        session->upsert_current(descriptor.id, user_2, 2);
        session->insert_history(descriptor.id, deleted, 3);
        session->upsert_current(descriptor.id, deleted, 3);
        session->commit_backend_transaction();
    }

    auto query = mt::QuerySpec::where_json_eq("$.active", true);
    query.limit = 1;
    query.after_key = "user:1";

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        auto rows = session->query_current_metadata(descriptor.id, query).rows;
        session->commit_backend_transaction();

        EXPECT_EQ(rows.size(), std::size_t{1});
        EXPECT_EQ(rows[0].key, std::string("user:2"));
        EXPECT_FALSE(rows[0].deleted);
        EXPECT_EQ(rows[0].value_hash, test_hash(0xA2));
    }

    std::filesystem::remove(path);
}

void test_sqlite_backend_enforces_unique_indexes()
{
    auto path = sqlite_test_path("mt_sqlite_unique_index_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};
    auto descriptor = backend.ensure_collection(sqlite_user_schema(1));

    mt::WriteEnvelope first{
        .collection = descriptor.id,
        .key = "user:1",
        .kind = mt::WriteKind::Put,
        .value = mt::Json::object({{"email", "same@example.com"}}),
        .value_hash = test_hash(0xB1)
    };
    mt::WriteEnvelope conflict{
        .collection = descriptor.id,
        .key = "user:2",
        .kind = mt::WriteKind::Put,
        .value = mt::Json::object({{"email", "same@example.com"}}),
        .value_hash = test_hash(0xB2)
    };

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        session->insert_history(descriptor.id, first, 1);
        session->upsert_current(descriptor.id, first, 1);
        EXPECT_THROW_AS(session->upsert_current(descriptor.id, conflict, 2), mt::BackendError);
        session->abort_backend_transaction();
    }

    std::filesystem::remove(path);
}

void test_sqlite_backend_unique_indexes_allow_same_key_update_missing_path_and_delete()
{
    auto path = sqlite_test_path("mt_sqlite_unique_index_allow_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};
    auto descriptor = backend.ensure_collection(sqlite_user_schema(1));

    mt::WriteEnvelope first{
        .collection = descriptor.id,
        .key = "user:1",
        .kind = mt::WriteKind::Put,
        .value = mt::Json::object({{"email", "same@example.com"}}),
        .value_hash = test_hash(0xC1)
    };
    mt::WriteEnvelope same_key{
        .collection = descriptor.id,
        .key = "user:1",
        .kind = mt::WriteKind::Put,
        .value = mt::Json::object({{"email", "same@example.com"}, {"active", false}}),
        .value_hash = test_hash(0xC2)
    };
    mt::WriteEnvelope missing_path{
        .collection = descriptor.id,
        .key = "user:2",
        .kind = mt::WriteKind::Put,
        .value = mt::Json::object({{"active", true}}),
        .value_hash = test_hash(0xC3)
    };
    mt::WriteEnvelope delete_write{
        .collection = descriptor.id,
        .key = "user:3",
        .kind = mt::WriteKind::Delete,
        .value_hash = test_hash(0xC4)
    };

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        session->insert_history(descriptor.id, first, 1);
        session->upsert_current(descriptor.id, first, 1);
        session->insert_history(descriptor.id, same_key, 2);
        session->upsert_current(descriptor.id, same_key, 2);
        session->insert_history(descriptor.id, missing_path, 3);
        session->upsert_current(descriptor.id, missing_path, 3);
        session->insert_history(descriptor.id, delete_write, 4);
        session->upsert_current(descriptor.id, delete_write, 4);
        session->commit_backend_transaction();
    }

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
    test_sqlite_backend_reports_capabilities();
    test_sqlite_backend_rejects_unimplemented_operations();
    test_sqlite_backend_bootstrap_creates_private_metadata();
    test_sqlite_backend_ensure_collection_creates_and_gets_descriptor();
    test_sqlite_backend_ensure_collection_persists_across_instances();
    test_sqlite_backend_accepts_compatible_schema_change();
    test_sqlite_backend_rejects_incompatible_schema_change();
    test_sqlite_backend_session_commits_and_aborts_transactions();
    test_sqlite_backend_session_rejects_invalid_lifecycle();
    test_sqlite_backend_session_rejects_unsupported_queries();
    test_sqlite_backend_clock_reads_and_increments();
    test_sqlite_backend_clock_increment_requires_lock();
    test_sqlite_backend_transaction_ids_are_unique_and_persisted();
    test_sqlite_backend_active_transaction_metadata_registers_and_cleans_up();
    test_sqlite_backend_document_writes_insert_history_and_current();
    test_sqlite_backend_document_writes_store_delete_tombstone();
    test_sqlite_backend_document_writes_rollback_on_abort();
    test_sqlite_backend_point_reads_return_snapshot_versions();
    test_sqlite_backend_point_reads_return_tombstones();
    test_sqlite_backend_list_snapshot_orders_and_paginates();
    test_sqlite_backend_list_current_metadata_orders_and_paginates();
    test_sqlite_backend_query_snapshot_supports_key_prefix_and_json_equals();
    test_sqlite_backend_query_current_metadata_filters_and_limits();
    test_sqlite_backend_enforces_unique_indexes();
    test_sqlite_backend_unique_indexes_allow_same_key_update_missing_path_and_delete();
    test_sqlite_detail_connection_executes_sql();
    test_sqlite_detail_statement_reuses_bindings_after_reset();
    test_sqlite_detail_statement_binds_text_and_null();

    std::cout << "All sqlite backend tests passed.\n";
    return 0;
}
