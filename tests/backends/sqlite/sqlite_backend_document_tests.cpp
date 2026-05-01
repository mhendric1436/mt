#include "sqlite_test_support.hpp"

void test_sqlite_backend_document_writes_insert_history_and_current()
{
    auto path = sqlite_test_path("mt_sqlite_document_write_test.sqlite");
    mt::backends::sqlite::SqliteBackend backend{path.string()};
    auto descriptor = backend.ensure_collection(sqlite_user_schema(1));

    mt::WriteEnvelope write{
        .collection = descriptor.id,
        .key = "user:1",
        .kind = mt::WriteKind::Put,
        .value = sqlite_user_json("user:1", "a@example.com"),
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
        connection.get(),
        mt::backends::sqlite::detail::PrivateSchemaSql::select_history_row_by_collection_key()
    };
    history.bind_int64(1, descriptor.id);
    history.bind_text(2, "user:1");
    EXPECT_TRUE(history.step());
    EXPECT_EQ(history.column_int64(0), std::int64_t{7});
    EXPECT_EQ(history.column_int64(1), std::int64_t{0});
    EXPECT_EQ(history.column_text(2), std::string("1213"));
    EXPECT_EQ(
        history.column_text(3), sqlite_user_json("user:1", "a@example.com").canonical_string()
    );

    mt::backends::sqlite::detail::Statement current{
        connection.get(),
        mt::backends::sqlite::detail::PrivateSchemaSql::select_current_row_by_collection_key()
    };
    current.bind_int64(1, descriptor.id);
    current.bind_text(2, "user:1");
    EXPECT_TRUE(current.step());
    EXPECT_EQ(current.column_int64(0), std::int64_t{7});
    EXPECT_EQ(current.column_int64(1), std::int64_t{0});
    EXPECT_EQ(current.column_text(2), std::string("1213"));
    EXPECT_EQ(
        current.column_text(3), sqlite_user_json("user:1", "a@example.com").canonical_string()
    );

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
        connection.get(),
        mt::backends::sqlite::detail::PrivateSchemaSql::select_current_row_by_collection_key()
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
        .value = sqlite_user_json("user:1", "a@example.com"),
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
        connection.get(), mt::backends::sqlite::detail::PrivateSchemaSql::count_history_rows()
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
        .value = sqlite_user_json("user:1", "old@example.com"),
        .value_hash = test_hash(0x41)
    };
    mt::WriteEnvelope second{
        .collection = descriptor.id,
        .key = "user:1",
        .kind = mt::WriteKind::Put,
        .value = sqlite_user_json("user:1", "new@example.com"),
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
        .value = sqlite_user_json("user:1", "one@example.com"),
        .value_hash = test_hash(0x71)
    };
    mt::WriteEnvelope user_2_old{
        .collection = descriptor.id,
        .key = "user:2",
        .kind = mt::WriteKind::Put,
        .value = sqlite_user_json("user:2", "old@example.com"),
        .value_hash = test_hash(0x72)
    };
    mt::WriteEnvelope user_2_new{
        .collection = descriptor.id,
        .key = "user:2",
        .kind = mt::WriteKind::Put,
        .value = sqlite_user_json("user:2", "new@example.com"),
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
        .value = sqlite_user_json("user:1", "one@example.com"),
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
        .value = sqlite_user_json("user:1", "match@example.com", true),
        .value_hash = test_hash(0x91)
    };
    mt::WriteEnvelope user_2{
        .collection = descriptor.id,
        .key = "user:2",
        .kind = mt::WriteKind::Put,
        .value = sqlite_user_json("user:2", "skip@example.com", false),
        .value_hash = test_hash(0x92)
    };
    mt::WriteEnvelope other{
        .collection = descriptor.id,
        .key = "other:1",
        .kind = mt::WriteKind::Put,
        .value = sqlite_user_json("other:1", "other@example.com", true),
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
        .value = sqlite_user_json("user:1", "one@example.com", true),
        .value_hash = test_hash(0xA1)
    };
    mt::WriteEnvelope user_2{
        .collection = descriptor.id,
        .key = "user:2",
        .kind = mt::WriteKind::Put,
        .value = sqlite_user_json("user:2", "two@example.com", true),
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
        .value = sqlite_user_json("user:1", "same@example.com"),
        .value_hash = test_hash(0xB1)
    };
    mt::WriteEnvelope conflict{
        .collection = descriptor.id,
        .key = "user:2",
        .kind = mt::WriteKind::Put,
        .value = sqlite_user_json("user:2", "same@example.com"),
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
        .value = sqlite_user_json("user:1", "same@example.com"),
        .value_hash = test_hash(0xC1)
    };
    mt::WriteEnvelope same_key{
        .collection = descriptor.id,
        .key = "user:1",
        .kind = mt::WriteKind::Put,
        .value = sqlite_user_json("user:1", "same@example.com", false),
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
