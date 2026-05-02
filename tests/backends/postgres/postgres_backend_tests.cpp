#include "../../../src/backends/postgres/postgres_connection.hpp"
#include "../../../src/backends/postgres/postgres_schema.hpp"
#include "../backend_test_support.hpp"

#include "mt/backends/postgres.hpp"
#include "mt/json_parser.hpp"

#include <cstdlib>
#include <iostream>
#include <string>
#include <string_view>
#include <utility>

namespace
{
mt::CollectionSpec postgres_user_schema(std::string logical_name)
{
    return mt::CollectionSpec{
        .logical_name = std::move(logical_name),
        .indexes = {mt::IndexSpec::json_path_index("email", "$.email").make_unique()},
        .schema_version = 1,
        .key_field = "id",
        .fields = {mt::FieldSpec::string("id"), mt::FieldSpec::string("email")}
    };
}

mt::Json postgres_user_json(
    std::string id,
    std::string email
)
{
    return backend_test_user_json(std::move(id), std::move(email), true, "Postgres Test User");
}

void reset_postgres_private_tables(std::string_view dsn)
{
    auto connection = mt::backends::postgres::detail::Connection::open(dsn);
    connection.exec_command(
        "DROP TABLE IF EXISTS "
        "mt_current, mt_history, mt_active_transactions, mt_schema_snapshots, "
        "mt_collections, mt_clock, mt_metadata "
        "CASCADE"
    );
}

void test_postgres_connection_executes_query(std::string_view dsn)
{
    auto connection = mt::backends::postgres::detail::Connection::open(dsn);
    auto result = connection.exec_params("SELECT $1::text", {"mt"}, {PGRES_TUPLES_OK});

    if (result.rows() != 1 || result.columns() != 1 || result.value(0, 0) != "mt")
    {
        throw mt::BackendError("postgres connection wrapper returned unexpected query result");
    }
}

void require_postgres_backend_capabilities_are_pending(const mt::BackendCapabilities& capabilities)
{
    if (capabilities.query.key_prefix || capabilities.query.json_equals ||
        capabilities.query.json_contains || capabilities.query.order_by_key ||
        capabilities.query.custom_ordering || capabilities.schema.json_indexes ||
        capabilities.schema.unique_indexes || capabilities.schema.migrations)
    {
        throw mt::BackendError("postgres backend reports capabilities before implementation");
    }
}

void test_postgres_backend_entry_points(std::string_view dsn)
{
    auto backend = mt::backends::postgres::PostgresBackend(std::string(dsn));
    require_postgres_backend_capabilities_are_pending(backend.capabilities());
    backend.bootstrap(mt::BootstrapSpec{});

    auto session = backend.open_session();
    if (!session)
    {
        throw mt::BackendError("postgres backend returned null session");
    }

    session->begin_backend_transaction();
    session->abort_backend_transaction();
}

void test_postgres_backend_session_rejects_invalid_lifecycle(std::string_view dsn)
{
    auto backend = mt::backends::postgres::PostgresBackend(std::string(dsn));
    backend.bootstrap(mt::BootstrapSpec{});

    {
        auto session = backend.open_session();
        auto threw = false;
        try
        {
            session->commit_backend_transaction();
        }
        catch (const mt::BackendError&)
        {
            threw = true;
        }

        if (!threw)
        {
            throw mt::BackendError("postgres backend accepted commit without open transaction");
        }
    }

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        auto threw = false;
        try
        {
            session->begin_backend_transaction();
        }
        catch (const mt::BackendError&)
        {
            threw = true;
        }
        session->abort_backend_transaction();

        if (!threw)
        {
            throw mt::BackendError("postgres backend accepted nested session lifecycle");
        }
    }
}

void test_postgres_backend_clock_reads_and_increments(std::string_view dsn)
{
    auto backend = mt::backends::postgres::PostgresBackend(std::string(dsn));
    backend.bootstrap(mt::BootstrapSpec{});

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        if (session->read_clock() != mt::Version{0} ||
            session->lock_clock_and_read() != mt::Version{0})
        {
            throw mt::BackendError("postgres clock did not start at zero");
        }

        auto threw = false;
        try
        {
            session->lock_clock_and_read();
        }
        catch (const mt::BackendError&)
        {
            threw = true;
        }

        if (!threw)
        {
            throw mt::BackendError("postgres clock accepted second lock in one session");
        }

        if (session->increment_clock_and_return() != mt::Version{1})
        {
            throw mt::BackendError("postgres clock increment returned unexpected version");
        }
        session->commit_backend_transaction();
    }

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        if (session->read_clock() != mt::Version{1})
        {
            throw mt::BackendError("postgres clock increment was not persisted");
        }
        session->abort_backend_transaction();
    }
}

void test_postgres_backend_clock_increment_requires_lock(std::string_view dsn)
{
    auto backend = mt::backends::postgres::PostgresBackend(std::string(dsn));
    backend.bootstrap(mt::BootstrapSpec{});

    auto session = backend.open_session();
    session->begin_backend_transaction();
    try
    {
        session->increment_clock_and_return();
    }
    catch (const mt::BackendError&)
    {
        session->abort_backend_transaction();
        return;
    }

    session->abort_backend_transaction();
    throw mt::BackendError("postgres clock increment succeeded without clock lock");
}

void test_postgres_backend_transaction_ids_are_unique_and_persisted(std::string_view dsn)
{
    auto backend = mt::backends::postgres::PostgresBackend(std::string(dsn));
    backend.bootstrap(mt::BootstrapSpec{});

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        if (session->create_transaction_id() != "postgres:1" ||
            session->create_transaction_id() != "postgres:2")
        {
            throw mt::BackendError("postgres transaction ids did not start at expected values");
        }
        session->commit_backend_transaction();
    }

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        if (session->create_transaction_id() != "postgres:3")
        {
            throw mt::BackendError("postgres transaction ids were not persisted");
        }
        session->abort_backend_transaction();
    }
}

void test_postgres_backend_active_transaction_metadata_registers_and_cleans_up(std::string_view dsn)
{
    auto backend = mt::backends::postgres::PostgresBackend(std::string(dsn));
    backend.bootstrap(mt::BootstrapSpec{});

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        session->register_active_transaction("postgres:test", 42);
        session->unregister_active_transaction("missing");
        session->commit_backend_transaction();
    }

    {
        auto connection = mt::backends::postgres::detail::Connection::open(dsn);
        auto count = connection.exec_query(
            mt::backends::postgres::detail::PrivateSchemaSql::count_active_transactions()
        );
        if (count.rows() != 1 || count.value(0, 0) != "1")
        {
            throw mt::BackendError("postgres active transaction was not registered");
        }
    }

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        session->unregister_active_transaction("postgres:test");
        session->commit_backend_transaction();
    }

    {
        auto connection = mt::backends::postgres::detail::Connection::open(dsn);
        auto count = connection.exec_query(
            mt::backends::postgres::detail::PrivateSchemaSql::count_active_transactions()
        );
        if (count.rows() != 1 || count.value(0, 0) != "0")
        {
            throw mt::BackendError("postgres active transaction was not removed");
        }
    }
}

void test_postgres_backend_document_writes_insert_history_and_current(std::string_view dsn)
{
    auto backend = mt::backends::postgres::PostgresBackend(std::string(dsn));
    auto descriptor = backend.ensure_collection(postgres_user_schema("postgres_document_users"));

    mt::WriteEnvelope write{
        .collection = descriptor.id,
        .key = "user:1",
        .kind = mt::WriteKind::Put,
        .value = postgres_user_json("user:1", "a@example.com"),
        .value_hash = test_hash(0x12)
    };

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        session->insert_history(descriptor.id, write, 7);
        session->upsert_current(descriptor.id, write, 7);
        session->commit_backend_transaction();
    }

    auto connection = mt::backends::postgres::detail::Connection::open(dsn);
    auto history = connection.exec_params(
        mt::backends::postgres::detail::PrivateSchemaSql::select_history_row_by_collection_key(),
        {std::to_string(descriptor.id), "user:1"}, {PGRES_TUPLES_OK}
    );
    if (history.rows() != 1 || history.value(0, 0) != "7" || history.value(0, 1) != "f" ||
        history.value(0, 2) != "1213" ||
        mt::parse_json(history.value(0, 3)) != postgres_user_json("user:1", "a@example.com"))
    {
        throw mt::BackendError("postgres history document write did not round-trip");
    }

    auto current = connection.exec_params(
        mt::backends::postgres::detail::PrivateSchemaSql::select_current_row_by_collection_key(),
        {std::to_string(descriptor.id), "user:1"}, {PGRES_TUPLES_OK}
    );
    if (current.rows() != 1 || current.value(0, 0) != "7" || current.value(0, 1) != "f" ||
        current.value(0, 2) != "1213" ||
        mt::parse_json(current.value(0, 3)) != postgres_user_json("user:1", "a@example.com"))
    {
        throw mt::BackendError("postgres current document write did not round-trip");
    }
}

void test_postgres_backend_document_writes_store_delete_tombstone(std::string_view dsn)
{
    auto backend = mt::backends::postgres::PostgresBackend(std::string(dsn));
    auto descriptor = backend.ensure_collection(postgres_user_schema("postgres_document_deletes"));

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

    auto connection = mt::backends::postgres::detail::Connection::open(dsn);
    auto current = connection.exec_params(
        mt::backends::postgres::detail::PrivateSchemaSql::select_current_row_by_collection_key(),
        {std::to_string(descriptor.id), "user:1"}, {PGRES_TUPLES_OK}
    );
    if (current.rows() != 1 || current.value(0, 0) != "8" || current.value(0, 1) != "t" ||
        current.value(0, 2) != "2122" || !current.is_null(0, 3))
    {
        throw mt::BackendError("postgres current delete tombstone did not round-trip");
    }
}

void test_postgres_backend_document_writes_rollback_on_abort(std::string_view dsn)
{
    auto backend = mt::backends::postgres::PostgresBackend(std::string(dsn));
    auto descriptor = backend.ensure_collection(postgres_user_schema("postgres_document_aborts"));

    mt::WriteEnvelope write{
        .collection = descriptor.id,
        .key = "user:1",
        .kind = mt::WriteKind::Put,
        .value = postgres_user_json("user:1", "a@example.com"),
        .value_hash = test_hash(0x31)
    };

    {
        auto session = backend.open_session();
        session->begin_backend_transaction();
        session->insert_history(descriptor.id, write, 9);
        session->upsert_current(descriptor.id, write, 9);
        session->abort_backend_transaction();
    }

    auto connection = mt::backends::postgres::detail::Connection::open(dsn);
    auto history = connection.exec_params(
        mt::backends::postgres::detail::PrivateSchemaSql::select_history_row_by_collection_key(),
        {std::to_string(descriptor.id), "user:1"}, {PGRES_TUPLES_OK}
    );
    if (history.rows() != 0)
    {
        throw mt::BackendError("postgres document abort did not roll back history rows");
    }
}

void test_postgres_backend_point_reads_return_snapshot_versions(std::string_view dsn)
{
    auto backend = mt::backends::postgres::PostgresBackend(std::string(dsn));
    auto descriptor = backend.ensure_collection(postgres_user_schema("postgres_point_read_users"));

    mt::WriteEnvelope first{
        .collection = descriptor.id,
        .key = "user:1",
        .kind = mt::WriteKind::Put,
        .value = postgres_user_json("user:1", "old@example.com"),
        .value_hash = test_hash(0x41)
    };
    mt::WriteEnvelope second{
        .collection = descriptor.id,
        .key = "user:1",
        .kind = mt::WriteKind::Put,
        .value = postgres_user_json("user:1", "new@example.com"),
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

        if (missing || !old_doc || old_doc->version != mt::Version{1} ||
            old_doc->value["email"].as_string() != "old@example.com" ||
            old_doc->value_hash != test_hash(0x41) || !new_doc ||
            new_doc->version != mt::Version{2} ||
            new_doc->value["email"].as_string() != "new@example.com" ||
            new_doc->value_hash != test_hash(0x51) || !current ||
            current->version != mt::Version{2} || current->deleted ||
            current->value_hash != test_hash(0x51))
        {
            throw mt::BackendError("postgres point reads returned unexpected document state");
        }
    }
}

void test_postgres_backend_point_reads_return_tombstones(std::string_view dsn)
{
    auto backend = mt::backends::postgres::PostgresBackend(std::string(dsn));
    auto descriptor =
        backend.ensure_collection(postgres_user_schema("postgres_point_read_deletes"));

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

        if (!doc || !doc->deleted || !doc->value.is_null() || doc->value_hash != test_hash(0x61) ||
            !current || !current->deleted || current->value_hash != test_hash(0x61))
        {
            throw mt::BackendError("postgres point reads did not return tombstones");
        }
    }
}

void test_postgres_backend_list_snapshot_orders_and_paginates(std::string_view dsn)
{
    auto backend = mt::backends::postgres::PostgresBackend(std::string(dsn));
    auto descriptor =
        backend.ensure_collection(postgres_user_schema("postgres_list_snapshot_users"));

    mt::WriteEnvelope user_1{
        .collection = descriptor.id,
        .key = "user:1",
        .kind = mt::WriteKind::Put,
        .value = postgres_user_json("user:1", "one@example.com"),
        .value_hash = test_hash(0x71)
    };
    mt::WriteEnvelope user_2_old{
        .collection = descriptor.id,
        .key = "user:2",
        .kind = mt::WriteKind::Put,
        .value = postgres_user_json("user:2", "old@example.com"),
        .value_hash = test_hash(0x72)
    };
    mt::WriteEnvelope user_2_new{
        .collection = descriptor.id,
        .key = "user:2",
        .kind = mt::WriteKind::Put,
        .value = postgres_user_json("user:2", "new@example.com"),
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

        if (rows.size() != 2 || rows[0].key != "user:1" || rows[1].key != "user:2" ||
            rows[1].value["email"].as_string() != "new@example.com" || after_rows.size() != 1 ||
            after_rows[0].key != "user:2" || early_rows.size() != 2 ||
            early_rows[0].key != "user:1" || early_rows[1].key != "user:2" ||
            early_rows[1].value["email"].as_string() != "old@example.com")
        {
            throw mt::BackendError("postgres snapshot list returned unexpected rows");
        }
    }
}

void test_postgres_backend_list_current_metadata_orders_and_paginates(std::string_view dsn)
{
    auto backend = mt::backends::postgres::PostgresBackend(std::string(dsn));
    auto descriptor =
        backend.ensure_collection(postgres_user_schema("postgres_list_current_users"));

    mt::WriteEnvelope user_1{
        .collection = descriptor.id,
        .key = "user:1",
        .kind = mt::WriteKind::Put,
        .value = postgres_user_json("user:1", "one@example.com"),
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

        if (rows.size() != 1 || rows[0].key != "user:2" || rows[0].version != mt::Version{1} ||
            !rows[0].deleted || rows[0].value_hash != test_hash(0x82))
        {
            throw mt::BackendError("postgres current metadata list returned unexpected rows");
        }
    }
}

void test_postgres_backend_query_snapshot_supports_key_prefix_and_json_equals(std::string_view dsn)
{
    auto backend = mt::backends::postgres::PostgresBackend(std::string(dsn));
    auto descriptor =
        backend.ensure_collection(postgres_user_schema("postgres_query_snapshot_users"));

    mt::WriteEnvelope user_1{
        .collection = descriptor.id,
        .key = "user:1",
        .kind = mt::WriteKind::Put,
        .value = backend_test_user_json("user:1", "match@example.com", true, "Postgres Test User"),
        .value_hash = test_hash(0x91)
    };
    mt::WriteEnvelope user_2{
        .collection = descriptor.id,
        .key = "user:2",
        .kind = mt::WriteKind::Put,
        .value = backend_test_user_json("user:2", "skip@example.com", false, "Postgres Test User"),
        .value_hash = test_hash(0x92)
    };
    mt::WriteEnvelope other{
        .collection = descriptor.id,
        .key = "other:1",
        .kind = mt::WriteKind::Put,
        .value = backend_test_user_json("other:1", "other@example.com", true, "Postgres Test User"),
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

        if (rows.size() != 1 || rows[0].key != "user:1" || !rows[0].value["active"].as_bool())
        {
            throw mt::BackendError("postgres snapshot query returned unexpected rows");
        }
    }
}

void test_postgres_backend_query_current_metadata_filters_and_limits(std::string_view dsn)
{
    auto backend = mt::backends::postgres::PostgresBackend(std::string(dsn));
    auto descriptor =
        backend.ensure_collection(postgres_user_schema("postgres_query_current_users"));

    mt::WriteEnvelope user_1{
        .collection = descriptor.id,
        .key = "user:1",
        .kind = mt::WriteKind::Put,
        .value = backend_test_user_json("user:1", "one@example.com", true, "Postgres Test User"),
        .value_hash = test_hash(0xA1)
    };
    mt::WriteEnvelope user_2{
        .collection = descriptor.id,
        .key = "user:2",
        .kind = mt::WriteKind::Put,
        .value = backend_test_user_json("user:2", "two@example.com", true, "Postgres Test User"),
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

        if (rows.size() != 1 || rows[0].key != "user:2" || rows[0].deleted ||
            rows[0].value_hash != test_hash(0xA2))
        {
            throw mt::BackendError("postgres current metadata query returned unexpected rows");
        }
    }
}

void test_postgres_backend_query_rejects_unsupported_features(std::string_view dsn)
{
    auto backend = mt::backends::postgres::PostgresBackend(std::string(dsn));
    auto session = backend.open_session();

    mt::QuerySpec contains;
    contains.predicates.push_back(
        mt::QueryPredicate{
            .op = mt::QueryOp::JsonContains,
            .path = "$",
            .value = postgres_user_json("user:1", "a@example.com")
        }
    );

    session->begin_backend_transaction();
    auto threw = false;
    try
    {
        session->query_snapshot(1, contains, 0);
    }
    catch (const mt::BackendError&)
    {
        threw = true;
    }
    if (!threw)
    {
        session->abort_backend_transaction();
        throw mt::BackendError("postgres query accepted JSON contains predicate");
    }

    auto unordered = mt::QuerySpec::key_prefix("user:");
    unordered.order_by_key = false;
    threw = false;
    try
    {
        session->query_current_metadata(1, unordered);
    }
    catch (const mt::BackendError&)
    {
        threw = true;
    }
    session->abort_backend_transaction();

    if (!threw)
    {
        throw mt::BackendError("postgres query accepted unsupported ordering");
    }
}

void test_postgres_backend_enforces_unique_indexes(std::string_view dsn)
{
    auto backend = mt::backends::postgres::PostgresBackend(std::string(dsn));
    auto descriptor = backend.ensure_collection(postgres_user_schema("postgres_unique_users"));

    mt::WriteEnvelope first{
        .collection = descriptor.id,
        .key = "user:1",
        .kind = mt::WriteKind::Put,
        .value = postgres_user_json("user:1", "same@example.com"),
        .value_hash = test_hash(0xB1)
    };
    mt::WriteEnvelope conflict{
        .collection = descriptor.id,
        .key = "user:2",
        .kind = mt::WriteKind::Put,
        .value = postgres_user_json("user:2", "same@example.com"),
        .value_hash = test_hash(0xB2)
    };

    auto session = backend.open_session();
    session->begin_backend_transaction();
    session->insert_history(descriptor.id, first, 1);
    session->upsert_current(descriptor.id, first, 1);

    auto threw = false;
    try
    {
        session->upsert_current(descriptor.id, conflict, 2);
    }
    catch (const mt::BackendError&)
    {
        threw = true;
    }
    session->abort_backend_transaction();

    if (!threw)
    {
        throw mt::BackendError("postgres accepted unique index conflict");
    }
}

void test_postgres_backend_unique_indexes_allow_same_key_missing_path_and_delete(
    std::string_view dsn
)
{
    auto backend = mt::backends::postgres::PostgresBackend(std::string(dsn));
    auto descriptor =
        backend.ensure_collection(postgres_user_schema("postgres_unique_allowed_users"));

    mt::WriteEnvelope first{
        .collection = descriptor.id,
        .key = "user:1",
        .kind = mt::WriteKind::Put,
        .value = postgres_user_json("user:1", "same@example.com"),
        .value_hash = test_hash(0xC1)
    };
    mt::WriteEnvelope same_key{
        .collection = descriptor.id,
        .key = "user:1",
        .kind = mt::WriteKind::Put,
        .value = backend_test_user_json("user:1", "same@example.com", false, "Postgres Test User"),
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

void test_postgres_bootstrap_creates_private_tables(std::string_view dsn)
{
    auto backend = mt::backends::postgres::PostgresBackend(std::string(dsn));
    backend.bootstrap(mt::BootstrapSpec{.metadata_schema_version = 3});

    auto connection = mt::backends::postgres::detail::Connection::open(dsn);
    auto tables = connection.exec_query(
        mt::backends::postgres::detail::PrivateSchemaSql::count_private_tables()
    );
    if (tables.rows() != 1 || tables.value(0, 0) != "7")
    {
        throw mt::BackendError("postgres bootstrap did not create all private tables");
    }

    auto metadata = connection.exec_query(
        mt::backends::postgres::detail::PrivateSchemaSql::select_metadata_schema_version()
    );
    if (metadata.rows() != 1 || metadata.value(0, 0) != "3")
    {
        throw mt::BackendError("postgres bootstrap did not seed metadata schema version");
    }

    auto clock =
        connection.exec_query(mt::backends::postgres::detail::PrivateSchemaSql::select_clock_row());
    if (clock.rows() != 1 || clock.value(0, 0) != "0" || clock.value(0, 1) != "1")
    {
        throw mt::BackendError("postgres bootstrap did not seed clock row");
    }
}

void test_postgres_collection_metadata_round_trips(std::string_view dsn)
{
    auto backend = mt::backends::postgres::PostgresBackend(std::string(dsn));
    auto initial = postgres_user_schema("postgres_schema_users");
    auto requested = postgres_user_schema("postgres_schema_users");
    requested.schema_version = 2;
    requested.fields.push_back(mt::FieldSpec::string("nickname").mark_required(false));

    auto first = backend.ensure_collection(initial);
    auto second = backend.ensure_collection(requested);
    auto descriptor = backend.get_collection("postgres_schema_users");

    if (first.id != second.id || descriptor.id != first.id || descriptor.schema_version != 2)
    {
        throw mt::BackendError("postgres collection descriptors are inconsistent");
    }

    auto connection = mt::backends::postgres::detail::Connection::open(dsn);
    auto snapshot =
        mt::backends::postgres::detail::load_collection_spec(connection, "postgres_schema_users");
    if (!snapshot || snapshot->schema_version != 2 || snapshot->fields.size() != 3 ||
        snapshot->indexes.size() != 1 || !snapshot->indexes[0].unique)
    {
        throw mt::BackendError("postgres collection schema snapshot did not round-trip");
    }
}

void test_postgres_rejects_incompatible_schema_change(std::string_view dsn)
{
    auto backend = mt::backends::postgres::PostgresBackend(std::string(dsn));
    auto initial = postgres_user_schema("postgres_incompatible_schema_users");
    auto requested = postgres_user_schema("postgres_incompatible_schema_users");
    requested.schema_version = 2;
    requested.fields = {mt::FieldSpec::string("id")};

    auto first = backend.ensure_collection(initial);
    try
    {
        backend.ensure_collection(requested);
    }
    catch (const mt::BackendError&)
    {
        auto descriptor = backend.get_collection("postgres_incompatible_schema_users");
        if (descriptor.schema_version != first.schema_version)
        {
            throw mt::BackendError("postgres incompatible schema update changed descriptor");
        }
        return;
    }

    throw mt::BackendError("postgres accepted incompatible schema change");
}

} // namespace

int main()
{
    const auto* dsn = std::getenv("MT_POSTGRES_TEST_DSN");
    if (!dsn || *dsn == '\0')
    {
        std::cout << "Skipping postgres backend tests: MT_POSTGRES_TEST_DSN is not set.\n";
        return 0;
    }

    try
    {
        test_postgres_connection_executes_query(dsn);
        reset_postgres_private_tables(dsn);
        test_postgres_backend_entry_points(dsn);
        test_postgres_bootstrap_creates_private_tables(dsn);
        test_postgres_backend_session_rejects_invalid_lifecycle(dsn);
        test_postgres_backend_clock_reads_and_increments(dsn);
        test_postgres_backend_clock_increment_requires_lock(dsn);
        test_postgres_backend_transaction_ids_are_unique_and_persisted(dsn);
        test_postgres_backend_active_transaction_metadata_registers_and_cleans_up(dsn);
        test_postgres_backend_document_writes_insert_history_and_current(dsn);
        test_postgres_backend_document_writes_store_delete_tombstone(dsn);
        test_postgres_backend_document_writes_rollback_on_abort(dsn);
        test_postgres_backend_point_reads_return_snapshot_versions(dsn);
        test_postgres_backend_point_reads_return_tombstones(dsn);
        test_postgres_backend_list_snapshot_orders_and_paginates(dsn);
        test_postgres_backend_list_current_metadata_orders_and_paginates(dsn);
        test_postgres_backend_query_snapshot_supports_key_prefix_and_json_equals(dsn);
        test_postgres_backend_query_current_metadata_filters_and_limits(dsn);
        test_postgres_backend_query_rejects_unsupported_features(dsn);
        test_postgres_backend_enforces_unique_indexes(dsn);
        test_postgres_backend_unique_indexes_allow_same_key_missing_path_and_delete(dsn);
        test_postgres_collection_metadata_round_trips(dsn);
        test_postgres_rejects_incompatible_schema_change(dsn);
    }
    catch (const mt::Error& error)
    {
        std::cerr << error.what() << "\n";
        return 1;
    }

    std::cout << "Postgres backend connection tests passed.\n";
    return 0;
}
