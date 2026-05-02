#include "postgres_schema.hpp"

#include "../common/schema_codec.hpp"

#include "mt/errors.hpp"

#include <cstdint>
#include <string>
#include <utility>

namespace mt::backends::postgres::detail
{

namespace
{
std::uint64_t parse_u64(std::string_view value)
{
    return static_cast<std::uint64_t>(std::stoull(std::string(value)));
}

int parse_int(std::string_view value)
{
    return std::stoi(std::string(value));
}

} // namespace

std::string PrivateSchemaSql::create_metadata_table()
{
    return "CREATE TABLE IF NOT EXISTS mt_metadata ("
           "key TEXT PRIMARY KEY,"
           "value BIGINT NOT NULL"
           ")";
}

std::string PrivateSchemaSql::upsert_metadata_schema_version()
{
    return "INSERT INTO mt_metadata (key, value) "
           "VALUES ('metadata_schema_version', $1::bigint) "
           "ON CONFLICT (key) DO UPDATE SET value = GREATEST(mt_metadata.value, excluded.value)";
}

std::string PrivateSchemaSql::create_clock_table()
{
    return "CREATE TABLE IF NOT EXISTS mt_clock ("
           "id INTEGER PRIMARY KEY CHECK (id = 1),"
           "version BIGINT NOT NULL,"
           "next_tx_id BIGINT NOT NULL"
           ")";
}

std::string PrivateSchemaSql::insert_default_clock_row()
{
    return "INSERT INTO mt_clock (id, version, next_tx_id) "
           "VALUES (1, 0, 1) "
           "ON CONFLICT (id) DO NOTHING";
}

std::string PrivateSchemaSql::create_collections_table()
{
    return "CREATE TABLE IF NOT EXISTS mt_collections ("
           "id BIGSERIAL PRIMARY KEY,"
           "logical_name TEXT NOT NULL UNIQUE,"
           "schema_version INTEGER NOT NULL,"
           "key_field TEXT NOT NULL"
           ")";
}

std::string PrivateSchemaSql::create_schema_snapshots_table()
{
    return "CREATE TABLE IF NOT EXISTS mt_schema_snapshots ("
           "collection_id BIGINT PRIMARY KEY REFERENCES mt_collections(id),"
           "schema_json TEXT NOT NULL,"
           "indexes_json TEXT NOT NULL"
           ")";
}

std::string PrivateSchemaSql::create_active_transactions_table()
{
    return "CREATE TABLE IF NOT EXISTS mt_active_transactions ("
           "tx_id TEXT PRIMARY KEY,"
           "start_version BIGINT NOT NULL"
           ")";
}

std::string PrivateSchemaSql::create_history_table()
{
    return "CREATE TABLE IF NOT EXISTS mt_history ("
           "collection_id BIGINT NOT NULL REFERENCES mt_collections(id),"
           "document_key TEXT NOT NULL,"
           "version BIGINT NOT NULL,"
           "deleted BOOLEAN NOT NULL,"
           "value_hash TEXT NOT NULL,"
           "value_json JSONB,"
           "PRIMARY KEY (collection_id, document_key, version)"
           ")";
}

std::string PrivateSchemaSql::create_history_snapshot_index()
{
    return "CREATE INDEX IF NOT EXISTS mt_history_snapshot_idx "
           "ON mt_history (collection_id, document_key, version)";
}

std::string PrivateSchemaSql::create_current_table()
{
    return "CREATE TABLE IF NOT EXISTS mt_current ("
           "collection_id BIGINT NOT NULL REFERENCES mt_collections(id),"
           "document_key TEXT NOT NULL,"
           "version BIGINT NOT NULL,"
           "deleted BOOLEAN NOT NULL,"
           "value_hash TEXT NOT NULL,"
           "value_json JSONB,"
           "PRIMARY KEY (collection_id, document_key)"
           ")";
}

std::string PrivateSchemaSql::count_private_tables()
{
    return "SELECT COUNT(*) "
           "FROM information_schema.tables "
           "WHERE table_schema = current_schema() "
           "AND table_name IN ("
           "'mt_metadata', 'mt_collections', 'mt_schema_snapshots', 'mt_clock', "
           "'mt_active_transactions', 'mt_history', 'mt_current')";
}

std::string PrivateSchemaSql::select_metadata_schema_version()
{
    return "SELECT value FROM mt_metadata WHERE key = 'metadata_schema_version'";
}

std::string PrivateSchemaSql::select_clock_row()
{
    return "SELECT version, next_tx_id FROM mt_clock WHERE id = 1";
}

std::string PrivateSchemaSql::begin_transaction()
{
    return "BEGIN";
}

std::string PrivateSchemaSql::commit()
{
    return "COMMIT";
}

std::string PrivateSchemaSql::rollback()
{
    return "ROLLBACK";
}

std::string PrivateSchemaSql::select_clock_version()
{
    return "SELECT version FROM mt_clock WHERE id = 1";
}

std::string PrivateSchemaSql::lock_clock_version()
{
    return "SELECT version FROM mt_clock WHERE id = 1 FOR UPDATE";
}

std::string PrivateSchemaSql::increment_clock_version_returning()
{
    return "UPDATE mt_clock SET version = version + 1 WHERE id = 1 RETURNING version";
}

std::string PrivateSchemaSql::increment_next_tx_id_returning()
{
    return "UPDATE mt_clock "
           "SET next_tx_id = next_tx_id + 1 "
           "WHERE id = 1 "
           "RETURNING next_tx_id - 1";
}

std::string PrivateSchemaSql::insert_or_replace_active_transaction()
{
    return "INSERT INTO mt_active_transactions (tx_id, start_version) "
           "VALUES ($1, $2::bigint) "
           "ON CONFLICT (tx_id) DO UPDATE SET start_version = EXCLUDED.start_version";
}

std::string PrivateSchemaSql::delete_active_transaction()
{
    return "DELETE FROM mt_active_transactions WHERE tx_id = $1";
}

std::string PrivateSchemaSql::count_active_transactions()
{
    return "SELECT COUNT(*) FROM mt_active_transactions";
}

std::string PrivateSchemaSql::select_snapshot_document()
{
    return "SELECT version, deleted, value_hash, value_json::text "
           "FROM mt_history "
           "WHERE collection_id = $1::bigint AND document_key = $2 AND version <= $3::bigint "
           "ORDER BY version DESC LIMIT 1";
}

std::string PrivateSchemaSql::select_current_metadata()
{
    return "SELECT version, deleted, value_hash "
           "FROM mt_current "
           "WHERE collection_id = $1::bigint AND document_key = $2";
}

std::string PrivateSchemaSql::select_snapshot_list(
    bool has_after_key,
    bool has_limit
)
{
    auto sql = std::string(
        "SELECT h.document_key, h.version, h.deleted, h.value_hash, h.value_json::text "
        "FROM mt_history h "
        "WHERE h.collection_id = $1::bigint "
        "AND h.version = ("
        "SELECT MAX(h2.version) FROM mt_history h2 "
        "WHERE h2.collection_id = h.collection_id "
        "AND h2.document_key = h.document_key "
        "AND h2.version <= $2::bigint"
        ") "
        "AND h.deleted = false "
    );
    auto next_param = 3;
    if (has_after_key)
    {
        sql += "AND h.document_key > $" + std::to_string(next_param++) + " ";
    }
    sql += "ORDER BY h.document_key ";
    if (has_limit)
    {
        sql += "LIMIT $" + std::to_string(next_param) + "::bigint";
    }
    return sql;
}

std::string PrivateSchemaSql::select_current_metadata_list(
    bool has_after_key,
    bool has_limit
)
{
    auto sql = std::string(
        "SELECT document_key, version, deleted, value_hash "
        "FROM mt_current "
        "WHERE collection_id = $1::bigint "
    );
    auto next_param = 2;
    if (has_after_key)
    {
        sql += "AND document_key > $" + std::to_string(next_param++) + " ";
    }
    sql += "ORDER BY document_key ";
    if (has_limit)
    {
        sql += "LIMIT $" + std::to_string(next_param) + "::bigint";
    }
    return sql;
}

std::string PrivateSchemaSql::select_current_query_candidates(bool has_after_key)
{
    auto sql = std::string(
        "SELECT document_key, version, deleted, value_hash, value_json::text "
        "FROM mt_current "
        "WHERE collection_id = $1::bigint "
    );
    if (has_after_key)
    {
        sql += "AND document_key > $2 ";
    }
    sql += "ORDER BY document_key";
    return sql;
}

std::string PrivateSchemaSql::select_current_unique_index_candidates()
{
    return "SELECT document_key, value_json::text "
           "FROM mt_current "
           "WHERE collection_id = $1::bigint "
           "AND document_key <> $2 "
           "AND deleted = false";
}

std::string PrivateSchemaSql::insert_history()
{
    return "INSERT INTO mt_history "
           "(collection_id, document_key, version, deleted, value_hash, value_json) "
           "VALUES ($1::bigint, $2, $3::bigint, $4::boolean, $5, "
           "CASE WHEN $4::boolean THEN NULL ELSE $6::jsonb END)";
}

std::string PrivateSchemaSql::upsert_current()
{
    return "INSERT INTO mt_current "
           "(collection_id, document_key, version, deleted, value_hash, value_json) "
           "VALUES ($1::bigint, $2, $3::bigint, $4::boolean, $5, "
           "CASE WHEN $4::boolean THEN NULL ELSE $6::jsonb END) "
           "ON CONFLICT (collection_id, document_key) DO UPDATE SET "
           "version = EXCLUDED.version, "
           "deleted = EXCLUDED.deleted, "
           "value_hash = EXCLUDED.value_hash, "
           "value_json = EXCLUDED.value_json";
}

std::string PrivateSchemaSql::select_history_row_by_collection_key()
{
    return "SELECT version, deleted, value_hash, value_json::text "
           "FROM mt_history "
           "WHERE collection_id = $1::bigint AND document_key = $2 "
           "ORDER BY version";
}

std::string PrivateSchemaSql::select_current_row_by_collection_key()
{
    return "SELECT version, deleted, value_hash, value_json::text "
           "FROM mt_current "
           "WHERE collection_id = $1::bigint AND document_key = $2";
}

std::string PrivateSchemaSql::count_history_rows()
{
    return "SELECT COUNT(*) FROM mt_history";
}

std::string PrivateSchemaSql::select_collection_spec_by_logical_name()
{
    return "SELECT c.logical_name, c.schema_version, c.key_field, s.schema_json, s.indexes_json "
           "FROM mt_collections c "
           "JOIN mt_schema_snapshots s ON s.collection_id = c.id "
           "WHERE c.logical_name = $1";
}

std::string PrivateSchemaSql::select_collection_descriptor_by_logical_name()
{
    return "SELECT id, logical_name, schema_version "
           "FROM mt_collections WHERE logical_name = $1";
}

std::string PrivateSchemaSql::insert_collection()
{
    return "INSERT INTO mt_collections (logical_name, schema_version, key_field) "
           "VALUES ($1, $2::integer, $3) RETURNING id, logical_name, schema_version";
}

std::string PrivateSchemaSql::insert_schema_snapshot()
{
    return "INSERT INTO mt_schema_snapshots (collection_id, schema_json, indexes_json) "
           "VALUES ($1::bigint, $2, $3)";
}

std::string PrivateSchemaSql::update_collection()
{
    return "UPDATE mt_collections "
           "SET schema_version = $1::integer, key_field = $2 "
           "WHERE logical_name = $3";
}

std::string PrivateSchemaSql::update_schema_snapshot()
{
    return "UPDATE mt_schema_snapshots "
           "SET schema_json = $1, indexes_json = $2 "
           "WHERE collection_id = (SELECT id FROM mt_collections WHERE logical_name = $3)";
}

std::string PrivateSchemaSql::select_collection_indexes_by_id()
{
    return "SELECT indexes_json FROM mt_schema_snapshots WHERE collection_id = $1::bigint";
}

void bootstrap_schema(
    Connection& connection,
    const BootstrapSpec& spec
)
{
    connection.exec_command(PrivateSchemaSql::create_metadata_table());
    connection.exec_params(
        PrivateSchemaSql::upsert_metadata_schema_version(),
        {std::to_string(spec.metadata_schema_version)}, {PGRES_COMMAND_OK}
    );

    connection.exec_command(PrivateSchemaSql::create_clock_table());
    connection.exec_command(PrivateSchemaSql::insert_default_clock_row());

    connection.exec_command(PrivateSchemaSql::create_collections_table());
    connection.exec_command(PrivateSchemaSql::create_schema_snapshots_table());

    connection.exec_command(PrivateSchemaSql::create_active_transactions_table());

    connection.exec_command(PrivateSchemaSql::create_history_table());
    connection.exec_command(PrivateSchemaSql::create_history_snapshot_index());

    connection.exec_command(PrivateSchemaSql::create_current_table());
}

std::optional<CollectionSpec> load_collection_spec(
    Connection& connection,
    std::string_view logical_name
)
{
    auto result = connection.exec_params(
        PrivateSchemaSql::select_collection_spec_by_logical_name(), {std::string(logical_name)},
        {PGRES_TUPLES_OK}
    );
    if (result.rows() == 0)
    {
        return std::nullopt;
    }

    return CollectionSpec{
        .logical_name = std::string(result.value(0, 0)),
        .indexes = common::deserialize_indexes(result.value(0, 4)),
        .schema_version = parse_int(result.value(0, 1)),
        .key_field = std::string(result.value(0, 2)),
        .fields = common::deserialize_fields(result.value(0, 3))
    };
}

CollectionDescriptor load_collection_descriptor(
    Connection& connection,
    std::string_view logical_name
)
{
    auto result = connection.exec_params(
        PrivateSchemaSql::select_collection_descriptor_by_logical_name(),
        {std::string(logical_name)}, {PGRES_TUPLES_OK}
    );
    if (result.rows() == 0)
    {
        throw BackendError("postgres collection not found");
    }

    return CollectionDescriptor{
        .id = parse_u64(result.value(0, 0)),
        .logical_name = std::string(result.value(0, 1)),
        .schema_version = parse_int(result.value(0, 2))
    };
}

CollectionDescriptor insert_collection(
    Connection& connection,
    const CollectionSpec& spec
)
{
    auto result = connection.exec_params(
        PrivateSchemaSql::insert_collection(),
        {spec.logical_name, std::to_string(spec.schema_version), spec.key_field}, {PGRES_TUPLES_OK}
    );
    if (result.rows() != 1)
    {
        throw BackendError("postgres collection insert did not return descriptor");
    }

    auto descriptor = CollectionDescriptor{
        .id = parse_u64(result.value(0, 0)),
        .logical_name = std::string(result.value(0, 1)),
        .schema_version = parse_int(result.value(0, 2))
    };

    connection.exec_params(
        PrivateSchemaSql::insert_schema_snapshot(),
        {std::to_string(descriptor.id), common::serialize_fields(spec.fields),
         common::serialize_indexes(spec.indexes)},
        {PGRES_COMMAND_OK}
    );
    return descriptor;
}

void update_collection(
    Connection& connection,
    const CollectionSpec& spec
)
{
    connection.exec_params(
        PrivateSchemaSql::update_collection(),
        {std::to_string(spec.schema_version), spec.key_field, spec.logical_name}, {PGRES_COMMAND_OK}
    );
    connection.exec_params(
        PrivateSchemaSql::update_schema_snapshot(),
        {common::serialize_fields(spec.fields), common::serialize_indexes(spec.indexes),
         spec.logical_name},
        {PGRES_COMMAND_OK}
    );
}

std::vector<IndexSpec> load_collection_indexes(
    Connection& connection,
    CollectionId collection
)
{
    auto result = connection.exec_params(
        PrivateSchemaSql::select_collection_indexes_by_id(), {std::to_string(collection)},
        {PGRES_TUPLES_OK}
    );
    if (result.rows() == 0)
    {
        throw BackendError("postgres collection not found");
    }
    return common::deserialize_indexes(result.value(0, 0));
}

} // namespace mt::backends::postgres::detail
