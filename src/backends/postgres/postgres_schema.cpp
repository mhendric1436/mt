#include "postgres_schema.hpp"

#include "../common/schema_codec.hpp"

#include "mt/errors.hpp"
#include "mt/schema.hpp"

#include <cctype>
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

std::string sql_literal(std::string_view value)
{
    std::string escaped;
    escaped.reserve(value.size() + 2);
    escaped.push_back('\'');
    for (auto ch : value)
    {
        if (ch == '\'')
        {
            escaped += "''";
        }
        else
        {
            escaped.push_back(ch);
        }
    }
    escaped.push_back('\'');
    return escaped;
}

std::string safe_identifier_part(std::string_view value)
{
    std::string out;
    out.reserve(value.size());
    for (auto ch : value)
    {
        auto byte = static_cast<unsigned char>(ch);
        if (std::isalnum(byte) || ch == '_')
        {
            out.push_back(static_cast<char>(std::tolower(byte)));
        }
        else
        {
            out.push_back('_');
        }
    }
    if (out.empty())
    {
        return "idx";
    }
    return out;
}

const IndexSpec* find_index_by_name(
    const std::vector<IndexSpec>& indexes,
    const std::string& name
)
{
    for (const auto& index : indexes)
    {
        if (index.name == name)
        {
            return &index;
        }
    }
    return nullptr;
}

bool same_index_definition(
    const IndexSpec& left,
    const IndexSpec& right
)
{
    return left.name == right.name && left.json_path == right.json_path &&
           left.unique == right.unique;
}

} // namespace

std::string physical_user_table_name(std::string_view logical_name)
{
    return "mt_user_" + std::string(logical_name);
}

std::string quote_identifier(std::string_view identifier)
{
    std::string quoted;
    quoted.reserve(identifier.size() + 2);
    quoted.push_back('"');
    for (auto ch : identifier)
    {
        if (ch == '"')
        {
            quoted += "\"\"";
        }
        else
        {
            quoted.push_back(ch);
        }
    }
    quoted.push_back('"');
    return quoted;
}

std::string physical_current_key_index_name(std::string_view logical_name)
{
    return physical_user_table_name(logical_name) + "_current_key_idx";
}

std::string physical_json_index_name(
    std::string_view logical_name,
    const IndexSpec& index
)
{
    return physical_user_table_name(logical_name) + "_" + safe_identifier_part(index.name) +
           (index.unique ? "_uidx" : "_idx");
}

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

std::string PrivateSchemaSql::create_transaction_id_sequence()
{
    return "CREATE SEQUENCE IF NOT EXISTS mt_tx_id_seq START WITH 1";
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

std::string PrivateSchemaSql::create_user_table(std::string_view logical_name)
{
    auto table = quote_identifier(physical_user_table_name(logical_name));
    return "CREATE TABLE IF NOT EXISTS " + table +
           " ("
           "document_key TEXT NOT NULL,"
           "version BIGINT NOT NULL,"
           "is_current BOOLEAN NOT NULL,"
           "deleted BOOLEAN NOT NULL,"
           "value_hash TEXT NOT NULL,"
           "value_json JSONB,"
           "PRIMARY KEY (document_key, version)"
           ")";
}

std::string PrivateSchemaSql::create_current_key_index(std::string_view logical_name)
{
    auto index = quote_identifier(physical_current_key_index_name(logical_name));
    auto table = quote_identifier(physical_user_table_name(logical_name));
    return "CREATE UNIQUE INDEX IF NOT EXISTS " + index + " ON " + table +
           " (document_key) WHERE is_current = true";
}

std::string PrivateSchemaSql::create_json_index(
    std::string_view logical_name,
    const IndexSpec& index
)
{
    auto index_name = quote_identifier(physical_json_index_name(logical_name, index));
    auto table = quote_identifier(physical_user_table_name(logical_name));
    auto field_name = top_level_index_field_name(index);
    auto unique = index.unique ? std::string("UNIQUE ") : std::string{};
    return "CREATE " + unique + "INDEX IF NOT EXISTS " + index_name + " ON " + table +
           " ((value_json ->> " + sql_literal(field_name) +
           ")) WHERE is_current = true AND deleted = false";
}

std::string PrivateSchemaSql::count_private_tables()
{
    return "SELECT COUNT(*) "
           "FROM information_schema.tables "
           "WHERE table_schema = current_schema() "
           "AND table_name IN ("
           "'mt_metadata', 'mt_collections', 'mt_schema_snapshots', 'mt_clock', "
           "'mt_active_transactions')";
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

std::string PrivateSchemaSql::next_transaction_id()
{
    return "SELECT nextval('mt_tx_id_seq')";
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

std::string PrivateSchemaSql::select_snapshot_document(std::string_view logical_name)
{
    auto table = quote_identifier(physical_user_table_name(logical_name));
    return "SELECT version, deleted, value_hash, value_json::text "
           "FROM " +
           table +
           " WHERE document_key = $1 AND version <= $2::bigint "
           "ORDER BY version DESC LIMIT 1";
}

std::string PrivateSchemaSql::select_current_metadata(std::string_view logical_name)
{
    auto table = quote_identifier(physical_user_table_name(logical_name));
    return "SELECT version, deleted, value_hash "
           "FROM " +
           table + " WHERE document_key = $1 AND is_current = true";
}

std::string PrivateSchemaSql::select_snapshot_list(
    std::string_view logical_name,
    bool has_after_key,
    bool has_limit
)
{
    auto table = quote_identifier(physical_user_table_name(logical_name));
    auto sql = "SELECT h.document_key, h.version, h.deleted, h.value_hash, h.value_json::text "
               "FROM " +
               table +
               " h "
               "WHERE h.version = ("
               "SELECT MAX(h2.version) FROM " +
               table +
               " h2 "
               "WHERE h2.document_key = h.document_key "
               "AND h2.version <= $1::bigint"
               ") "
               "AND h.deleted = false ";
    auto next_param = 2;
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
    std::string_view logical_name,
    bool has_after_key,
    bool has_limit
)
{
    auto table = quote_identifier(physical_user_table_name(logical_name));
    auto sql = "SELECT document_key, version, deleted, value_hash "
               "FROM " +
               table + " WHERE is_current = true ";
    auto next_param = 1;
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

std::string PrivateSchemaSql::select_current_query_candidates(
    std::string_view logical_name,
    bool has_after_key
)
{
    auto table = quote_identifier(physical_user_table_name(logical_name));
    auto sql = "SELECT document_key, version, deleted, value_hash, value_json::text "
               "FROM " +
               table + " WHERE is_current = true ";
    if (has_after_key)
    {
        sql += "AND document_key > $1 ";
    }
    sql += "ORDER BY document_key";
    return sql;
}

std::string PrivateSchemaSql::select_current_query_by_index(
    std::string_view logical_name,
    std::string_view field_name,
    bool has_after_key
)
{
    auto table = quote_identifier(physical_user_table_name(logical_name));
    auto sql = "SELECT document_key, version, deleted, value_hash, value_json::text "
               "FROM " +
               table +
               " WHERE is_current = true "
               "AND deleted = false "
               "AND value_json ->> " +
               sql_literal(field_name) + " = $1 ";
    if (has_after_key)
    {
        sql += "AND document_key > $2 ";
    }
    sql += "ORDER BY document_key";
    return sql;
}

std::string PrivateSchemaSql::insert_user_row(
    std::string_view logical_name,
    bool is_current
)
{
    auto table = quote_identifier(physical_user_table_name(logical_name));
    return "INSERT INTO " + table +
           " (document_key, version, is_current, deleted, value_hash, value_json) "
           "VALUES ($1, $2::bigint, " +
           std::string(is_current ? "true" : "false") +
           ", $3::boolean, $4, CASE WHEN $3::boolean THEN NULL ELSE $5::jsonb END) "
           "ON CONFLICT (document_key, version) DO UPDATE SET "
           "is_current = EXCLUDED.is_current, "
           "deleted = EXCLUDED.deleted, "
           "value_hash = EXCLUDED.value_hash, "
           "value_json = EXCLUDED.value_json";
}

std::string PrivateSchemaSql::clear_current_row(std::string_view logical_name)
{
    auto table = quote_identifier(physical_user_table_name(logical_name));
    return "UPDATE " + table +
           " SET is_current = false "
           "WHERE document_key = $1 AND is_current = true";
}

std::string PrivateSchemaSql::select_user_row_by_key(
    std::string_view logical_name,
    bool only_current
)
{
    auto table = quote_identifier(physical_user_table_name(logical_name));
    auto sql = "SELECT version, deleted, value_hash, value_json::text, is_current "
               "FROM " +
               table + " WHERE document_key = $1";
    if (only_current)
    {
        sql += " AND is_current = true";
    }
    sql += " ORDER BY version";
    return sql;
}

std::string PrivateSchemaSql::count_user_rows(std::string_view logical_name)
{
    return "SELECT COUNT(*) FROM " + quote_identifier(physical_user_table_name(logical_name));
}

std::string PrivateSchemaSql::select_collection_spec_by_logical_name()
{
    return "SELECT c.logical_name, c.schema_version, c.key_field, s.schema_json, s.indexes_json "
           "FROM mt_collections c "
           "JOIN mt_schema_snapshots s ON s.collection_id = c.id "
           "WHERE c.logical_name = $1";
}

std::string PrivateSchemaSql::select_collection_spec_by_id()
{
    return "SELECT c.logical_name, c.schema_version, c.key_field, s.schema_json, s.indexes_json "
           "FROM mt_collections c "
           "JOIN mt_schema_snapshots s ON s.collection_id = c.id "
           "WHERE c.id = $1::bigint";
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

std::string
PrivateSchemaSql::select_current_document_with_missing_index_value(std::string_view logical_name)
{
    auto table = quote_identifier(physical_user_table_name(logical_name));
    return "SELECT document_key "
           "FROM " +
           table +
           " WHERE is_current = true "
           "AND deleted = false "
           "AND value_json ->> $1 IS NULL "
           "LIMIT 1";
}

std::string PrivateSchemaSql::count_physical_index_by_name()
{
    return "SELECT COUNT(*) FROM pg_indexes "
           "WHERE schemaname = current_schema() AND indexname = $1";
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
    connection.exec_command(PrivateSchemaSql::create_transaction_id_sequence());
    connection.exec_command(PrivateSchemaSql::insert_default_clock_row());

    connection.exec_command(PrivateSchemaSql::create_collections_table());
    connection.exec_command(PrivateSchemaSql::create_schema_snapshots_table());

    connection.exec_command(PrivateSchemaSql::create_active_transactions_table());
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

CollectionSpec load_collection_spec(
    Connection& connection,
    CollectionId collection
)
{
    auto result = connection.exec_params(
        PrivateSchemaSql::select_collection_spec_by_id(), {std::to_string(collection)},
        {PGRES_TUPLES_OK}
    );
    if (result.rows() == 0)
    {
        throw BackendError("postgres collection not found");
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
    create_user_storage(connection, spec);
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

void validate_postgres_index_update(
    const CollectionSpec& existing,
    const CollectionSpec& requested
)
{
    for (const auto& existing_index : existing.indexes)
    {
        auto requested_index = find_index_by_name(requested.indexes, existing_index.name);
        if (!requested_index)
        {
            throw BackendError("postgres backend does not support removing indexes");
        }
        if (!same_index_definition(existing_index, *requested_index))
        {
            throw BackendError("postgres backend does not support changing index definitions");
        }
    }
}

void validate_existing_values_for_unique_index(
    Connection& connection,
    const CollectionSpec& spec,
    const IndexSpec& index
)
{
    auto field_name = top_level_index_field_name(index);
    auto missing = connection.exec_params(
        PrivateSchemaSql::select_current_document_with_missing_index_value(spec.logical_name),
        {field_name}, {PGRES_TUPLES_OK}
    );
    if (missing.rows() != 0)
    {
        throw BackendError("postgres backend unique index value must not be null or missing");
    }
}

void create_user_storage(
    Connection& connection,
    const CollectionSpec& spec
)
{
    connection.exec_command(PrivateSchemaSql::create_user_table(spec.logical_name));
    connection.exec_command(PrivateSchemaSql::create_current_key_index(spec.logical_name));
    for (const auto& index : spec.indexes)
    {
        if (index.unique)
        {
            validate_existing_values_for_unique_index(connection, spec, index);
        }

        connection.exec_command(PrivateSchemaSql::create_json_index(spec.logical_name, index));
    }
}

void create_added_user_indexes(
    Connection& connection,
    const CollectionSpec& existing,
    const CollectionSpec& requested
)
{
    for (const auto& index : requested.indexes)
    {
        if (find_index_by_name(existing.indexes, index.name))
        {
            continue;
        }

        if (index.unique)
        {
            validate_existing_values_for_unique_index(connection, requested, index);
        }
        connection.exec_command(PrivateSchemaSql::create_json_index(requested.logical_name, index));
    }
}

} // namespace mt::backends::postgres::detail
