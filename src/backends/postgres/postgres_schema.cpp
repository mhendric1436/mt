#include "postgres_schema.hpp"

#include <string>
#include <vector>

namespace mt::backends::postgres::detail
{

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

} // namespace mt::backends::postgres::detail
