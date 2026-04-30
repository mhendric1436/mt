#include "mt/backends/sqlite.hpp"

#include "sqlite_detail.hpp"

#include "mt/errors.hpp"

#include <memory>
#include <string>
#include <string_view>

// -----------------------------------------------------------------------------
// SQLite backend implementation unit.
//
// The public header remains dependency-free. SQLite client details stay in this
// optional implementation unit and private helpers.
// -----------------------------------------------------------------------------

namespace mt::backends::sqlite
{

namespace
{

void bootstrap_schema(
    detail::Connection& connection,
    const BootstrapSpec& spec
)
{
    connection.execute("PRAGMA foreign_keys = ON");

    connection.execute(
        "CREATE TABLE IF NOT EXISTS mt_meta ("
        "key TEXT PRIMARY KEY,"
        "value INTEGER NOT NULL"
        ")"
    );

    {
        detail::Statement statement{
            connection.get(),
            "INSERT INTO mt_meta (key, value) VALUES ('metadata_schema_version', ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        };
        statement.bind_int64(1, spec.metadata_schema_version);
        statement.step();
    }

    connection.execute(
        "CREATE TABLE IF NOT EXISTS mt_clock ("
        "id INTEGER PRIMARY KEY CHECK (id = 1),"
        "version INTEGER NOT NULL,"
        "next_tx_id INTEGER NOT NULL"
        ")"
    );
    connection.execute("INSERT OR IGNORE INTO mt_clock (id, version, next_tx_id) VALUES (1, 0, 1)");

    connection.execute(
        "CREATE TABLE IF NOT EXISTS mt_collections ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "logical_name TEXT NOT NULL UNIQUE,"
        "schema_version INTEGER NOT NULL,"
        "key_field TEXT NOT NULL,"
        "schema_json TEXT NOT NULL,"
        "indexes_json TEXT NOT NULL"
        ")"
    );

    connection.execute(
        "CREATE TABLE IF NOT EXISTS mt_active_transactions ("
        "tx_id TEXT PRIMARY KEY,"
        "start_version INTEGER NOT NULL"
        ")"
    );

    connection.execute(
        "CREATE TABLE IF NOT EXISTS mt_history ("
        "collection_id INTEGER NOT NULL,"
        "document_key TEXT NOT NULL,"
        "version INTEGER NOT NULL,"
        "deleted INTEGER NOT NULL,"
        "value_hash TEXT NOT NULL,"
        "value_json TEXT,"
        "PRIMARY KEY (collection_id, document_key, version),"
        "FOREIGN KEY (collection_id) REFERENCES mt_collections(id)"
        ")"
    );
    connection.execute(
        "CREATE INDEX IF NOT EXISTS mt_history_snapshot_idx "
        "ON mt_history (collection_id, document_key, version)"
    );

    connection.execute(
        "CREATE TABLE IF NOT EXISTS mt_current ("
        "collection_id INTEGER NOT NULL,"
        "document_key TEXT NOT NULL,"
        "version INTEGER NOT NULL,"
        "deleted INTEGER NOT NULL,"
        "value_hash TEXT NOT NULL,"
        "value_json TEXT,"
        "PRIMARY KEY (collection_id, document_key),"
        "FOREIGN KEY (collection_id) REFERENCES mt_collections(id)"
        ")"
    );
}

} // namespace

SqliteBackend::SqliteBackend()
    : SqliteBackend(":memory:")
{
}

SqliteBackend::SqliteBackend(std::string path)
    : path_(std::move(path))
{
}

BackendCapabilities SqliteBackend::capabilities() const
{
    auto capabilities = BackendCapabilities{};
    capabilities.query.order_by_key = false;
    return capabilities;
}

std::unique_ptr<IBackendSession> SqliteBackend::open_session()
{
    throw BackendError("sqlite backend sessions are not implemented");
}

void SqliteBackend::bootstrap(const BootstrapSpec& spec)
{
    auto connection = detail::Connection::open(path_);
    bootstrap_schema(connection, spec);
}

CollectionDescriptor SqliteBackend::ensure_collection(const CollectionSpec&)
{
    throw BackendError("sqlite collection metadata is not implemented");
}

CollectionDescriptor SqliteBackend::get_collection(std::string_view)
{
    throw BackendError("sqlite collection metadata is not implemented");
}

} // namespace mt::backends::sqlite
