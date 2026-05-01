#include "sqlite_state.hpp"

#include <utility>

namespace mt::backends::sqlite
{

SqliteBackendState::SqliteBackendState(std::string sqlite_path)
    : path(std::move(sqlite_path))
{
}

void bootstrap_schema(
    detail::Connection& connection,
    const BootstrapSpec& spec
)
{
    connection.execute(detail::PrivateSchemaSql::enable_foreign_keys());

    connection.execute(detail::PrivateSchemaSql::create_meta_table());

    {
        detail::Statement statement{
            connection.get(), detail::PrivateSchemaSql::upsert_metadata_schema_version()
        };
        statement.bind_int64(1, spec.metadata_schema_version);
        statement.step();
    }

    connection.execute(detail::PrivateSchemaSql::create_clock_table());
    connection.execute(detail::PrivateSchemaSql::insert_default_clock_row());

    connection.execute(detail::PrivateSchemaSql::create_collections_table());

    connection.execute(detail::PrivateSchemaSql::create_active_transactions_table());

    connection.execute(detail::PrivateSchemaSql::create_history_table());
    connection.execute(detail::PrivateSchemaSql::create_history_snapshot_index());

    connection.execute(detail::PrivateSchemaSql::create_current_table());
}

void ensure_bootstrapped(
    const std::shared_ptr<SqliteBackendState>& state,
    const BootstrapSpec& spec
)
{
    if (state->path == detail::StoragePath::memory())
    {
        return;
    }

    if (state->bootstrapped.load())
    {
        return;
    }

    auto lock = std::lock_guard<std::mutex>{state->bootstrap_mutex};
    if (state->bootstrapped.load())
    {
        return;
    }

    auto connection = detail::Connection::open(state->path);
    bootstrap_schema(connection, spec);
    state->bootstrapped.store(true);
}

detail::Connection open_bootstrapped_connection(const std::shared_ptr<SqliteBackendState>& state)
{
    if (state->path == detail::StoragePath::memory())
    {
        // SQLite in-memory databases are scoped to a single connection.
        auto connection = detail::Connection::open(state->path);
        bootstrap_schema(connection, BootstrapSpec{});
        return connection;
    }

    ensure_bootstrapped(state);
    return detail::Connection::open(state->path);
}

} // namespace mt::backends::sqlite
