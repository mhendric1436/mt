#include "mt/backends/sqlite.hpp"

#include "sqlite_detail.hpp"
#include "sqlite_schema.hpp"
#include "sqlite_session.hpp"
#include "sqlite_state.hpp"

#include "mt/errors.hpp"
#include "mt/schema.hpp"

#include <memory>
#include <string>
#include <string_view>
#include <utility>

// -----------------------------------------------------------------------------
// SQLite backend implementation unit.
//
// The public header remains dependency-free. SQLite client details stay in this
// optional implementation unit and private helpers.
// -----------------------------------------------------------------------------

namespace mt::backends::sqlite
{

SqliteBackend::SqliteBackend()
    : SqliteBackend(std::string(detail::StoragePath::memory()))
{
}

SqliteBackend::SqliteBackend(std::string path)
    : state_(std::make_shared<SqliteBackendState>(std::move(path)))
{
}

BackendCapabilities SqliteBackend::capabilities() const
{
    auto capabilities = BackendCapabilities{};
    capabilities.query.key_prefix = true;
    capabilities.query.json_equals = true;
    capabilities.schema.json_indexes = true;
    capabilities.schema.unique_indexes = true;
    return capabilities;
}

std::unique_ptr<IBackendSession> SqliteBackend::open_session()
{
    return std::make_unique<SqliteSession>(state_);
}

void SqliteBackend::bootstrap(const BootstrapSpec& spec)
{
    if (state_->path == detail::StoragePath::memory())
    {
        auto connection = detail::Connection::open(state_->path);
        bootstrap_schema(connection, spec);
        return;
    }

    ensure_bootstrapped(state_, spec);
}

CollectionDescriptor SqliteBackend::ensure_collection(const CollectionSpec& spec)
{
    auto connection = open_bootstrapped_connection(state_);
    connection.execute(detail::PrivateSchemaSql::begin_immediate());
    try
    {
        auto stored = load_collection_spec(connection, spec.logical_name);
        if (!stored)
        {
            insert_collection(connection, spec);
            auto descriptor = load_collection_descriptor(connection, spec.logical_name);
            connection.execute(detail::PrivateSchemaSql::commit());
            return descriptor;
        }

        auto diff = diff_schemas(*stored, spec);
        if (!diff.is_compatible())
        {
            const auto& change = diff.incompatible_changes.front();
            throw BackendError(
                "incompatible schema change for collection '" + spec.logical_name + "' at " +
                change.path + ": " + change.message
            );
        }

        update_collection(connection, spec);
        auto descriptor = load_collection_descriptor(connection, spec.logical_name);
        connection.execute(detail::PrivateSchemaSql::commit());
        return descriptor;
    }
    catch (...)
    {
        sqlite3_exec(
            connection.get(), detail::PrivateSchemaSql::rollback().c_str(), nullptr, nullptr,
            nullptr
        );
        throw;
    }
}

CollectionDescriptor SqliteBackend::get_collection(std::string_view logical_name)
{
    auto connection = open_bootstrapped_connection(state_);
    return load_collection_descriptor(connection, logical_name);
}

} // namespace mt::backends::sqlite
