#include "mt/backends/postgres.hpp"

#include "postgres_connection.hpp"
#include "postgres_schema.hpp"
#include "postgres_session.hpp"
#include "postgres_state.hpp"

#include "mt/errors.hpp"
#include "mt/schema.hpp"

#include <cstdlib>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>

namespace mt::backends::postgres
{

namespace
{
std::string default_postgres_dsn()
{
    const auto* dsn = std::getenv("MT_POSTGRES_DSN");
    if (dsn)
    {
        return dsn;
    }
    return {};
}
} // namespace

PostgresBackend::PostgresBackend()
    : PostgresBackend(default_postgres_dsn())
{
}

PostgresBackend::PostgresBackend(std::string dsn)
    : state_(std::make_shared<PostgresBackendState>(std::move(dsn)))
{
}

BackendCapabilities PostgresBackend::capabilities() const
{
    auto capabilities = BackendCapabilities{};
    capabilities.query.key_prefix = true;
    capabilities.query.json_equals = true;
    capabilities.query.order_by_key = true;
    capabilities.schema.json_indexes = true;
    capabilities.schema.unique_indexes = true;
    return capabilities;
}

std::unique_ptr<IBackendSession> PostgresBackend::open_session()
{
    return std::make_unique<PostgresSession>(state_);
}

void PostgresBackend::bootstrap(const BootstrapSpec& spec)
{
    std::lock_guard lock(state_->bootstrap_mutex);
    if (state_->bootstrapped)
    {
        return;
    }

    auto connection = detail::Connection::open(state_->dsn);
    detail::bootstrap_schema(connection, spec);
    state_->bootstrapped = true;
}

CollectionDescriptor PostgresBackend::ensure_collection(const CollectionSpec& spec)
{
    if (!spec.migrations.empty())
    {
        throw BackendError("postgres backend does not support explicit collection migrations");
    }

    bootstrap(BootstrapSpec{});

    auto connection = detail::Connection::open(state_->dsn);
    connection.exec_command("BEGIN");
    try
    {
        auto stored = detail::load_collection_spec(connection, spec.logical_name);
        if (!stored)
        {
            auto descriptor = detail::insert_collection(connection, spec);
            connection.exec_command("COMMIT");
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

        detail::update_collection(connection, spec);
        auto descriptor = detail::load_collection_descriptor(connection, spec.logical_name);
        connection.exec_command("COMMIT");
        return descriptor;
    }
    catch (...)
    {
        try
        {
            connection.exec_command("ROLLBACK");
        }
        catch (...)
        {
        }
        throw;
    }
}

CollectionDescriptor PostgresBackend::get_collection(std::string_view logical_name)
{
    bootstrap(BootstrapSpec{});
    auto connection = detail::Connection::open(state_->dsn);
    return detail::load_collection_descriptor(connection, logical_name);
}

} // namespace mt::backends::postgres
