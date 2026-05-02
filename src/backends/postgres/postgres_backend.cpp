#include "mt/backends/postgres.hpp"

#include "postgres_connection.hpp"
#include "postgres_session.hpp"
#include "postgres_state.hpp"

#include "mt/errors.hpp"

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
    capabilities.query.order_by_key = false;
    return capabilities;
}

std::unique_ptr<IBackendSession> PostgresBackend::open_session()
{
    return std::make_unique<PostgresSession>(state_);
}

void PostgresBackend::bootstrap(const BootstrapSpec&)
{
    std::lock_guard lock(state_->bootstrap_mutex);
    if (state_->bootstrapped)
    {
        return;
    }

    auto connection = detail::Connection::open(state_->dsn);
    connection.exec_query("SELECT 1");
    state_->bootstrapped = true;
}

CollectionDescriptor PostgresBackend::ensure_collection(const CollectionSpec&)
{
    throw BackendError("postgres backend collection metadata is not implemented");
}

CollectionDescriptor PostgresBackend::get_collection(std::string_view)
{
    throw BackendError("postgres backend collection metadata is not implemented");
}

} // namespace mt::backends::postgres
