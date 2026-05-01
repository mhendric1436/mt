#pragma once

#include "mt/backend/database_backend.hpp"
#include "mt/errors.hpp"

#include <memory>
#include <string_view>

// -----------------------------------------------------------------------------
// mt/backends/postgres.hpp
//
// Dependency-free skeleton for a future optional PostgreSQL backend.
// -----------------------------------------------------------------------------

namespace mt::backends::postgres
{

class PostgresBackend final : public IDatabaseBackend
{
  public:
    BackendCapabilities capabilities() const override
    {
        auto capabilities = BackendCapabilities{};
        capabilities.query.order_by_key = false;
        return capabilities;
    }

    std::unique_ptr<IBackendSession> open_session() override
    {
        throw BackendError("postgres backend skeleton is not implemented");
    }

    void bootstrap(const BootstrapSpec&) override
    {
        throw BackendError("postgres backend skeleton is not implemented");
    }

    CollectionDescriptor ensure_collection(const CollectionSpec&) override
    {
        throw BackendError("postgres backend skeleton is not implemented");
    }

    CollectionDescriptor get_collection(std::string_view) override
    {
        throw BackendError("postgres backend skeleton is not implemented");
    }
};

} // namespace mt::backends::postgres
