#pragma once

#include "mt/backend.hpp"
#include "mt/errors.hpp"

#include <memory>
#include <string_view>

// -----------------------------------------------------------------------------
// mt/backends/sqlite.hpp
//
// Dependency-free skeleton for a future optional SQLite backend.
// -----------------------------------------------------------------------------

namespace mt::backends::sqlite
{

class SqliteBackend final : public IDatabaseBackend
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
        throw BackendError("sqlite backend skeleton is not implemented");
    }

    void bootstrap(const BootstrapSpec&) override
    {
        throw BackendError("sqlite backend skeleton is not implemented");
    }

    CollectionDescriptor ensure_collection(const CollectionSpec&) override
    {
        throw BackendError("sqlite backend skeleton is not implemented");
    }

    CollectionDescriptor get_collection(std::string_view) override
    {
        throw BackendError("sqlite backend skeleton is not implemented");
    }
};

} // namespace mt::backends::sqlite
