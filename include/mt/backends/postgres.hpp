#pragma once

#include "mt/backend/database_backend.hpp"

#include <memory>
#include <string>
#include <string_view>

// -----------------------------------------------------------------------------
// mt/backends/postgres.hpp
//
// Optional PostgreSQL backend public interface. The implementation links against
// libpq only through optional backend build targets.
// -----------------------------------------------------------------------------

namespace mt::backends::postgres
{

struct PostgresBackendState;

class PostgresBackend final : public IDatabaseBackend
{
  public:
    PostgresBackend();
    explicit PostgresBackend(std::string dsn);

    BackendCapabilities capabilities() const override;
    std::unique_ptr<IBackendSession> open_session() override;
    void bootstrap(const BootstrapSpec& spec) override;
    CollectionDescriptor ensure_collection(const CollectionSpec& spec) override;
    CollectionDescriptor get_collection(std::string_view logical_name) override;

  private:
    std::shared_ptr<PostgresBackendState> state_;
};

} // namespace mt::backends::postgres
