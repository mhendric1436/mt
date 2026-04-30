#pragma once

#include "mt/backend.hpp"

#include <memory>
#include <string>
#include <string_view>

// -----------------------------------------------------------------------------
// mt/backends/sqlite.hpp
//
// Optional SQLite backend public interface. The implementation links against
// SQLite only through optional backend build targets.
// -----------------------------------------------------------------------------

namespace mt::backends::sqlite
{

class SqliteBackend final : public IDatabaseBackend
{
  public:
    SqliteBackend();
    explicit SqliteBackend(std::string path);

    BackendCapabilities capabilities() const override;
    std::unique_ptr<IBackendSession> open_session() override;
    void bootstrap(const BootstrapSpec& spec) override;
    CollectionDescriptor ensure_collection(const CollectionSpec& spec) override;
    CollectionDescriptor get_collection(std::string_view logical_name) override;

  private:
    std::string path_;
};

} // namespace mt::backends::sqlite
