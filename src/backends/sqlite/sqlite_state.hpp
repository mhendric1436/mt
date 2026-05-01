#pragma once

#include "sqlite_detail.hpp"

#include "mt/backend.hpp"
#include "mt/backends/sqlite.hpp"

#include <atomic>
#include <memory>
#include <mutex>
#include <string>

namespace mt::backends::sqlite
{

struct SqliteBackendState
{
    explicit SqliteBackendState(std::string sqlite_path);

    std::string path;
    std::mutex bootstrap_mutex;
    std::atomic_bool bootstrapped = false;
};

void bootstrap_schema(
    detail::Connection& connection,
    const BootstrapSpec& spec
);

void ensure_bootstrapped(
    const std::shared_ptr<SqliteBackendState>& state,
    const BootstrapSpec& spec = BootstrapSpec{}
);

detail::Connection open_bootstrapped_connection(const std::shared_ptr<SqliteBackendState>& state);

} // namespace mt::backends::sqlite
