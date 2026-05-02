#pragma once

#include <atomic>
#include <mutex>
#include <string>

namespace mt::backends::postgres
{

struct PostgresBackendState
{
    explicit PostgresBackendState(std::string postgres_dsn);

    std::string dsn;
    std::mutex bootstrap_mutex;
    std::atomic_bool bootstrapped = false;
};

} // namespace mt::backends::postgres
