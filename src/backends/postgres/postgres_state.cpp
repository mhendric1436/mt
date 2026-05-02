#include "postgres_state.hpp"

#include <utility>

namespace mt::backends::postgres
{

PostgresBackendState::PostgresBackendState(std::string postgres_dsn)
    : dsn(std::move(postgres_dsn))
{
}

} // namespace mt::backends::postgres
