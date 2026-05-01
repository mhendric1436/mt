#pragma once

#include "sqlite_detail.hpp"

#include "mt/types.hpp"

namespace mt::backends::sqlite
{

void check_unique_constraints(
    detail::Connection& connection,
    CollectionId collection,
    const WriteEnvelope& write
);

} // namespace mt::backends::sqlite
