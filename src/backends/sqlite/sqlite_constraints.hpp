#pragma once

#include "sqlite_detail.hpp"

#include "mt/collection.hpp"
#include "mt/types.hpp"

namespace mt::backends::sqlite
{

void check_unique_constraints(
    detail::Connection& connection,
    CollectionId collection,
    const WriteEnvelope& write
);

void maintain_unique_index_values(
    detail::Connection& connection,
    CollectionId collection,
    const WriteEnvelope& write
);

void validate_sqlite_index_update(
    const CollectionSpec& existing,
    const CollectionSpec& requested
);

void rebuild_added_unique_indexes(
    detail::Connection& connection,
    CollectionId collection,
    const CollectionSpec& existing,
    const CollectionSpec& requested
);

} // namespace mt::backends::sqlite
