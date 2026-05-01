#pragma once

#include "sqlite_detail.hpp"

#include "mt/collection.hpp"
#include "mt/query.hpp"

#include <optional>
#include <string_view>
#include <vector>

namespace mt::backends::sqlite
{

std::optional<CollectionSpec> load_collection_spec(
    detail::Connection& connection,
    std::string_view logical_name
);

CollectionDescriptor load_collection_descriptor(
    detail::Connection& connection,
    std::string_view logical_name
);

void insert_collection(
    detail::Connection& connection,
    const CollectionSpec& spec
);

void update_collection(
    detail::Connection& connection,
    const CollectionSpec& spec
);

std::vector<IndexSpec> load_collection_indexes(
    detail::Connection& connection,
    CollectionId collection
);

} // namespace mt::backends::sqlite
