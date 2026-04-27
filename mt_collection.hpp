#pragma once

#include "mt_json.hpp"
#include "mt_query.hpp"

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

// -----------------------------------------------------------------------------
// mt_collection.hpp
//
// Collection descriptors and migration specs.
// -----------------------------------------------------------------------------

namespace mt
{

using CollectionId = std::uint64_t;
using Version = std::uint64_t;
using TxId = std::string;

struct Migration
{
    int from_version = 0;
    int to_version = 0;
    std::function<void(Json&)> transform;
};

struct CollectionSpec
{
    std::string logical_name;
    std::vector<IndexSpec> indexes;
    int schema_version = 1;
    std::vector<Migration> migrations;
};

struct CollectionDescriptor
{
    CollectionId id = 0;
    std::string logical_name;
    int schema_version = 1;
};

struct BootstrapSpec
{
    int metadata_schema_version = 1;
};

} // namespace mt
