#pragma once

// -----------------------------------------------------------------------------
// mt/backend/capabilities.hpp
//
// Backend feature declarations shared by table validation and backend providers.
// -----------------------------------------------------------------------------

namespace mt
{

struct QueryCapabilities
{
    bool key_prefix = false;
    bool json_equals = false;
    bool json_contains = false;
    bool order_by_key = true;
    bool custom_ordering = false;
};

struct SchemaCapabilities
{
    bool json_indexes = false;
    bool unique_indexes = false;
    bool migrations = false;
};

struct BackendCapabilities
{
    QueryCapabilities query;
    SchemaCapabilities schema;
};

} // namespace mt
