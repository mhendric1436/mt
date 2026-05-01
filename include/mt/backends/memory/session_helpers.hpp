#pragma once

#include "mt/backends/memory/state.hpp"

#include "mt/errors.hpp"
#include "mt/query.hpp"

#include <string>
#include <vector>

// -----------------------------------------------------------------------------
// mt/backends/memory/session_helpers.hpp
//
// Query and constraint helpers shared by the header-only memory backend session.
// -----------------------------------------------------------------------------

namespace mt::backends::memory
{

inline const MemoryVersion* best_visible_version(
    const std::vector<MemoryVersion>& versions,
    Version version
)
{
    const MemoryVersion* best = nullptr;
    for (const auto& candidate : versions)
    {
        if (candidate.version <= version)
        {
            if (!best || candidate.version > best->version)
            {
                best = &candidate;
            }
        }
    }
    return best;
}

inline void validate_supported_query(const QuerySpec& query)
{
    if (!query.order_by_key)
    {
        throw BackendError("memory backend only supports key ordering");
    }

    for (const auto& predicate : query.predicates)
    {
        if (predicate.op == QueryOp::JsonContains)
        {
            throw BackendError("memory backend does not support JSON contains predicates");
        }
    }
}

inline bool matches_memory_query(
    const std::string& key,
    const Json& value,
    const QuerySpec& query
)
{
    return mt::matches_query(key, value, query);
}

inline void check_unique_constraints(
    const MemoryCollection& collection,
    const WriteEnvelope& write
)
{
    if (write.kind == WriteKind::Delete)
    {
        return;
    }

    for (const auto& index : collection.indexes)
    {
        if (!index.unique)
        {
            continue;
        }

        auto write_value = mt::json_path_value(write.value, index.json_path);
        if (!write_value)
        {
            continue;
        }

        for (const auto& [key, current] : collection.current)
        {
            if (key == write.key || current.deleted)
            {
                continue;
            }

            auto current_value = mt::json_path_value(current.value, index.json_path);
            if (current_value && *current_value == *write_value)
            {
                throw BackendError("memory backend unique index constraint violation");
            }
        }
    }
}

} // namespace mt::backends::memory
