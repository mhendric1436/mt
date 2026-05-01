#pragma once

#include "mt/backends/memory/state.hpp"

#include "mt/errors.hpp"
#include "mt/schema.hpp"

#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

// -----------------------------------------------------------------------------
// mt/backends/memory/backend_helpers.hpp
//
// Schema and descriptor helpers for the header-only memory backend.
// -----------------------------------------------------------------------------

namespace mt::backends::memory
{

inline std::optional<CollectionSpec> memory_schema_snapshot(
    MemoryState& state,
    std::string_view logical_name
)
{
    std::lock_guard lock(state.mutex);
    auto descriptor_it = state.descriptors_by_name.find(std::string(logical_name));
    if (descriptor_it == state.descriptors_by_name.end())
    {
        return std::nullopt;
    }

    auto collection_it = state.collections.find(descriptor_it->second.id);
    if (collection_it == state.collections.end())
    {
        return std::nullopt;
    }

    return collection_it->second.schema;
}

inline void validate_memory_collection_schema_update(
    const CollectionSpec& existing,
    const CollectionSpec& requested
)
{
    auto diff = diff_schemas(existing, requested);
    if (diff.is_compatible())
    {
        return;
    }

    const auto& change = diff.incompatible_changes.front();
    throw BackendError(
        "incompatible schema change for collection '" + requested.logical_name + "' at " +
        change.path + ": " + change.message
    );
}

inline CollectionDescriptor update_memory_collection(
    MemoryState& state,
    std::map<
        std::string,
        CollectionDescriptor>::iterator descriptor_it,
    const CollectionSpec& spec
)
{
    auto collection_it = state.collections.find(descriptor_it->second.id);
    if (collection_it == state.collections.end())
    {
        throw BackendError("memory backend collection metadata is inconsistent");
    }

    validate_memory_collection_schema_update(collection_it->second.schema, spec);

    auto projected_collection = collection_it->second;
    auto projected_descriptor = descriptor_it->second;

    projected_collection.schema = spec;
    projected_collection.indexes = spec.indexes;
    projected_collection.descriptor.schema_version = spec.schema_version;
    projected_descriptor.schema_version = spec.schema_version;

    using std::swap;
    swap(collection_it->second, projected_collection);
    swap(descriptor_it->second, projected_descriptor);
    return descriptor_it->second;
}

inline CollectionDescriptor create_memory_collection(
    MemoryState& state,
    const CollectionSpec& spec
)
{
    CollectionDescriptor descriptor{
        .id = state.next_collection_id++,
        .logical_name = spec.logical_name,
        .schema_version = spec.schema_version
    };

    MemoryCollection collection;
    collection.descriptor = descriptor;
    collection.schema = spec;
    collection.indexes = spec.indexes;

    state.descriptors_by_name[spec.logical_name] = descriptor;
    state.collections[descriptor.id] = std::move(collection);

    return descriptor;
}

inline CollectionDescriptor ensure_memory_collection(
    MemoryState& state,
    const CollectionSpec& spec
)
{
    std::lock_guard lock(state.mutex);

    if (!spec.migrations.empty())
    {
        throw BackendError("memory backend does not support collection migrations");
    }

    auto existing = state.descriptors_by_name.find(spec.logical_name);
    if (existing != state.descriptors_by_name.end())
    {
        return update_memory_collection(state, existing, spec);
    }

    return create_memory_collection(state, spec);
}

inline CollectionDescriptor get_memory_collection(
    MemoryState& state,
    std::string_view logical_name
)
{
    std::lock_guard lock(state.mutex);
    return state.descriptors_by_name.at(std::string(logical_name));
}

} // namespace mt::backends::memory
