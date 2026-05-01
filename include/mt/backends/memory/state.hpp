#pragma once

#include "mt/collection.hpp"
#include "mt/types.hpp"

#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <unordered_set>
#include <vector>

// -----------------------------------------------------------------------------
// mt/backends/memory/state.hpp
//
// Process-local state model for the header-only memory backend.
// -----------------------------------------------------------------------------

namespace mt::backends::memory
{

struct MemoryVersion
{
    Version version = 0;
    bool deleted = false;
    Json value;
    Hash hash;
    std::string key;
};

struct MemoryCollection
{
    CollectionDescriptor descriptor;
    CollectionSpec schema;
    std::vector<IndexSpec> indexes;
    std::map<std::string, MemoryVersion> current;
    std::map<std::string, std::vector<MemoryVersion>> history;
};

struct MemoryState
{
    std::mutex mutex;
    Version clock = 0;
    CollectionId next_collection_id = 1;
    std::uint64_t next_transaction_id = 1;
    std::map<std::string, CollectionDescriptor> descriptors_by_name;
    std::map<CollectionId, MemoryCollection> collections;
    std::unordered_set<TxId> active_transactions;
    bool clock_locked = false;
};

} // namespace mt::backends::memory
