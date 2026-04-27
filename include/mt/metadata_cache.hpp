#pragma once

#include "mt/collection.hpp"

#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

// -----------------------------------------------------------------------------
// mt/metadata_cache.hpp
//
// In-process collection descriptor cache.
// -----------------------------------------------------------------------------

namespace mt
{

class MetadataCache
{
  public:
    std::optional<CollectionDescriptor> find(std::string_view logical_name) const
    {
        auto it = by_name_.find(std::string(logical_name));
        if (it == by_name_.end())
        {
            return std::nullopt;
        }
        return it->second;
    }

    void put(CollectionDescriptor descriptor)
    {
        by_name_[descriptor.logical_name] = std::move(descriptor);
    }

  private:
    std::unordered_map<std::string, CollectionDescriptor> by_name_;
};

} // namespace mt
