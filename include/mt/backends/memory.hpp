#pragma once

#include "mt/backends/memory/session.hpp"
#include "mt/backends/memory/state.hpp"

#include "mt/backend.hpp"
#include "mt/errors.hpp"
#include "mt/schema.hpp"

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

// -----------------------------------------------------------------------------
// mt/backends/memory.hpp
//
// Small process-local, non-durable backend for tests, local development, and
// application-owned ephemeral use cases.
//
// This is intentionally backend-compatible with mt/core.hpp and has no test-row
// knowledge. Tests can include this file instead of embedding a backend.
// -----------------------------------------------------------------------------

namespace mt::backends::memory
{

class MemoryBackend final : public IDatabaseBackend
{
  public:
    MemoryBackend()
        : state_(std::make_shared<MemoryState>())
    {
    }

    BackendCapabilities capabilities() const override
    {
        return BackendCapabilities{
            .query =
                QueryCapabilities{
                    .key_prefix = true,
                    .json_equals = true,
                    .json_contains = false,
                    .order_by_key = true,
                    .custom_ordering = false
                },
            .schema = SchemaCapabilities{
                .json_indexes = true, .unique_indexes = true, .migrations = false
            }
        };
    }

    std::unique_ptr<IBackendSession> open_session() override
    {
        return std::make_unique<MemorySession>(state_);
    }

    void bootstrap(const BootstrapSpec&) override {}

    std::optional<CollectionSpec> schema_snapshot(std::string_view logical_name) const
    {
        std::lock_guard lock(state_->mutex);
        auto descriptor_it = state_->descriptors_by_name.find(std::string(logical_name));
        if (descriptor_it == state_->descriptors_by_name.end())
        {
            return std::nullopt;
        }

        auto collection_it = state_->collections.find(descriptor_it->second.id);
        if (collection_it == state_->collections.end())
        {
            return std::nullopt;
        }

        return collection_it->second.schema;
    }

    CollectionDescriptor ensure_collection(const CollectionSpec& spec) override
    {
        std::lock_guard lock(state_->mutex);

        if (!spec.migrations.empty())
        {
            throw BackendError("memory backend does not support collection migrations");
        }

        auto existing = state_->descriptors_by_name.find(spec.logical_name);
        if (existing != state_->descriptors_by_name.end())
        {
            auto collection_it = state_->collections.find(existing->second.id);
            if (collection_it == state_->collections.end())
            {
                throw BackendError("memory backend collection metadata is inconsistent");
            }

            auto diff = diff_schemas(collection_it->second.schema, spec);
            if (!diff.is_compatible())
            {
                const auto& change = diff.incompatible_changes.front();
                throw BackendError(
                    "incompatible schema change for collection '" + spec.logical_name + "' at " +
                    change.path + ": " + change.message
                );
            }

            collection_it->second.schema = spec;
            collection_it->second.indexes = spec.indexes;
            collection_it->second.descriptor.schema_version = spec.schema_version;
            existing->second.schema_version = spec.schema_version;
            return existing->second;
        }

        CollectionDescriptor descriptor{
            .id = state_->next_collection_id++,
            .logical_name = spec.logical_name,
            .schema_version = spec.schema_version
        };

        MemoryCollection collection;
        collection.descriptor = descriptor;
        collection.schema = spec;
        collection.indexes = spec.indexes;

        state_->descriptors_by_name[spec.logical_name] = descriptor;
        state_->collections[descriptor.id] = std::move(collection);

        return descriptor;
    }

    CollectionDescriptor get_collection(std::string_view logical_name) override
    {
        std::lock_guard lock(state_->mutex);
        return state_->descriptors_by_name.at(std::string(logical_name));
    }

  private:
    std::shared_ptr<MemoryState> state_;
};

} // namespace mt::backends::memory
