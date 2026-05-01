#pragma once

#include "mt/backends/memory/backend_helpers.hpp"
#include "mt/backends/memory/session.hpp"
#include "mt/backends/memory/state.hpp"

#include "mt/backend.hpp"

#include <memory>
#include <optional>
#include <string_view>

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
        return memory_schema_snapshot(*state_, logical_name);
    }

    CollectionDescriptor ensure_collection(const CollectionSpec& spec) override
    {
        return ensure_memory_collection(*state_, spec);
    }

    CollectionDescriptor get_collection(std::string_view logical_name) override
    {
        return get_memory_collection(*state_, logical_name);
    }

  private:
    std::shared_ptr<MemoryState> state_;
};

} // namespace mt::backends::memory
