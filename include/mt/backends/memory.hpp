#pragma once

#include "mt/backends/memory/state.hpp"

#include "mt/backend.hpp"
#include "mt/errors.hpp"
#include "mt/schema.hpp"

#include <memory>
#include <optional>
#include <stdexcept>
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

class MemorySession final : public IBackendSession
{
  public:
    explicit MemorySession(std::shared_ptr<MemoryState> state)
        : state_(std::move(state))
    {
    }

    void begin_backend_transaction() override
    {
        in_backend_tx_ = true;
    }

    void commit_backend_transaction() override
    {
        require_backend_tx();
        release_clock_if_locked();
        in_backend_tx_ = false;
    }

    void abort_backend_transaction() noexcept override
    {
        release_clock_if_locked();
        in_backend_tx_ = false;
    }

    Version read_clock() override
    {
        std::lock_guard lock(state_->mutex);
        return state_->clock;
    }

    Version lock_clock_and_read() override
    {
        std::lock_guard lock(state_->mutex);
        if (state_->clock_locked)
        {
            throw BackendError("memory clock already locked");
        }
        state_->clock_locked = true;
        owns_clock_lock_ = true;
        return state_->clock;
    }

    Version increment_clock_and_return() override
    {
        std::lock_guard lock(state_->mutex);
        if (!owns_clock_lock_)
        {
            throw BackendError("clock must be locked before increment");
        }
        return ++state_->clock;
    }

    TxId create_transaction_id() override
    {
        std::lock_guard lock(state_->mutex);
        return "memory:" + std::to_string(state_->next_transaction_id++);
    }

    void register_active_transaction(
        TxId tx_id,
        Version
    ) override
    {
        std::lock_guard lock(state_->mutex);
        active_tx_id_ = tx_id;
        state_->active_transactions.insert(std::move(tx_id));
    }

    void unregister_active_transaction(TxId tx_id) noexcept override
    {
        std::lock_guard lock(state_->mutex);
        state_->active_transactions.erase(tx_id);
    }

    std::optional<DocumentEnvelope> read_snapshot(
        CollectionId collection,
        std::string_view key,
        Version version
    ) override
    {
        std::lock_guard lock(state_->mutex);
        const auto& c = collection_ref(collection);

        auto it = c.history.find(std::string(key));
        if (it == c.history.end())
        {
            return std::nullopt;
        }

        const MemoryVersion* best = nullptr;
        for (const auto& candidate : it->second)
        {
            if (candidate.version <= version)
            {
                if (!best || candidate.version > best->version)
                {
                    best = &candidate;
                }
            }
        }

        if (!best)
        {
            return std::nullopt;
        }

        return DocumentEnvelope{
            .collection = collection,
            .key = best->key,
            .version = best->version,
            .deleted = best->deleted,
            .value_hash = best->hash,
            .value = best->value
        };
    }

    std::optional<DocumentMetadata> read_current_metadata(
        CollectionId collection,
        std::string_view key
    ) override
    {
        std::lock_guard lock(state_->mutex);
        const auto& c = collection_ref(collection);

        auto it = c.current.find(std::string(key));
        if (it == c.current.end())
        {
            return std::nullopt;
        }

        return DocumentMetadata{
            .collection = collection,
            .key = it->first,
            .version = it->second.version,
            .deleted = it->second.deleted,
            .value_hash = it->second.hash
        };
    }

    QueryResultEnvelope query_snapshot(
        CollectionId collection,
        const QuerySpec& query,
        Version version
    ) override
    {
        validate_supported_query(query);
        std::lock_guard lock(state_->mutex);
        const auto& c = collection_ref(collection);

        QueryResultEnvelope result;
        for (const auto& [key, versions] : c.history)
        {
            if (query.after_key && key <= *query.after_key)
            {
                continue;
            }

            const auto* best = best_visible_version(versions, version);
            if (!best || best->deleted)
            {
                continue;
            }
            if (!matches_query(key, best->value, query))
            {
                continue;
            }

            result.rows.push_back(
                DocumentEnvelope{
                    .collection = collection,
                    .key = key,
                    .version = best->version,
                    .deleted = false,
                    .value_hash = best->hash,
                    .value = best->value
                }
            );

            if (query.limit && result.rows.size() >= *query.limit)
            {
                break;
            }
        }
        return result;
    }

    QueryMetadataResult query_current_metadata(
        CollectionId collection,
        const QuerySpec& query
    ) override
    {
        validate_supported_query(query);
        std::lock_guard lock(state_->mutex);
        const auto& c = collection_ref(collection);

        QueryMetadataResult result;
        for (const auto& [key, current] : c.current)
        {
            if (query.after_key && key <= *query.after_key)
            {
                continue;
            }
            if (current.deleted)
            {
                continue;
            }
            if (!matches_query(key, current.value, query))
            {
                continue;
            }

            result.rows.push_back(
                DocumentMetadata{
                    .collection = collection,
                    .key = key,
                    .version = current.version,
                    .deleted = current.deleted,
                    .value_hash = current.hash
                }
            );

            if (query.limit && result.rows.size() >= *query.limit)
            {
                break;
            }
        }
        return result;
    }

    QueryResultEnvelope list_snapshot(
        CollectionId collection,
        const ListOptions& options,
        Version version
    ) override
    {
        std::lock_guard lock(state_->mutex);
        const auto& c = collection_ref(collection);

        QueryResultEnvelope result;
        for (const auto& [key, versions] : c.history)
        {
            if (options.after_key && key <= *options.after_key)
            {
                continue;
            }

            const auto* best = best_visible_version(versions, version);
            if (!best || best->deleted)
            {
                continue;
            }

            result.rows.push_back(
                DocumentEnvelope{
                    .collection = collection,
                    .key = key,
                    .version = best->version,
                    .deleted = false,
                    .value_hash = best->hash,
                    .value = best->value
                }
            );

            if (options.limit && result.rows.size() >= *options.limit)
            {
                break;
            }
        }

        return result;
    }

    QueryMetadataResult list_current_metadata(
        CollectionId collection,
        const ListOptions& options
    ) override
    {
        std::lock_guard lock(state_->mutex);
        const auto& c = collection_ref(collection);

        QueryMetadataResult result;
        for (const auto& [key, current] : c.current)
        {
            if (options.after_key && key <= *options.after_key)
            {
                continue;
            }
            if (current.deleted)
            {
                continue;
            }

            result.rows.push_back(
                DocumentMetadata{
                    .collection = collection,
                    .key = key,
                    .version = current.version,
                    .deleted = current.deleted,
                    .value_hash = current.hash
                }
            );

            if (options.limit && result.rows.size() >= *options.limit)
            {
                break;
            }
        }
        return result;
    }

    void insert_history(
        CollectionId collection,
        const WriteEnvelope& write,
        Version commit_version
    ) override
    {
        std::lock_guard lock(state_->mutex);
        auto& c = collection_mut(collection);
        check_unique_constraints(c, write);

        MemoryVersion version{
            .version = commit_version,
            .deleted = write.kind == WriteKind::Delete,
            .value = write.value,
            .hash = write.value_hash,
            .key = write.key
        };

        c.history[write.key].push_back(std::move(version));
    }

    void upsert_current(
        CollectionId collection,
        const WriteEnvelope& write,
        Version commit_version
    ) override
    {
        std::lock_guard lock(state_->mutex);
        auto& c = collection_mut(collection);
        check_unique_constraints(c, write);

        c.current[write.key] = MemoryVersion{
            .version = commit_version,
            .deleted = write.kind == WriteKind::Delete,
            .value = write.value,
            .hash = write.value_hash,
            .key = write.key
        };
    }

  private:
    static const MemoryVersion* best_visible_version(
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

    static void validate_supported_query(const QuerySpec& query)
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

    static bool matches_query(
        const std::string& key,
        const Json& value,
        const QuerySpec& query
    )
    {
        return mt::matches_query(key, value, query);
    }

    static void check_unique_constraints(
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

    void require_backend_tx() const
    {
        if (!in_backend_tx_)
        {
            throw BackendError("operation requires backend transaction");
        }
    }

    void release_clock_if_locked() noexcept
    {
        if (!owns_clock_lock_)
        {
            return;
        }

        std::lock_guard lock(state_->mutex);
        state_->clock_locked = false;
        owns_clock_lock_ = false;
    }

    const MemoryCollection& collection_ref(CollectionId id) const
    {
        auto it = state_->collections.find(id);
        if (it == state_->collections.end())
        {
            throw BackendError("unknown collection");
        }
        return it->second;
    }

    MemoryCollection& collection_mut(CollectionId id)
    {
        auto it = state_->collections.find(id);
        if (it == state_->collections.end())
        {
            throw BackendError("unknown collection");
        }
        return it->second;
    }

  private:
    std::shared_ptr<MemoryState> state_;
    bool in_backend_tx_ = false;
    bool owns_clock_lock_ = false;
    TxId active_tx_id_;
};

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
