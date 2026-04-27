#pragma once

#include "mt/backend.hpp"
#include "mt/errors.hpp"

#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

// -----------------------------------------------------------------------------
// mt/memory_backend.hpp
//
// Small in-memory backend for tests and local development.
//
// This is intentionally backend-compatible with mt/core.hpp and has no test-row
// knowledge. Tests can include this file instead of embedding a backend.
// -----------------------------------------------------------------------------

namespace mt::memory
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

    void rollback_backend_transaction() noexcept override
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
        auto result = list_snapshot(
            collection, ListOptions{.limit = query.limit, .after_key = query.after_key}, version
        );

        auto prefix = key_prefix_from(query);
        if (!prefix)
        {
            return result;
        }

        QueryResultEnvelope filtered;
        for (const auto& row : result.rows)
        {
            if (row.key.rfind(*prefix, 0) == 0)
            {
                filtered.rows.push_back(row);
            }
        }
        return filtered;
    }

    QueryMetadataResult query_current_metadata(
        CollectionId collection,
        const QuerySpec& query
    ) override
    {
        auto result = list_current_metadata(
            collection, ListOptions{.limit = query.limit, .after_key = query.after_key}
        );

        auto prefix = key_prefix_from(query);
        if (!prefix)
        {
            return result;
        }

        QueryMetadataResult filtered;
        for (const auto& row : result.rows)
        {
            if (row.key.rfind(*prefix, 0) == 0)
            {
                filtered.rows.push_back(row);
            }
        }
        return filtered;
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

        c.current[write.key] = MemoryVersion{
            .version = commit_version,
            .deleted = write.kind == WriteKind::Delete,
            .value = write.value,
            .hash = write.value_hash,
            .key = write.key
        };
    }

  private:
    static std::optional<std::string> key_prefix_from(const QuerySpec& query)
    {
        for (const auto& predicate : query.predicates)
        {
            if (predicate.op == QueryOp::KeyPrefix)
            {
                return predicate.text;
            }
        }
        return std::nullopt;
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

    std::unique_ptr<IBackendSession> open_session() override
    {
        return std::make_unique<MemorySession>(state_);
    }

    void bootstrap(const BootstrapSpec&) override {}

    CollectionDescriptor ensure_collection(const CollectionSpec& spec) override
    {
        std::lock_guard lock(state_->mutex);

        auto existing = state_->descriptors_by_name.find(spec.logical_name);
        if (existing != state_->descriptors_by_name.end())
        {
            return existing->second;
        }

        CollectionDescriptor descriptor{
            .id = state_->next_collection_id++,
            .logical_name = spec.logical_name,
            .schema_version = spec.schema_version
        };

        MemoryCollection collection;
        collection.descriptor = descriptor;

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

} // namespace mt::memory
