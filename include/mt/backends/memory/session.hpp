#pragma once

#include "mt/backends/memory/session_helpers.hpp"
#include "mt/backends/memory/state.hpp"

#include "mt/backend/session.hpp"
#include "mt/errors.hpp"

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

// -----------------------------------------------------------------------------
// mt/backends/memory/session.hpp
//
// Backend session implementation for the header-only memory backend.
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

        const auto* best = best_visible_version(it->second, version);
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
            if (!matches_memory_query(key, best->value, query))
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
            if (!matches_memory_query(key, current.value, query))
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

} // namespace mt::backends::memory
