#pragma once

#include "mt/collection.hpp"
#include "mt/query.hpp"
#include "mt/types.hpp"

#include <memory>
#include <optional>
#include <string_view>

// -----------------------------------------------------------------------------
// mt/backend.hpp
//
// Backend interfaces for concrete storage engines.
// -----------------------------------------------------------------------------

namespace mt
{

class IBackendSession
{
  public:
    virtual ~IBackendSession() = default;

    virtual void begin_backend_transaction() = 0;
    virtual void commit_backend_transaction() = 0;
    virtual void rollback_backend_transaction() noexcept = 0;

    virtual Version read_clock() = 0;
    virtual Version lock_clock_and_read() = 0;
    virtual Version increment_clock_and_return() = 0;

    virtual TxId create_transaction_id() = 0;

    virtual void register_active_transaction(
        TxId tx_id,
        Version start_version
    ) = 0;
    virtual void unregister_active_transaction(TxId tx_id) noexcept = 0;

    virtual std::optional<DocumentEnvelope> read_snapshot(
        CollectionId collection,
        std::string_view key,
        Version version
    ) = 0;

    virtual std::optional<DocumentMetadata> read_current_metadata(
        CollectionId collection,
        std::string_view key
    ) = 0;

    virtual QueryResultEnvelope query_snapshot(
        CollectionId collection,
        const QuerySpec& query,
        Version version
    ) = 0;

    virtual QueryMetadataResult query_current_metadata(
        CollectionId collection,
        const QuerySpec& query
    ) = 0;

    virtual QueryResultEnvelope list_snapshot(
        CollectionId collection,
        const ListOptions& options,
        Version version
    ) = 0;

    virtual QueryMetadataResult list_current_metadata(
        CollectionId collection,
        const ListOptions& options
    ) = 0;

    virtual void insert_history(
        CollectionId collection,
        const WriteEnvelope& write,
        Version commit_version
    ) = 0;

    virtual void upsert_current(
        CollectionId collection,
        const WriteEnvelope& write,
        Version commit_version
    ) = 0;
};

class IDatabaseBackend
{
  public:
    virtual ~IDatabaseBackend() = default;

    virtual std::unique_ptr<IBackendSession> open_session() = 0;

    virtual void bootstrap(const BootstrapSpec& spec) = 0;

    virtual CollectionDescriptor ensure_collection(const CollectionSpec& spec) = 0;

    virtual CollectionDescriptor get_collection(std::string_view logical_name) = 0;
};

} // namespace mt
