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

struct QueryCapabilities
{
    bool key_prefix = false;
    bool json_equals = false;
    bool json_contains = false;
    bool order_by_key = true;
    bool custom_ordering = false;
};

struct SchemaCapabilities
{
    bool json_indexes = false;
    bool unique_indexes = false;
    bool migrations = false;
};

struct BackendCapabilities
{
    QueryCapabilities query;
    SchemaCapabilities schema;
};

class IBackendSession
{
  public:
    virtual ~IBackendSession() = default;

    // Starts the backend transaction that bounds all session operations.
    virtual void begin_backend_transaction() = 0;

    // Makes all staged mt mutations visible atomically.
    virtual void commit_backend_transaction() = 0;

    // Aborts/closes the backend transaction and releases resources. This is
    // cleanup, not logical undo of committed mt changes.
    virtual void rollback_backend_transaction() noexcept = 0;

    // Returns the latest committed version for a new snapshot.
    virtual Version read_clock() = 0;

    // Serializes commits and returns the current committed version.
    virtual Version lock_clock_and_read() = 0;

    // Advances the commit clock. The caller must own the clock lock.
    virtual Version increment_clock_and_return() = 0;

    // Returns a backend-owned opaque transaction ID.
    virtual TxId create_transaction_id() = 0;

    // Records active transaction metadata for backend cleanup/retention.
    virtual void register_active_transaction(
        TxId tx_id,
        Version start_version
    ) = 0;

    // Removes active transaction metadata. Missing IDs should be tolerated.
    virtual void unregister_active_transaction(TxId tx_id) noexcept = 0;

    // Returns the latest visible document version at or before version.
    virtual std::optional<DocumentEnvelope> read_snapshot(
        CollectionId collection,
        std::string_view key,
        Version version
    ) = 0;

    // Returns latest committed metadata, including tombstones when present.
    virtual std::optional<DocumentMetadata> read_current_metadata(
        CollectionId collection,
        std::string_view key
    ) = 0;

    // Runs a snapshot query using the backend's advertised query semantics.
    virtual QueryResultEnvelope query_snapshot(
        CollectionId collection,
        const QuerySpec& query,
        Version version
    ) = 0;

    // Runs the same query against latest metadata for conflict validation.
    virtual QueryMetadataResult query_current_metadata(
        CollectionId collection,
        const QuerySpec& query
    ) = 0;

    // Lists snapshot documents using stable key pagination when supported.
    virtual QueryResultEnvelope list_snapshot(
        CollectionId collection,
        const ListOptions& options,
        Version version
    ) = 0;

    // Lists latest metadata using the same list semantics as list_snapshot().
    virtual QueryMetadataResult list_current_metadata(
        CollectionId collection,
        const ListOptions& options
    ) = 0;

    // Stages a committed document version or tombstone in history.
    virtual void insert_history(
        CollectionId collection,
        const WriteEnvelope& write,
        Version commit_version
    ) = 0;

    // Stages latest current metadata/state for the same commit version.
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

    // Must truthfully describe query and schema features implemented by this backend.
    virtual BackendCapabilities capabilities() const = 0;

    // Opens a session for one logical backend transaction.
    virtual std::unique_ptr<IBackendSession> open_session() = 0;

    // Initializes backend metadata storage. Must be safe to call more than once.
    virtual void bootstrap(const BootstrapSpec& spec) = 0;

    // Creates or returns a collection descriptor matching the requested spec.
    virtual CollectionDescriptor ensure_collection(const CollectionSpec& spec) = 0;

    // Returns an existing collection descriptor by logical name.
    virtual CollectionDescriptor get_collection(std::string_view logical_name) = 0;
};

} // namespace mt
