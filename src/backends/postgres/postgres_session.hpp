#pragma once

#include "postgres_state.hpp"

#include "mt/backend/session.hpp"

#include <memory>
#include <optional>
#include <string_view>

namespace mt::backends::postgres
{

class PostgresSession final : public IBackendSession
{
  public:
    explicit PostgresSession(std::shared_ptr<PostgresBackendState> state);

    void begin_backend_transaction() override;
    void commit_backend_transaction() override;
    void abort_backend_transaction() noexcept override;

    Version read_clock() override;
    Version lock_clock_and_read() override;
    Version increment_clock_and_return() override;
    TxId create_transaction_id() override;

    void register_active_transaction(
        TxId tx_id,
        Version start_version
    ) override;

    void unregister_active_transaction(TxId tx_id) noexcept override;

    std::optional<DocumentEnvelope> read_snapshot(
        CollectionId collection,
        std::string_view key,
        Version version
    ) override;

    std::optional<DocumentMetadata> read_current_metadata(
        CollectionId collection,
        std::string_view key
    ) override;

    QueryResultEnvelope query_snapshot(
        CollectionId collection,
        const QuerySpec& query,
        Version version
    ) override;

    QueryMetadataResult query_current_metadata(
        CollectionId collection,
        const QuerySpec& query
    ) override;

    QueryResultEnvelope list_snapshot(
        CollectionId collection,
        const ListOptions& options,
        Version version
    ) override;

    QueryMetadataResult list_current_metadata(
        CollectionId collection,
        const ListOptions& options
    ) override;

    void insert_history(
        CollectionId collection,
        const WriteEnvelope& write,
        Version commit_version
    ) override;

    void upsert_current(
        CollectionId collection,
        const WriteEnvelope& write,
        Version commit_version
    ) override;

  private:
    std::shared_ptr<PostgresBackendState> state_;
};

} // namespace mt::backends::postgres
