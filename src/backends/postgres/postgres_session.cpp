#include "postgres_session.hpp"

#include "mt/errors.hpp"

#include <utility>

namespace mt::backends::postgres
{

namespace
{
[[noreturn]] void throw_session_not_implemented()
{
    throw BackendError("postgres backend session is not implemented");
}
} // namespace

PostgresSession::PostgresSession(std::shared_ptr<PostgresBackendState> state)
    : state_(std::move(state))
{
}

void PostgresSession::begin_backend_transaction()
{
    throw_session_not_implemented();
}

void PostgresSession::commit_backend_transaction()
{
    throw_session_not_implemented();
}

void PostgresSession::abort_backend_transaction() noexcept {}

Version PostgresSession::read_clock()
{
    throw_session_not_implemented();
}

Version PostgresSession::lock_clock_and_read()
{
    throw_session_not_implemented();
}

Version PostgresSession::increment_clock_and_return()
{
    throw_session_not_implemented();
}

TxId PostgresSession::create_transaction_id()
{
    throw_session_not_implemented();
}

void PostgresSession::register_active_transaction(
    TxId,
    Version
)
{
    throw_session_not_implemented();
}

void PostgresSession::unregister_active_transaction(TxId) noexcept {}

std::optional<DocumentEnvelope> PostgresSession::read_snapshot(
    CollectionId,
    std::string_view,
    Version
)
{
    throw_session_not_implemented();
}

std::optional<DocumentMetadata> PostgresSession::read_current_metadata(
    CollectionId,
    std::string_view
)
{
    throw_session_not_implemented();
}

QueryResultEnvelope PostgresSession::query_snapshot(
    CollectionId,
    const QuerySpec&,
    Version
)
{
    throw_session_not_implemented();
}

QueryMetadataResult PostgresSession::query_current_metadata(
    CollectionId,
    const QuerySpec&
)
{
    throw_session_not_implemented();
}

QueryResultEnvelope PostgresSession::list_snapshot(
    CollectionId,
    const ListOptions&,
    Version
)
{
    throw_session_not_implemented();
}

QueryMetadataResult PostgresSession::list_current_metadata(
    CollectionId,
    const ListOptions&
)
{
    throw_session_not_implemented();
}

void PostgresSession::insert_history(
    CollectionId,
    const WriteEnvelope&,
    Version
)
{
    throw_session_not_implemented();
}

void PostgresSession::upsert_current(
    CollectionId,
    const WriteEnvelope&,
    Version
)
{
    throw_session_not_implemented();
}

} // namespace mt::backends::postgres
