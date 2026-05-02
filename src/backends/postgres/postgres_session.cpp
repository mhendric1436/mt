#include "postgres_session.hpp"

#include "mt/errors.hpp"

#include "postgres_schema.hpp"

#include <cstdint>
#include <string>
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
    if (in_backend_tx_)
    {
        throw BackendError("postgres backend transaction is already open");
    }

    connection_ = detail::Connection::open(state_->dsn);
    connection_->exec_command(detail::PrivateSchemaSql::begin_transaction());
    in_backend_tx_ = true;
}

void PostgresSession::commit_backend_transaction()
{
    require_backend_tx();
    connection_->exec_command(detail::PrivateSchemaSql::commit());
    in_backend_tx_ = false;
    clock_locked_ = false;
    connection_.reset();
}

void PostgresSession::abort_backend_transaction() noexcept
{
    try
    {
        if (connection_ && in_backend_tx_)
        {
            connection_->exec_command(detail::PrivateSchemaSql::rollback());
        }
    }
    catch (...)
    {
    }

    in_backend_tx_ = false;
    clock_locked_ = false;
    connection_.reset();
}

Version PostgresSession::read_clock()
{
    require_backend_tx();
    return read_clock_row(detail::PrivateSchemaSql::select_clock_version());
}

Version PostgresSession::lock_clock_and_read()
{
    require_backend_tx();
    if (clock_locked_)
    {
        throw BackendError("postgres clock is already locked by this session");
    }

    auto version = read_clock_row(detail::PrivateSchemaSql::lock_clock_version());
    clock_locked_ = true;
    return version;
}

Version PostgresSession::increment_clock_and_return()
{
    require_backend_tx();
    if (!clock_locked_)
    {
        throw BackendError("clock must be locked before increment");
    }

    return read_clock_row(detail::PrivateSchemaSql::increment_clock_version_returning());
}

TxId PostgresSession::create_transaction_id()
{
    require_backend_tx();

    auto result =
        connection_->exec_query(detail::PrivateSchemaSql::increment_next_tx_id_returning());
    if (result.rows() != 1)
    {
        throw BackendError("postgres clock row is missing");
    }

    return "postgres:" + std::string(result.value(0, 0));
}

void PostgresSession::register_active_transaction(
    TxId tx_id,
    Version start_version
)
{
    require_backend_tx();

    connection_->exec_params(
        detail::PrivateSchemaSql::insert_or_replace_active_transaction(),
        {std::move(tx_id), std::to_string(start_version)}, {PGRES_COMMAND_OK}
    );
}

void PostgresSession::unregister_active_transaction(TxId tx_id) noexcept
{
    try
    {
        if (!connection_ || !in_backend_tx_)
        {
            return;
        }

        connection_->exec_params(
            detail::PrivateSchemaSql::delete_active_transaction(), {std::move(tx_id)},
            {PGRES_COMMAND_OK}
        );
    }
    catch (...)
    {
    }
}

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

void PostgresSession::require_backend_tx() const
{
    if (!connection_ || !in_backend_tx_)
    {
        throw BackendError("postgres backend transaction is not open");
    }
}

Version PostgresSession::read_clock_row(std::string_view sql)
{
    auto result = connection_->exec_query(sql);
    if (result.rows() != 1)
    {
        throw BackendError("postgres clock row is missing");
    }

    return static_cast<Version>(std::stoull(std::string(result.value(0, 0))));
}

} // namespace mt::backends::postgres
