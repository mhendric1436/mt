#include "sqlite_session.hpp"

#include "sqlite_constraints.hpp"
#include "sqlite_document.hpp"

#include "mt/errors.hpp"
#include "mt/query.hpp"

#include <cstdint>
#include <string>
#include <utility>

namespace mt::backends::sqlite
{

namespace
{
void validate_supported_query(const QuerySpec& query)
{
    if (!query.order_by_key)
    {
        throw BackendError("sqlite backend only supports key ordering");
    }

    for (const auto& predicate : query.predicates)
    {
        if (predicate.op == QueryOp::JsonContains)
        {
            throw BackendError("sqlite backend does not support JSON contains predicates");
        }
    }
}

} // namespace

SqliteSession::SqliteSession(std::shared_ptr<SqliteBackendState> state)
    : state_(std::move(state))
{
}

void SqliteSession::begin_backend_transaction()
{
    if (in_backend_tx_)
    {
        throw BackendError("sqlite backend transaction is already open");
    }

    connection_ = open_bootstrapped_connection(state_);
    connection_->execute(detail::PrivateSchemaSql::begin_immediate());
    in_backend_tx_ = true;
}

void SqliteSession::commit_backend_transaction()
{
    require_backend_tx();
    connection_->execute(detail::PrivateSchemaSql::commit());
    in_backend_tx_ = false;
    clock_locked_ = false;
    connection_.reset();
}

void SqliteSession::abort_backend_transaction() noexcept
{
    if (connection_ && in_backend_tx_)
    {
        sqlite3_exec(
            connection_->get(), detail::PrivateSchemaSql::rollback().c_str(), nullptr, nullptr,
            nullptr
        );
    }
    in_backend_tx_ = false;
    clock_locked_ = false;
    connection_.reset();
}

Version SqliteSession::read_clock()
{
    require_backend_tx();
    return read_clock_row();
}

Version SqliteSession::lock_clock_and_read()
{
    require_backend_tx();
    if (clock_locked_)
    {
        throw BackendError("sqlite clock is already locked by this session");
    }

    auto version = read_clock_row();
    clock_locked_ = true;
    return version;
}

Version SqliteSession::increment_clock_and_return()
{
    require_backend_tx();
    if (!clock_locked_)
    {
        throw BackendError("clock must be locked before increment");
    }

    connection_->execute(detail::PrivateSchemaSql::increment_clock_version());
    return read_clock_row();
}

TxId SqliteSession::create_transaction_id()
{
    require_backend_tx();

    auto tx_number = std::int64_t{0};
    {
        detail::Statement statement{
            connection_->get(), detail::PrivateSchemaSql::select_next_tx_id()
        };
        if (!statement.step())
        {
            throw BackendError("sqlite clock row is missing");
        }
        tx_number = statement.column_int64(0);
    }

    connection_->execute(detail::PrivateSchemaSql::increment_next_tx_id());
    return "sqlite:" + std::to_string(tx_number);
}

void SqliteSession::register_active_transaction(
    TxId tx_id,
    Version start_version
)
{
    require_backend_tx();

    detail::Statement statement{
        connection_->get(), detail::PrivateSchemaSql::insert_or_replace_active_transaction()
    };
    statement.bind_text(1, tx_id);
    statement.bind_int64(2, start_version);
    statement.step();
}

void SqliteSession::unregister_active_transaction(TxId tx_id) noexcept
{
    try
    {
        if (!connection_ || !in_backend_tx_)
        {
            return;
        }

        detail::Statement statement{
            connection_->get(), detail::PrivateSchemaSql::delete_active_transaction()
        };
        statement.bind_text(1, tx_id);
        statement.step();
    }
    catch (...)
    {
    }
}

std::optional<DocumentEnvelope> SqliteSession::read_snapshot(
    CollectionId collection,
    std::string_view key,
    Version version
)
{
    require_backend_tx();

    detail::Statement statement{
        connection_->get(), detail::PrivateSchemaSql::select_snapshot_document()
    };
    statement.bind_int64(1, collection);
    statement.bind_text(2, key);
    statement.bind_int64(3, version);
    if (!statement.step())
    {
        return std::nullopt;
    }

    auto deleted = statement.column_int64(1) != 0;
    return DocumentEnvelope{
        .collection = collection,
        .key = std::string(key),
        .version = static_cast<Version>(statement.column_int64(0)),
        .deleted = deleted,
        .value_hash = hash_from_text(statement.column_text(2)),
        .value = deleted ? Json::null() : parse_stored_json(statement.column_text(3))
    };
}

std::optional<DocumentMetadata> SqliteSession::read_current_metadata(
    CollectionId collection,
    std::string_view key
)
{
    require_backend_tx();

    detail::Statement statement{
        connection_->get(), detail::PrivateSchemaSql::select_current_metadata()
    };
    statement.bind_int64(1, collection);
    statement.bind_text(2, key);
    if (!statement.step())
    {
        return std::nullopt;
    }

    return DocumentMetadata{
        .collection = collection,
        .key = std::string(key),
        .version = static_cast<Version>(statement.column_int64(0)),
        .deleted = statement.column_int64(1) != 0,
        .value_hash = hash_from_text(statement.column_text(2))
    };
}

QueryResultEnvelope SqliteSession::query_snapshot(
    CollectionId collection,
    const QuerySpec& query,
    Version version
)
{
    require_backend_tx();
    validate_supported_query(query);

    auto candidates = list_snapshot(collection, ListOptions{.after_key = query.after_key}, version);

    QueryResultEnvelope result;
    for (const auto& row : candidates.rows)
    {
        if (row.deleted)
        {
            continue;
        }
        if (!matches_query(row.key, row.value, query))
        {
            continue;
        }

        result.rows.push_back(row);
        if (query.limit && result.rows.size() >= *query.limit)
        {
            break;
        }
    }

    return result;
}

QueryMetadataResult SqliteSession::query_current_metadata(
    CollectionId collection,
    const QuerySpec& query
)
{
    require_backend_tx();
    validate_supported_query(query);

    auto sql =
        detail::PrivateSchemaSql::select_current_query_candidates(query.after_key.has_value());

    detail::Statement statement{connection_->get(), sql};
    auto index = 1;
    statement.bind_int64(index++, collection);
    if (query.after_key)
    {
        statement.bind_text(index++, *query.after_key);
    }

    QueryMetadataResult result;
    while (statement.step())
    {
        if (statement.column_int64(2) != 0)
        {
            continue;
        }

        auto key = statement.column_text(0);
        auto value = parse_stored_json(statement.column_text(4));
        if (!matches_query(key, value, query))
        {
            continue;
        }

        result.rows.push_back(
            DocumentMetadata{
                .collection = collection,
                .key = std::move(key),
                .version = static_cast<Version>(statement.column_int64(1)),
                .deleted = false,
                .value_hash = hash_from_text(statement.column_text(3))
            }
        );
        if (query.limit && result.rows.size() >= *query.limit)
        {
            break;
        }
    }

    return result;
}

QueryResultEnvelope SqliteSession::list_snapshot(
    CollectionId collection,
    const ListOptions& options,
    Version version
)
{
    require_backend_tx();

    auto sql = detail::PrivateSchemaSql::select_snapshot_list(
        options.after_key.has_value(), options.limit.has_value()
    );

    detail::Statement statement{connection_->get(), sql};
    auto index = 1;
    statement.bind_int64(index++, collection);
    statement.bind_int64(index++, version);
    if (options.after_key)
    {
        statement.bind_text(index++, *options.after_key);
    }
    if (options.limit)
    {
        statement.bind_int64(index++, static_cast<std::int64_t>(*options.limit));
    }

    QueryResultEnvelope result;
    while (statement.step())
    {
        auto deleted = statement.column_int64(2) != 0;
        result.rows.push_back(
            DocumentEnvelope{
                .collection = collection,
                .key = statement.column_text(0),
                .version = static_cast<Version>(statement.column_int64(1)),
                .deleted = deleted,
                .value_hash = hash_from_text(statement.column_text(3)),
                .value = deleted ? Json::null() : parse_stored_json(statement.column_text(4))
            }
        );
    }

    return result;
}

QueryMetadataResult SqliteSession::list_current_metadata(
    CollectionId collection,
    const ListOptions& options
)
{
    require_backend_tx();

    auto sql = detail::PrivateSchemaSql::select_current_metadata_list(
        options.after_key.has_value(), options.limit.has_value()
    );

    detail::Statement statement{connection_->get(), sql};
    auto index = 1;
    statement.bind_int64(index++, collection);
    if (options.after_key)
    {
        statement.bind_text(index++, *options.after_key);
    }
    if (options.limit)
    {
        statement.bind_int64(index++, static_cast<std::int64_t>(*options.limit));
    }

    QueryMetadataResult result;
    while (statement.step())
    {
        result.rows.push_back(
            DocumentMetadata{
                .collection = collection,
                .key = statement.column_text(0),
                .version = static_cast<Version>(statement.column_int64(1)),
                .deleted = statement.column_int64(2) != 0,
                .value_hash = hash_from_text(statement.column_text(3))
            }
        );
    }

    return result;
}

void SqliteSession::insert_history(
    CollectionId collection,
    const WriteEnvelope& write,
    Version commit_version
)
{
    require_backend_tx();

    detail::Statement statement{connection_->get(), detail::PrivateSchemaSql::insert_history()};
    statement.bind_int64(1, collection);
    statement.bind_text(2, write.key);
    statement.bind_int64(3, commit_version);
    statement.bind_int64(4, write_is_deleted(write) ? 1 : 0);
    statement.bind_text(5, hash_to_text(write.value_hash));
    if (write_is_deleted(write))
    {
        statement.bind_null(6);
    }
    else
    {
        statement.bind_text(6, write_value_text(write));
    }
    statement.step();
}

void SqliteSession::upsert_current(
    CollectionId collection,
    const WriteEnvelope& write,
    Version commit_version
)
{
    require_backend_tx();
    check_unique_constraints(*connection_, collection, write);

    detail::Statement statement{connection_->get(), detail::PrivateSchemaSql::upsert_current()};
    statement.bind_int64(1, collection);
    statement.bind_text(2, write.key);
    statement.bind_int64(3, commit_version);
    statement.bind_int64(4, write_is_deleted(write) ? 1 : 0);
    statement.bind_text(5, hash_to_text(write.value_hash));
    if (write_is_deleted(write))
    {
        statement.bind_null(6);
    }
    else
    {
        statement.bind_text(6, write_value_text(write));
    }
    statement.step();
}

void SqliteSession::require_backend_tx() const
{
    if (!connection_ || !in_backend_tx_)
    {
        throw BackendError("sqlite backend transaction is not open");
    }
}

Version SqliteSession::read_clock_row()
{
    detail::Statement statement{
        connection_->get(), detail::PrivateSchemaSql::select_clock_version()
    };
    if (!statement.step())
    {
        throw BackendError("sqlite clock row is missing");
    }
    return static_cast<Version>(statement.column_int64(0));
}

} // namespace mt::backends::sqlite
