#include "postgres_session.hpp"

#include "mt/errors.hpp"
#include "mt/hash.hpp"
#include "mt/json_parser.hpp"
#include "mt/query.hpp"

#include "postgres_schema.hpp"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace mt::backends::postgres
{

namespace
{
Hash hash_from_text(const std::string& encoded)
{
    if (encoded.size() % 2 != 0)
    {
        throw BackendError("invalid stored PostgreSQL hash");
    }

    Hash hash;
    hash.bytes.reserve(encoded.size() / 2);
    for (std::size_t i = 0; i < encoded.size(); i += 2)
    {
        hash.bytes.push_back(
            static_cast<std::uint8_t>((hex_value(encoded[i]) << 4) | hex_value(encoded[i + 1]))
        );
    }
    return hash;
}

bool postgres_bool(std::string_view encoded)
{
    return encoded == "t" || encoded == "true" || encoded == "1";
}

bool write_is_deleted(const WriteEnvelope& write)
{
    return write.kind == WriteKind::Delete;
}

std::string write_value_text(const WriteEnvelope& write)
{
    if (write_is_deleted(write))
    {
        return "null";
    }
    return write.value.canonical_string();
}

void validate_supported_query(const QuerySpec& query)
{
    if (!query.order_by_key)
    {
        throw BackendError("postgres backend only supports key ordering");
    }

    for (const auto& predicate : query.predicates)
    {
        if (predicate.op == QueryOp::JsonContains)
        {
            throw BackendError("postgres backend does not support JSON contains predicates");
        }
    }
}

void check_unique_constraints(
    detail::Connection& connection,
    CollectionId collection,
    const WriteEnvelope& write
)
{
    if (write.kind == WriteKind::Delete)
    {
        return;
    }

    for (const auto& index : detail::load_collection_indexes(connection, collection))
    {
        if (!index.unique)
        {
            continue;
        }

        auto write_value = json_path_value(write.value, index.json_path);
        if (!write_value)
        {
            continue;
        }

        auto candidates = connection.exec_params(
            detail::PrivateSchemaSql::select_current_unique_index_candidates(),
            {std::to_string(collection), write.key}, {PGRES_TUPLES_OK}
        );
        for (auto row = 0; row < candidates.rows(); ++row)
        {
            if (candidates.is_null(row, 1))
            {
                continue;
            }

            auto current_value =
                json_path_value(parse_json(candidates.value(row, 1)), index.json_path);
            if (current_value && *current_value == *write_value)
            {
                throw BackendError("postgres backend unique index constraint violation");
            }
        }
    }
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
    CollectionId collection,
    std::string_view key,
    Version version
)
{
    require_backend_tx();

    auto result = connection_->exec_params(
        detail::PrivateSchemaSql::select_snapshot_document(),
        {std::to_string(collection), std::string(key), std::to_string(version)}, {PGRES_TUPLES_OK}
    );
    if (result.rows() == 0)
    {
        return std::nullopt;
    }

    auto deleted = postgres_bool(result.value(0, 1));
    return DocumentEnvelope{
        .collection = collection,
        .key = std::string(key),
        .version = static_cast<Version>(std::stoull(std::string(result.value(0, 0)))),
        .deleted = deleted,
        .value_hash = hash_from_text(std::string(result.value(0, 2))),
        .value = deleted ? Json::null() : parse_json(result.value(0, 3))
    };
}

std::optional<DocumentMetadata> PostgresSession::read_current_metadata(
    CollectionId collection,
    std::string_view key
)
{
    require_backend_tx();

    auto result = connection_->exec_params(
        detail::PrivateSchemaSql::select_current_metadata(),
        {std::to_string(collection), std::string(key)}, {PGRES_TUPLES_OK}
    );
    if (result.rows() == 0)
    {
        return std::nullopt;
    }

    return DocumentMetadata{
        .collection = collection,
        .key = std::string(key),
        .version = static_cast<Version>(std::stoull(std::string(result.value(0, 0)))),
        .deleted = postgres_bool(result.value(0, 1)),
        .value_hash = hash_from_text(std::string(result.value(0, 2)))
    };
}

QueryResultEnvelope PostgresSession::query_snapshot(
    CollectionId collection,
    const QuerySpec& query,
    Version version
)
{
    require_backend_tx();
    validate_supported_query(query);

    auto candidates = list_snapshot(collection, ListOptions{.after_key = query.after_key}, version);

    QueryResultEnvelope envelope;
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

        envelope.rows.push_back(row);
        if (query.limit && envelope.rows.size() >= *query.limit)
        {
            break;
        }
    }

    return envelope;
}

QueryMetadataResult PostgresSession::query_current_metadata(
    CollectionId collection,
    const QuerySpec& query
)
{
    require_backend_tx();
    validate_supported_query(query);

    auto params = std::vector<std::string>{std::to_string(collection)};
    if (query.after_key)
    {
        params.push_back(*query.after_key);
    }

    auto result = connection_->exec_params(
        detail::PrivateSchemaSql::select_current_query_candidates(query.after_key.has_value()),
        params, {PGRES_TUPLES_OK}
    );

    QueryMetadataResult envelope;
    for (auto row = 0; row < result.rows(); ++row)
    {
        if (postgres_bool(result.value(row, 2)))
        {
            continue;
        }

        auto key = std::string(result.value(row, 0));
        auto value = parse_json(result.value(row, 4));
        if (!matches_query(key, value, query))
        {
            continue;
        }

        envelope.rows.push_back(
            DocumentMetadata{
                .collection = collection,
                .key = std::move(key),
                .version = static_cast<Version>(std::stoull(std::string(result.value(row, 1)))),
                .deleted = false,
                .value_hash = hash_from_text(std::string(result.value(row, 3)))
            }
        );
        if (query.limit && envelope.rows.size() >= *query.limit)
        {
            break;
        }
    }

    return envelope;
}

QueryResultEnvelope PostgresSession::list_snapshot(
    CollectionId collection,
    const ListOptions& options,
    Version version
)
{
    require_backend_tx();

    auto params = std::vector<std::string>{std::to_string(collection), std::to_string(version)};
    if (options.after_key)
    {
        params.push_back(*options.after_key);
    }
    if (options.limit)
    {
        params.push_back(std::to_string(*options.limit));
    }

    auto result = connection_->exec_params(
        detail::PrivateSchemaSql::select_snapshot_list(
            options.after_key.has_value(), options.limit.has_value()
        ),
        params, {PGRES_TUPLES_OK}
    );

    QueryResultEnvelope envelope;
    for (auto row = 0; row < result.rows(); ++row)
    {
        auto deleted = postgres_bool(result.value(row, 2));
        envelope.rows.push_back(
            DocumentEnvelope{
                .collection = collection,
                .key = std::string(result.value(row, 0)),
                .version = static_cast<Version>(std::stoull(std::string(result.value(row, 1)))),
                .deleted = deleted,
                .value_hash = hash_from_text(std::string(result.value(row, 3))),
                .value = deleted ? Json::null() : parse_json(result.value(row, 4))
            }
        );
    }

    return envelope;
}

QueryMetadataResult PostgresSession::list_current_metadata(
    CollectionId collection,
    const ListOptions& options
)
{
    require_backend_tx();

    auto params = std::vector<std::string>{std::to_string(collection)};
    if (options.after_key)
    {
        params.push_back(*options.after_key);
    }
    if (options.limit)
    {
        params.push_back(std::to_string(*options.limit));
    }

    auto result = connection_->exec_params(
        detail::PrivateSchemaSql::select_current_metadata_list(
            options.after_key.has_value(), options.limit.has_value()
        ),
        params, {PGRES_TUPLES_OK}
    );

    QueryMetadataResult envelope;
    for (auto row = 0; row < result.rows(); ++row)
    {
        envelope.rows.push_back(
            DocumentMetadata{
                .collection = collection,
                .key = std::string(result.value(row, 0)),
                .version = static_cast<Version>(std::stoull(std::string(result.value(row, 1)))),
                .deleted = postgres_bool(result.value(row, 2)),
                .value_hash = hash_from_text(std::string(result.value(row, 3)))
            }
        );
    }

    return envelope;
}

void PostgresSession::insert_history(
    CollectionId collection,
    const WriteEnvelope& write,
    Version commit_version
)
{
    require_backend_tx();

    connection_->exec_params(
        detail::PrivateSchemaSql::insert_history(),
        {std::to_string(collection), write.key, std::to_string(commit_version),
         write_is_deleted(write) ? "true" : "false", hash_to_text(write.value_hash),
         write_value_text(write)},
        {PGRES_COMMAND_OK}
    );
}

void PostgresSession::upsert_current(
    CollectionId collection,
    const WriteEnvelope& write,
    Version commit_version
)
{
    require_backend_tx();
    check_unique_constraints(*connection_, collection, write);

    connection_->exec_params(
        detail::PrivateSchemaSql::upsert_current(),
        {std::to_string(collection), write.key, std::to_string(commit_version),
         write_is_deleted(write) ? "true" : "false", hash_to_text(write.value_hash),
         write_value_text(write)},
        {PGRES_COMMAND_OK}
    );
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
