#include "mt/backends/sqlite.hpp"

#include "sqlite_detail.hpp"
#include "sqlite_document.hpp"
#include "sqlite_schema.hpp"

#include "mt/errors.hpp"
#include "mt/schema.hpp"

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

// -----------------------------------------------------------------------------
// SQLite backend implementation unit.
//
// The public header remains dependency-free. SQLite client details stay in this
// optional implementation unit and private helpers.
// -----------------------------------------------------------------------------

namespace mt::backends::sqlite
{

struct SqliteBackendState
{
    explicit SqliteBackendState(std::string sqlite_path)
        : path(std::move(sqlite_path))
    {
    }

    std::string path;
    std::mutex bootstrap_mutex;
    std::atomic_bool bootstrapped = false;
};

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

    for (const auto& index : load_collection_indexes(connection, collection))
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

        detail::Statement statement{
            connection.get(), detail::PrivateSchemaSql::select_current_unique_index_candidates()
        };
        statement.bind_int64(1, collection);
        statement.bind_text(2, write.key);

        while (statement.step())
        {
            if (statement.column_is_null(1))
            {
                continue;
            }

            auto current_value =
                json_path_value(parse_stored_json(statement.column_text(1)), index.json_path);
            if (current_value && *current_value == *write_value)
            {
                throw BackendError("sqlite backend unique index constraint violation");
            }
        }
    }
}

void bootstrap_schema(
    detail::Connection& connection,
    const BootstrapSpec& spec
)
{
    connection.execute(detail::PrivateSchemaSql::enable_foreign_keys());

    connection.execute(detail::PrivateSchemaSql::create_meta_table());

    {
        detail::Statement statement{
            connection.get(), detail::PrivateSchemaSql::upsert_metadata_schema_version()
        };
        statement.bind_int64(1, spec.metadata_schema_version);
        statement.step();
    }

    connection.execute(detail::PrivateSchemaSql::create_clock_table());
    connection.execute(detail::PrivateSchemaSql::insert_default_clock_row());

    connection.execute(detail::PrivateSchemaSql::create_collections_table());

    connection.execute(detail::PrivateSchemaSql::create_active_transactions_table());

    connection.execute(detail::PrivateSchemaSql::create_history_table());
    connection.execute(detail::PrivateSchemaSql::create_history_snapshot_index());

    connection.execute(detail::PrivateSchemaSql::create_current_table());
}

void ensure_bootstrapped(
    const std::shared_ptr<SqliteBackendState>& state,
    const BootstrapSpec& spec = BootstrapSpec{}
)
{
    if (state->path == detail::StoragePath::memory())
    {
        return;
    }

    if (state->bootstrapped.load())
    {
        return;
    }

    auto lock = std::lock_guard<std::mutex>{state->bootstrap_mutex};
    if (state->bootstrapped.load())
    {
        return;
    }

    auto connection = detail::Connection::open(state->path);
    bootstrap_schema(connection, spec);
    state->bootstrapped.store(true);
}

detail::Connection open_bootstrapped_connection(const std::shared_ptr<SqliteBackendState>& state)
{
    if (state->path == detail::StoragePath::memory())
    {
        // SQLite in-memory databases are scoped to a single connection.
        auto connection = detail::Connection::open(state->path);
        bootstrap_schema(connection, BootstrapSpec{});
        return connection;
    }

    ensure_bootstrapped(state);
    return detail::Connection::open(state->path);
}

class SqliteSession final : public IBackendSession
{
  public:
    explicit SqliteSession(std::shared_ptr<SqliteBackendState> state)
        : state_(std::move(state))
    {
    }

    void begin_backend_transaction() override
    {
        if (in_backend_tx_)
        {
            throw BackendError("sqlite backend transaction is already open");
        }

        connection_ = open_bootstrapped_connection(state_);
        connection_->execute(detail::PrivateSchemaSql::begin_immediate());
        in_backend_tx_ = true;
    }

    void commit_backend_transaction() override
    {
        require_backend_tx();
        connection_->execute(detail::PrivateSchemaSql::commit());
        in_backend_tx_ = false;
        clock_locked_ = false;
        connection_.reset();
    }

    void abort_backend_transaction() noexcept override
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

    Version read_clock() override
    {
        require_backend_tx();
        return read_clock_row();
    }

    Version lock_clock_and_read() override
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

    Version increment_clock_and_return() override
    {
        require_backend_tx();
        if (!clock_locked_)
        {
            throw BackendError("clock must be locked before increment");
        }

        connection_->execute(detail::PrivateSchemaSql::increment_clock_version());
        return read_clock_row();
    }

    TxId create_transaction_id() override
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

    void register_active_transaction(
        TxId tx_id,
        Version start_version
    ) override
    {
        require_backend_tx();

        detail::Statement statement{
            connection_->get(), detail::PrivateSchemaSql::insert_or_replace_active_transaction()
        };
        statement.bind_text(1, tx_id);
        statement.bind_int64(2, start_version);
        statement.step();
    }

    void unregister_active_transaction(TxId tx_id) noexcept override
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

    std::optional<DocumentEnvelope> read_snapshot(
        CollectionId collection,
        std::string_view key,
        Version version
    ) override
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

    std::optional<DocumentMetadata> read_current_metadata(
        CollectionId collection,
        std::string_view key
    ) override
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

    QueryResultEnvelope query_snapshot(
        CollectionId collection,
        const QuerySpec& query,
        Version version
    ) override
    {
        require_backend_tx();
        validate_supported_query(query);

        auto candidates =
            list_snapshot(collection, ListOptions{.after_key = query.after_key}, version);

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

    QueryMetadataResult query_current_metadata(
        CollectionId collection,
        const QuerySpec& query
    ) override
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

    QueryResultEnvelope list_snapshot(
        CollectionId collection,
        const ListOptions& options,
        Version version
    ) override
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

    QueryMetadataResult list_current_metadata(
        CollectionId collection,
        const ListOptions& options
    ) override
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

    void insert_history(
        CollectionId collection,
        const WriteEnvelope& write,
        Version commit_version
    ) override
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

    void upsert_current(
        CollectionId collection,
        const WriteEnvelope& write,
        Version commit_version
    ) override
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

  private:
    void require_backend_tx() const
    {
        if (!connection_ || !in_backend_tx_)
        {
            throw BackendError("sqlite backend transaction is not open");
        }
    }

    Version read_clock_row()
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

  private:
    std::shared_ptr<SqliteBackendState> state_;
    std::optional<detail::Connection> connection_;
    bool in_backend_tx_ = false;
    bool clock_locked_ = false;
};

} // namespace

SqliteBackend::SqliteBackend()
    : SqliteBackend(std::string(detail::StoragePath::memory()))
{
}

SqliteBackend::SqliteBackend(std::string path)
    : state_(std::make_shared<SqliteBackendState>(std::move(path)))
{
}

BackendCapabilities SqliteBackend::capabilities() const
{
    auto capabilities = BackendCapabilities{};
    capabilities.query.key_prefix = true;
    capabilities.query.json_equals = true;
    capabilities.schema.json_indexes = true;
    capabilities.schema.unique_indexes = true;
    return capabilities;
}

std::unique_ptr<IBackendSession> SqliteBackend::open_session()
{
    return std::make_unique<SqliteSession>(state_);
}

void SqliteBackend::bootstrap(const BootstrapSpec& spec)
{
    if (state_->path == detail::StoragePath::memory())
    {
        auto connection = detail::Connection::open(state_->path);
        bootstrap_schema(connection, spec);
        return;
    }

    ensure_bootstrapped(state_, spec);
}

CollectionDescriptor SqliteBackend::ensure_collection(const CollectionSpec& spec)
{
    auto connection = open_bootstrapped_connection(state_);
    connection.execute(detail::PrivateSchemaSql::begin_immediate());
    try
    {
        auto stored = load_collection_spec(connection, spec.logical_name);
        if (!stored)
        {
            insert_collection(connection, spec);
            auto descriptor = load_collection_descriptor(connection, spec.logical_name);
            connection.execute(detail::PrivateSchemaSql::commit());
            return descriptor;
        }

        auto diff = diff_schemas(*stored, spec);
        if (!diff.is_compatible())
        {
            const auto& change = diff.incompatible_changes.front();
            throw BackendError(
                "incompatible schema change for collection '" + spec.logical_name + "' at " +
                change.path + ": " + change.message
            );
        }

        update_collection(connection, spec);
        auto descriptor = load_collection_descriptor(connection, spec.logical_name);
        connection.execute(detail::PrivateSchemaSql::commit());
        return descriptor;
    }
    catch (...)
    {
        sqlite3_exec(
            connection.get(), detail::PrivateSchemaSql::rollback().c_str(), nullptr, nullptr,
            nullptr
        );
        throw;
    }
}

CollectionDescriptor SqliteBackend::get_collection(std::string_view logical_name)
{
    auto connection = open_bootstrapped_connection(state_);
    return load_collection_descriptor(connection, logical_name);
}

} // namespace mt::backends::sqlite
